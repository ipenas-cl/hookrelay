//! HTTP surface: webhook ingest, Meta verification handshake, ops API, metrics and health.
use crate::metrics::Metrics;
use crate::signature;
use crate::store::{Enqueued, Status, Store};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

pub const MAX_BODY: usize = 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub metrics: Metrics,
    pub secret: Arc<Vec<u8>>,
    pub verify_token: Option<Arc<String>>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/ingest/:source", post(ingest).get(verify_subscription))
        .route("/api/deliveries", get(list_deliveries))
        .route("/api/deliveries/:id", get(get_delivery))
        .route("/api/deliveries/:id/replay", post(replay))
        .route("/api/stats", get(stats))
        .route("/metrics", get(metrics))
        .route("/healthz", get(healthz))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_BODY))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

fn err(code: StatusCode, msg: &str) -> Response {
    (code, Json(json!({ "error": msg }))).into_response()
}

/// Derives the idempotency key: explicit header > provider event id > SHA-256 of the body.
fn idempotency_key(source: &str, headers: &HeaderMap, body: &[u8]) -> String {
    for h in ["idempotency-key", "x-github-delivery", "x-request-id"] {
        if let Some(v) = headers.get(h).and_then(|v| v.to_str().ok()) {
            if !v.is_empty() {
                return format!("{source}:{v}");
            }
        }
    }
    format!("{source}:sha256:{}", hex::encode(Sha256::digest(body)))
}

async fn ingest(
    State(st): State<AppState>,
    Path(source): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let sig = headers
        .get("x-hub-signature-256")
        .and_then(|v| v.to_str().ok());
    let Some(sig) = sig else {
        st.metrics
            .ingested
            .with_label_values(&[&source, "unsigned"])
            .inc();
        return err(StatusCode::UNAUTHORIZED, "missing X-Hub-Signature-256");
    };
    if !signature::verify(&st.secret, &body, sig) {
        st.metrics
            .ingested
            .with_label_values(&[&source, "bad_signature"])
            .inc();
        return err(StatusCode::UNAUTHORIZED, "invalid signature");
    }
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let key = idempotency_key(&source, &headers, &body);
    let store = st.store.clone();
    let src = source.clone();
    let res = tokio::task::spawn_blocking(move || store.enqueue(&key, &src, &body, &ct)).await;
    match res {
        Ok(Ok(Enqueued::New(id))) => {
            st.metrics
                .ingested
                .with_label_values(&[&source, "accepted"])
                .inc();
            (
                StatusCode::ACCEPTED,
                Json(json!({ "id": id, "duplicate": false })),
            )
                .into_response()
        }
        Ok(Ok(Enqueued::Duplicate(id))) => {
            st.metrics
                .ingested
                .with_label_values(&[&source, "duplicate"])
                .inc();
            (StatusCode::OK, Json(json!({ "id": id, "duplicate": true }))).into_response()
        }
        _ => {
            st.metrics
                .ingested
                .with_label_values(&[&source, "error"])
                .inc();
            // 5xx makes the provider retry, so nothing is lost if persistence fails.
            err(StatusCode::SERVICE_UNAVAILABLE, "failed to persist event")
        }
    }
}

#[derive(Deserialize)]
struct HubQuery {
    #[serde(rename = "hub.mode")]
    mode: Option<String>,
    #[serde(rename = "hub.verify_token")]
    token: Option<String>,
    #[serde(rename = "hub.challenge")]
    challenge: Option<String>,
}

/// Meta webhook subscription handshake (GET with hub.mode=subscribe).
async fn verify_subscription(State(st): State<AppState>, Query(q): Query<HubQuery>) -> Response {
    match (&st.verify_token, q.mode.as_deref(), q.token, q.challenge) {
        (Some(expected), Some("subscribe"), Some(tok), Some(ch)) if tok == **expected => {
            (StatusCode::OK, ch).into_response()
        }
        _ => err(StatusCode::FORBIDDEN, "verification failed"),
    }
}

#[derive(Deserialize)]
struct ListQuery {
    status: Option<String>,
    limit: Option<u32>,
    before: Option<i64>,
}

async fn list_deliveries(State(st): State<AppState>, Query(q): Query<ListQuery>) -> Response {
    let status = match q.status.as_deref() {
        None | Some("") | Some("all") => None,
        Some(s) => match Status::parse(s) {
            Some(s) => Some(s),
            None => return err(StatusCode::BAD_REQUEST, "unknown status"),
        },
    };
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    match st.store.list(status, limit, q.before) {
        Ok(items) => Json(json!({ "items": items })).into_response(),
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "query failed"),
    }
}

async fn get_delivery(State(st): State<AppState>, Path(id): Path<i64>) -> Response {
    match st.store.get(id) {
        Ok(Some(d)) => Json(d).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "not found"),
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "query failed"),
    }
}

async fn replay(State(st): State<AppState>, Path(id): Path<i64>) -> Response {
    match st.store.replay(id) {
        Ok(true) => (
            StatusCode::ACCEPTED,
            Json(json!({ "id": id, "status": "pending" })),
        )
            .into_response(),
        Ok(false) => match st.store.get(id) {
            Ok(Some(_)) => err(
                StatusCode::CONFLICT,
                "only dead or delivered events can be replayed",
            ),
            _ => err(StatusCode::NOT_FOUND, "not found"),
        },
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "replay failed"),
    }
}

async fn stats(State(st): State<AppState>) -> Response {
    match st.store.stats() {
        Ok(s) => Json(s).into_response(),
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "query failed"),
    }
}

async fn metrics(State(st): State<AppState>) -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4")],
        st.metrics.render(),
    )
}

async fn healthz(State(st): State<AppState>) -> Response {
    if st.store.ping() {
        Json(json!({ "status": "ok" })).into_response()
    } else {
        err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable")
    }
}
