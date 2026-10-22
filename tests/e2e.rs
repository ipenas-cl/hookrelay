//! End-to-end tests: real HTTP downstream, real SQLite, real worker.
use axum::{body::Body, http::Request, routing::post, Router};
use hookrelay::{
    api,
    backoff::Backoff,
    metrics::Metrics,
    signature,
    store::{Status, Store},
    worker::Worker,
};
use http_body_util::BodyExt;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tower::ServiceExt;

const SECRET: &[u8] = b"test-app-secret";

async fn downstream(fail_first: usize, fail_code: u16) -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let app = Router::new().route(
        "/hook",
        post(move || {
            let h = h.clone();
            async move {
                let n = h.fetch_add(1, Ordering::SeqCst);
                if n < fail_first {
                    axum::http::StatusCode::from_u16(fail_code).unwrap()
                } else {
                    axum::http::StatusCode::OK
                }
            }
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (format!("http://{addr}/hook"), hits)
}

fn setup(target: String, max_attempts: u32) -> (Router, Worker, Store) {
    let store = Store::in_memory().unwrap();
    let metrics = Metrics::new();
    let worker = Worker {
        store: store.clone(),
        metrics: metrics.clone(),
        client: reqwest::Client::new(),
        target_url: target,
        max_attempts,
        backoff: Backoff {
            base: Duration::from_millis(1),
            max: Duration::from_millis(5),
        },
        poll_interval: Duration::from_millis(5),
        batch_size: 16,
    };
    let app = api::router(api::AppState {
        store: store.clone(),
        metrics,
        secret: Arc::new(SECRET.to_vec()),
        verify_token: Some(Arc::new("vt".into())),
    });
    (app, worker, store)
}

fn signed(body: &str) -> Request<Body> {
    Request::post("/ingest/whatsapp")
        .header("content-type", "application/json")
        .header(
            "x-hub-signature-256",
            signature::sign(SECRET, body.as_bytes()),
        )
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn drain(w: &Worker) {
    for _ in 0..200 {
        w.tick().await.unwrap();
        let s = w.store.stats().unwrap();
        if s.pending == 0 && s.inflight == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    panic!("queue did not drain");
}

#[tokio::test]
async fn rejects_bad_or_missing_signature() {
    let (app, _, store) = setup("http://127.0.0.1:1/".into(), 3);
    let r = app
        .clone()
        .oneshot(Request::post("/ingest/wa").body(Body::from("{}")).unwrap())
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    let r = app
        .oneshot(
            Request::post("/ingest/wa")
                .header("x-hub-signature-256", "sha256=00")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    assert_eq!(store.stats().unwrap().pending, 0);
}

#[tokio::test]
async fn accepts_dedupes_and_delivers() {
    let (url, hits) = downstream(0, 500).await;
    let (app, worker, store) = setup(url, 3);
    let body = r#"{"object":"whatsapp_business_account","entry":[{"id":"1"}]}"#;
    assert_eq!(
        app.clone().oneshot(signed(body)).await.unwrap().status(),
        202
    );
    let dup = app.clone().oneshot(signed(body)).await.unwrap();
    assert_eq!(dup.status(), 200);
    let v: serde_json::Value =
        serde_json::from_slice(&dup.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(v["duplicate"], true);
    drain(&worker).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "exactly one downstream delivery"
    );
    assert_eq!(store.stats().unwrap().delivered, 1);
}

#[tokio::test]
async fn retries_with_backoff_then_succeeds() {
    let (url, hits) = downstream(2, 503).await;
    let (app, worker, store) = setup(url, 5);
    app.oneshot(signed(r#"{"n":1}"#)).await.unwrap();
    drain(&worker).await;
    assert_eq!(hits.load(Ordering::SeqCst), 3);
    let d = &store.list(Some(Status::Delivered), 10, None).unwrap()[0];
    assert_eq!(d.attempts, 3);
}

#[tokio::test]
async fn exhausts_budget_to_dlq_and_replays() {
    let (url, hits) = downstream(3, 500).await;
    let (app, worker, store) = setup(url, 3);
    app.clone().oneshot(signed(r#"{"n":2}"#)).await.unwrap();
    drain(&worker).await;
    assert_eq!(hits.load(Ordering::SeqCst), 3);
    let dead = store.list(Some(Status::Dead), 10, None).unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].last_status_code, Some(500));

    let r = app
        .clone()
        .oneshot(
            Request::post(format!("/api/deliveries/{}/replay", dead[0].id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    drain(&worker).await;
    assert_eq!(store.stats().unwrap().delivered, 1);
}

#[tokio::test]
async fn permanent_4xx_goes_straight_to_dlq() {
    let (url, hits) = downstream(99, 422).await;
    let (app, worker, store) = setup(url, 8);
    app.oneshot(signed(r#"{"n":3}"#)).await.unwrap();
    drain(&worker).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(store.stats().unwrap().dead, 1);
}

#[tokio::test]
async fn meta_handshake_health_and_metrics() {
    let (app, worker, _) = setup("http://127.0.0.1:1/".into(), 1);
    let ok = app
        .clone()
        .oneshot(
            Request::get("/ingest/wa?hub.mode=subscribe&hub.verify_token=vt&hub.challenge=42")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
    assert_eq!(
        &ok.into_body().collect().await.unwrap().to_bytes()[..],
        b"42"
    );
    let bad = app
        .clone()
        .oneshot(
            Request::get("/ingest/wa?hub.mode=subscribe&hub.verify_token=nope&hub.challenge=42")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad.status(), 403);
    assert_eq!(
        app.clone()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        200
    );
    app.clone().oneshot(signed("{}")).await.unwrap();
    worker.refresh_gauges();
    let m = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let text =
        String::from_utf8(m.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(text.contains("hookrelay_ingested_total{result=\"accepted\",source=\"whatsapp\"} 1"));
    assert!(text.contains("hookrelay_queue_depth{status=\"pending\"} 1"));
}
