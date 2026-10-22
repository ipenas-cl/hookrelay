use hookrelay::{
    api, backoff::Backoff, config::Config, metrics::Metrics, store::Store, worker::Worker,
};
use std::sync::Arc;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "hookrelay=info".into()),
        )
        .json()
        .init();
    let cfg = Config::from_env()?;
    let store = Store::open(&cfg.db_path)?;
    let metrics = Metrics::new();

    let worker = Worker {
        store: store.clone(),
        metrics: metrics.clone(),
        client: reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .build()?,
        target_url: cfg.target_url.clone(),
        max_attempts: cfg.max_attempts,
        backoff: Backoff {
            base: cfg.backoff_base,
            max: cfg.backoff_max,
        },
        poll_interval: cfg.poll_interval,
        batch_size: cfg.batch_size,
    };
    let (tx, rx) = watch::channel(false);
    let worker_handle = tokio::spawn(worker.run(rx));

    let app = api::router(api::AppState {
        store,
        metrics,
        secret: Arc::new(cfg.secret.into_bytes()),
        verify_token: cfg.verify_token.map(Arc::new),
    });
    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    tracing::info!(listen = %cfg.listen, target = %cfg.target_url, "hookrelay started");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    let _ = tx.send(true);
    let _ = worker_handle.await;
    Ok(())
}
