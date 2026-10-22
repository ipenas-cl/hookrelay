//! Configuration from environment variables.
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: String,
    pub db_path: String,
    /// HMAC secret shared with the webhook provider (Meta "App Secret").
    pub secret: String,
    /// Token used for the Meta `hub.challenge` subscription handshake.
    pub verify_token: Option<String>,
    /// Downstream URL that receives relayed events.
    pub target_url: String,
    pub max_attempts: u32,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub request_timeout: Duration,
    pub poll_interval: Duration,
    pub batch_size: u32,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_num(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let secret =
            std::env::var("HOOKRELAY_SECRET").map_err(|_| "HOOKRELAY_SECRET is required")?;
        let target_url = std::env::var("HOOKRELAY_TARGET_URL")
            .map_err(|_| "HOOKRELAY_TARGET_URL is required")?;
        Ok(Self {
            listen: env_or("HOOKRELAY_LISTEN", "0.0.0.0:8080"),
            db_path: env_or("HOOKRELAY_DB", "hookrelay.db"),
            secret,
            verify_token: std::env::var("HOOKRELAY_VERIFY_TOKEN").ok(),
            target_url,
            max_attempts: env_num("HOOKRELAY_MAX_ATTEMPTS", 8) as u32,
            backoff_base: Duration::from_millis(env_num("HOOKRELAY_BACKOFF_BASE_MS", 1000)),
            backoff_max: Duration::from_millis(env_num("HOOKRELAY_BACKOFF_MAX_MS", 300_000)),
            request_timeout: Duration::from_millis(env_num("HOOKRELAY_TIMEOUT_MS", 10_000)),
            poll_interval: Duration::from_millis(env_num("HOOKRELAY_POLL_MS", 250)),
            batch_size: env_num("HOOKRELAY_BATCH", 32) as u32,
        })
    }
}
