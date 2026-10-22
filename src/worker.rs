//! Delivery worker: claims due jobs, relays them downstream, schedules retries or dead-letters.
use crate::backoff::Backoff;
use crate::metrics::Metrics;
use crate::store::{now_ms, Job, Store};
use std::time::{Duration, Instant};
use tokio::sync::watch;

#[derive(Clone)]
pub struct Worker {
    pub store: Store,
    pub metrics: Metrics,
    pub client: reqwest::Client,
    pub target_url: String,
    pub max_attempts: u32,
    pub backoff: Backoff,
    pub poll_interval: Duration,
    pub batch_size: u32,
}

/// Outcome classification: 2xx = success, 4xx (except 408/429) = permanent, everything else = retryable.
pub fn is_retryable(code: u16) -> bool {
    !(400..500).contains(&code) || code == 408 || code == 429
}

impl Worker {
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            match self.tick().await {
                Ok(n) if n > 0 => continue,
                Ok(_) => {}
                Err(e) => tracing::error!(error = %e, "worker tick failed"),
            }
            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {}
                _ = shutdown.changed() => { tracing::info!("worker shutting down"); return; }
            }
        }
    }

    /// Processes one batch concurrently. Returns how many jobs were handled.
    pub async fn tick(&self) -> Result<usize, crate::store::StoreError> {
        let jobs = self.store.claim_due(now_ms(), self.batch_size)?;
        let n = jobs.len();
        let mut set = tokio::task::JoinSet::new();
        for job in jobs {
            let w = self.clone();
            set.spawn(async move { w.deliver(job).await });
        }
        while let Some(r) = set.join_next().await {
            if let Ok(Err(e)) = r {
                tracing::error!(error = %e, "failed to record delivery result");
            }
        }
        self.refresh_gauges();
        Ok(n)
    }

    pub fn refresh_gauges(&self) {
        if let Ok(s) = self.store.stats() {
            for (k, v) in [
                ("pending", s.pending),
                ("inflight", s.inflight),
                ("delivered", s.delivered),
                ("dead", s.dead),
            ] {
                self.metrics.queue.with_label_values(&[k]).set(v);
            }
        }
    }

    async fn deliver(&self, job: Job) -> Result<(), crate::store::StoreError> {
        let start = Instant::now();
        let res = self
            .client
            .post(&self.target_url)
            .header("content-type", &job.content_type)
            .header("idempotency-key", &job.idempotency_key)
            .header("x-hookrelay-source", &job.source)
            .header("x-hookrelay-attempt", (job.attempts + 1).to_string())
            .body(job.payload.clone())
            .send()
            .await;
        let elapsed = start.elapsed().as_secs_f64();
        let attempt = job.attempts + 1;
        let (code, err) = match res {
            Ok(r) if r.status().is_success() => {
                self.metrics
                    .latency
                    .with_label_values(&["success"])
                    .observe(elapsed);
                self.metrics
                    .deliveries
                    .with_label_values(&["success"])
                    .inc();
                tracing::info!(id = job.id, attempt, "delivered");
                return self.store.mark_delivered(job.id, r.status().as_u16());
            }
            Ok(r) => (
                Some(r.status().as_u16()),
                format!("downstream returned {}", r.status()),
            ),
            Err(e) => (None, format!("request error: {e}")),
        };
        self.metrics
            .latency
            .with_label_values(&["failure"])
            .observe(elapsed);
        let retryable = code.map(is_retryable).unwrap_or(true);
        if retryable && attempt < self.max_attempts {
            let delay = self.backoff.delay(attempt);
            self.metrics.deliveries.with_label_values(&["retry"]).inc();
            tracing::warn!(id = job.id, attempt, ?delay, %err, "delivery failed, retrying");
            self.store.mark_failed(
                job.id,
                code,
                &err,
                Some(now_ms() + delay.as_millis() as i64),
            )
        } else {
            self.metrics.deliveries.with_label_values(&["dead"]).inc();
            tracing::error!(id = job.id, attempt, %err, "delivery dead-lettered");
            self.store.mark_failed(job.id, code, &err, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_retryable;

    #[test]
    fn retry_classification() {
        assert!(is_retryable(500));
        assert!(is_retryable(503));
        assert!(is_retryable(429));
        assert!(is_retryable(408));
        assert!(!is_retryable(400));
        assert!(!is_retryable(404));
        assert!(!is_retryable(422));
    }
}
