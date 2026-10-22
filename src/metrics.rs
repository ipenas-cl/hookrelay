//! Prometheus metrics (own registry, no global state, test-friendly).
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
};

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub ingested: IntCounterVec,
    pub deliveries: IntCounterVec,
    pub latency: HistogramVec,
    pub queue: IntGaugeVec,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let ingested = IntCounterVec::new(
            Opts::new("hookrelay_ingested_total", "Webhooks received, by result"),
            &["source", "result"],
        )
        .unwrap();
        let deliveries = IntCounterVec::new(
            Opts::new(
                "hookrelay_delivery_attempts_total",
                "Delivery attempts, by outcome",
            ),
            &["outcome"],
        )
        .unwrap();
        let latency = HistogramVec::new(
            HistogramOpts::new(
                "hookrelay_delivery_duration_seconds",
                "Downstream request latency",
            )
            .buckets(vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["outcome"],
        )
        .unwrap();
        let queue = IntGaugeVec::new(
            Opts::new("hookrelay_queue_depth", "Deliveries by status"),
            &["status"],
        )
        .unwrap();
        registry.register(Box::new(ingested.clone())).unwrap();
        registry.register(Box::new(deliveries.clone())).unwrap();
        registry.register(Box::new(latency.clone())).unwrap();
        registry.register(Box::new(queue.clone())).unwrap();
        Self {
            registry,
            ingested,
            deliveries,
            latency,
            queue,
        }
    }

    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }
}
