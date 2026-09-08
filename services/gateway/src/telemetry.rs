use std::{
    fmt::Write,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
const BOUNDS_NS: [u64; 10] = [
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    1_000_000_000,
];
pub struct Metrics {
    pub events: AtomicU64,
    pub duplicates: AtomicU64,
    pub replayed: AtomicU64,
    pub snapshots: AtomicU64,
    pub slow_clients: AtomicU64,
    buckets: [AtomicU64; 10],
    sum_ns: AtomicU64,
    batches: AtomicU64,
}
impl Default for Metrics {
    fn default() -> Self {
        Self {
            events: AtomicU64::new(0),
            duplicates: AtomicU64::new(0),
            replayed: AtomicU64::new(0),
            snapshots: AtomicU64::new(0),
            slow_clients: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_ns: AtomicU64::new(0),
            batches: AtomicU64::new(0),
        }
    }
}
impl Metrics {
    pub fn published(&self, events: u64, duplicates: u64, elapsed: Duration) {
        self.events.fetch_add(events, Ordering::Relaxed);
        self.duplicates.fetch_add(duplicates, Ordering::Relaxed);
        let ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        for (bound, bucket) in BOUNDS_NS.iter().zip(&self.buckets) {
            if ns <= *bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    pub fn prometheus(&self) -> String {
        let mut text = String::with_capacity(2048);
        for (name, metric) in [
            ("events_published_total", &self.events),
            ("duplicate_events_total", &self.duplicates),
            ("events_replayed_total", &self.replayed),
            ("snapshots_total", &self.snapshots),
            ("slow_client_disconnects_total", &self.slow_clients),
        ] {
            let _ = writeln!(
                text,
                "# TYPE montlok_gateway_{name} counter\nmontlok_gateway_{name} {}",
                metric.load(Ordering::Relaxed)
            );
        }
        text.push_str("# HELP montlok_gateway_publish_seconds Durable journal and broadcast batch service time.\n# TYPE montlok_gateway_publish_seconds histogram\n");
        for (ns, bucket) in BOUNDS_NS.iter().zip(&self.buckets) {
            let _ = writeln!(
                text,
                "montlok_gateway_publish_seconds_bucket{{le=\"{}\"}} {}",
                *ns as f64 / 1e9,
                bucket.load(Ordering::Relaxed)
            );
        }
        let count = self.batches.load(Ordering::Relaxed);
        let _ = writeln!(
            text,
            "montlok_gateway_publish_seconds_bucket{{le=\"+Inf\"}} {count}\nmontlok_gateway_publish_seconds_count {count}\nmontlok_gateway_publish_seconds_sum {}",
            self.sum_ns.load(Ordering::Relaxed) as f64 / 1e9
        );
        text
    }
}
