//! Prometheus export of [`crate::traffic`], read when Prometheus scrapes.

use prometheus::{
    IntCounterVec, Opts,
    core::{Collector, Desc},
    proto::MetricFamily,
};

use crate::traffic::{sv1_totals, sv2_totals};

/// Registered once per process by [`super::prometheus_metrics::PrometheusMetrics::new`].
pub struct TrafficCollector([IntCounterVec; 4]);

impl TrafficCollector {
    pub fn new() -> Self {
        let v = |name, help, label| {
            IntCounterVec::new(Opts::new(name, help), &["direction", label]).expect("static metric")
        };
        Self([
            v("sv2_traffic_frames_total", "SV2 frames decoded (in) or encoded (out)", "msg_type"),
            v("sv2_traffic_bytes_total", "Plaintext bytes of those SV2 frames, header included", "msg_type"),
            v("sv1_traffic_messages_total", "SV1 lines read (in) or written (out)", "kind"),
            v("sv1_traffic_bytes_total", "Bytes of those SV1 lines, newline included", "kind"),
        ])
    }
}

impl Default for TrafficCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Collector for TrafficCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.0.iter().flat_map(|v| v.desc()).collect()
    }

    // Fresh vectors set to the current totals: monotonic counters, and nothing kept between scrapes.
    fn collect(&self) -> Vec<MetricFamily> {
        let [frames, frame_bytes, lines, line_bytes] = Self::new().0;
        for (d, msg_type, count, bytes) in sv2_totals() {
            let labels = [d.label(), &format!("0x{msg_type:02x}")];
            frames.with_label_values(&labels).inc_by(count);
            frame_bytes.with_label_values(&labels).inc_by(bytes);
        }
        for (d, kind, count, bytes) in sv1_totals() {
            lines.with_label_values(&[d.label(), kind]).inc_by(count);
            line_bytes.with_label_values(&[d.label(), kind]).inc_by(bytes);
        }
        [frames, frame_bytes, lines, line_bytes]
            .iter()
            .flat_map(|v| v.collect())
            .filter(|f| !f.get_metric().is_empty())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traffic::{Direction, record_sv1, record_sv2, sv1_kind};

    #[test]
    fn scrape_exports_counters() {
        record_sv2(Direction::Out, 0xF2, 123);
        record_sv1(Direction::Out, sv1_kind(Some("mining.set_extranonce")), 77);
        let registry = prometheus::Registry::new();
        registry.register(Box::new(TrafficCollector::new())).unwrap();
        let text = prometheus::TextEncoder::new().encode_to_string(&registry.gather()).unwrap();
        assert!(text.contains(r#"sv2_traffic_frames_total{direction="out",msg_type="0xf2"}"#), "{text}");
        assert!(text.contains(r#"sv1_traffic_bytes_total{direction="out",kind="mining.set_extranonce"}"#), "{text}");
    }
}
