//! Helpers for querying and asserting on Prometheus metrics and JSON API endpoints
//! exposed by SV2 components during integration tests.

use std::net::SocketAddr;

/// Fetch the raw Prometheus text-format metrics from a component's `/metrics` endpoint.
/// Uses `spawn_blocking` to avoid blocking the tokio runtime with synchronous HTTP calls.
pub async fn fetch_metrics(monitoring_addr: SocketAddr) -> String {
    let url = format!("http://{}/metrics", monitoring_addr);
    tokio::task::spawn_blocking(move || {
        let bytes = crate::utils::http::make_get_request(&url, 5);
        String::from_utf8(bytes).expect("metrics response should be valid UTF-8")
    })
    .await
    .expect("spawn_blocking for fetch_metrics panicked")
}

/// Fetch the JSON body from a component's API endpoint (e.g. `/api/v1/health`).
/// Uses `spawn_blocking` to avoid blocking the tokio runtime with synchronous HTTP calls.
pub async fn fetch_api(monitoring_addr: SocketAddr, path: &str) -> String {
    let url = format!("http://{}{}", monitoring_addr, path);
    tokio::task::spawn_blocking(move || {
        let bytes = crate::utils::http::make_get_request(&url, 5);
        String::from_utf8(bytes).expect("api response should be valid UTF-8")
    })
    .await
    .expect("spawn_blocking for fetch_api panicked")
}

/// Parse a specific metric value from Prometheus text format.
/// Returns `None` if the metric line is not found.
///
/// For simple gauges/counters (no labels), pass `metric_name` like `"sv2_clients_total"`.
/// For labeled metrics, pass the full label selector like
/// `"sv2_server_channels{channel_type=\"extended\"}"`.
pub(crate) fn parse_metric_value(metrics_text: &str, metric_name: &str) -> Option<f64> {
    for line in metrics_text.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(metric_name) {
            let rest = rest.trim();
            if rest.is_empty() {
                continue;
            }
            // Bare metric (no labels): value follows directly after the name
            if rest.starts_with(|c: char| c.is_ascii_digit() || c == '-') {
                return rest.parse::<f64>().ok();
            }
            // Labeled metric: skip past the closing brace to get the value
            if rest.starts_with('{') {
                if let Some(brace_end) = rest.find('}') {
                    let value_str = rest[brace_end + 1..].trim();
                    return value_str.parse::<f64>().ok();
                }
            }
        }
    }
    None
}

/// Assert that a metric is present and its value satisfies the given predicate.
pub(crate) fn assert_metric<F: Fn(f64) -> bool>(
    metrics_text: &str,
    metric_name: &str,
    predicate: F,
    description: &str,
) {
    let value = parse_metric_value(metrics_text, metric_name);
    match value {
        Some(v) => {
            assert!(
                predicate(v),
                "Metric '{}' has value {} but expected: {}",
                metric_name,
                v,
                description
            );
        }
        None => {
            panic!(
                "Metric '{}' not found in metrics output. Expected: {}",
                metric_name, description
            );
        }
    }
}

/// Assert that a metric is present with a value >= the given minimum.
pub fn assert_metric_gte(metrics_text: &str, metric_name: &str, min: f64) {
    assert_metric(
        metrics_text,
        metric_name,
        |v| v >= min,
        &format!(">= {}", min),
    );
}

/// Assert that a metric is present with the exact given value.
pub fn assert_metric_eq(metrics_text: &str, metric_name: &str, expected: f64) {
    assert_metric(
        metrics_text,
        metric_name,
        |v| (v - expected).abs() < f64::EPSILON,
        &format!("== {}", expected),
    );
}

/// Assert that a metric name does NOT appear in the metrics output at all.
pub fn assert_metric_not_present(metrics_text: &str, metric_name: &str) {
    for line in metrics_text.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(metric_name) {
            // Make sure it's an exact match (not a prefix of another metric name)
            if rest.starts_with(' ') || rest.starts_with('{') {
                panic!(
                    "Metric '{}' was found in metrics output but was expected to be absent. Line: {}",
                    metric_name, line
                );
            }
        }
    }
}

/// Assert that a metric name appears at least once in the metrics output (with any label/value).
pub fn assert_metric_present(metrics_text: &str, metric_name: &str) {
    for line in metrics_text.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(metric_name) {
            if rest.starts_with(' ') || rest.starts_with('{') {
                return;
            }
        }
    }
    panic!(
        "Metric '{}' was expected to be present but was not found in metrics output",
        metric_name
    );
}

/// Poll `/metrics` until `metric_name` is present with a value >= `min`, or panic after
/// `timeout`. Polls every 100ms to react quickly while tolerating cache refresh jitter.
///
/// Returns the full metrics text from the successful scrape so callers can make additional
/// assertions without a second fetch.
pub async fn poll_until_metric_gte(
    monitoring_addr: SocketAddr,
    metric_name: &str,
    min: f64,
    timeout: std::time::Duration,
) -> String {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let metrics = fetch_metrics(monitoring_addr).await;
        if let Some(v) = parse_metric_value(&metrics, metric_name) {
            if v >= min {
                return metrics;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "Metric '{}' never reached >= {} within {:?}. Last /metrics response:\n{}",
                metric_name, min, timeout, metrics
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Assert that the `/api/v1/health` endpoint returns a response containing `"status":"ok"`.
pub async fn assert_api_health(monitoring_addr: SocketAddr) {
    let body = fetch_api(monitoring_addr, "/api/v1/health").await;
    assert!(
        body.contains("\"status\":\"ok\""),
        "Health endpoint should return ok status, got: {}",
        body
    );
}

/// Assert that the uptime metric is present and positive.
pub fn assert_uptime(metrics_text: &str) {
    assert_metric(
        metrics_text,
        "sv2_uptime_seconds",
        |v| v >= 0.0,
        ">= 0.0 (uptime should be non-negative)",
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_METRICS: &str = r#"# HELP sv2_uptime_seconds Server uptime in seconds
# TYPE sv2_uptime_seconds gauge
sv2_uptime_seconds 42
# HELP sv2_clients_total Total number of connected clients
# TYPE sv2_clients_total gauge
sv2_clients_total 3
# HELP sv2_server_channels Number of server channels by type
# TYPE sv2_server_channels gauge
sv2_server_channels{channel_type="extended"} 1
sv2_server_channels{channel_type="standard"} 0
# HELP sv2_client_shares_accepted_total Per-channel accepted shares
# TYPE sv2_client_shares_accepted_total gauge
sv2_client_shares_accepted_total{channel_id="1",user_identity="user1"} 5
"#;

    #[test]
    fn test_parse_simple_metric() {
        assert_eq!(
            parse_metric_value(SAMPLE_METRICS, "sv2_uptime_seconds"),
            Some(42.0)
        );
        assert_eq!(
            parse_metric_value(SAMPLE_METRICS, "sv2_clients_total"),
            Some(3.0)
        );
    }

    #[test]
    fn test_parse_labeled_metric() {
        assert_eq!(
            parse_metric_value(
                SAMPLE_METRICS,
                "sv2_server_channels{channel_type=\"extended\"}"
            ),
            Some(1.0)
        );
        assert_eq!(
            parse_metric_value(
                SAMPLE_METRICS,
                "sv2_server_channels{channel_type=\"standard\"}"
            ),
            Some(0.0)
        );
    }

    #[test]
    fn test_parse_missing_metric() {
        assert_eq!(
            parse_metric_value(SAMPLE_METRICS, "nonexistent_metric"),
            None
        );
    }

    #[test]
    fn test_assert_metric_gte() {
        assert_metric_gte(SAMPLE_METRICS, "sv2_clients_total", 1.0);
        assert_metric_gte(SAMPLE_METRICS, "sv2_clients_total", 3.0);
    }

    #[test]
    fn test_assert_metric_eq() {
        assert_metric_eq(SAMPLE_METRICS, "sv2_uptime_seconds", 42.0);
    }

    #[test]
    fn test_assert_metric_not_present() {
        assert_metric_not_present(SAMPLE_METRICS, "nonexistent_metric");
    }

    #[test]
    #[should_panic(expected = "was found in metrics output")]
    fn test_assert_metric_not_present_panics() {
        assert_metric_not_present(SAMPLE_METRICS, "sv2_clients_total");
    }

    #[test]
    fn test_assert_metric_present() {
        assert_metric_present(SAMPLE_METRICS, "sv2_clients_total");
        assert_metric_present(SAMPLE_METRICS, "sv2_server_channels");
    }

    #[test]
    #[should_panic(expected = "was expected to be present")]
    fn test_assert_metric_present_panics() {
        assert_metric_present(SAMPLE_METRICS, "nonexistent_metric");
    }

    #[test]
    fn test_assert_uptime() {
        assert_uptime(SAMPLE_METRICS);
    }

    #[test]
    fn test_no_false_prefix_match() {
        // sv2_clients_total should not match sv2_clients_total_extra
        let metrics = "sv2_clients_total_extra 99\n";
        assert_metric_not_present(metrics, "sv2_clients_total");
    }
}
