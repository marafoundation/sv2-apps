//! Helpers for querying and asserting on Prometheus metrics and JSON API endpoints
//! exposed by SV2 components during integration tests.

use std::{collections::HashMap, fmt, net::SocketAddr, time::Duration};
use stratum_apps::monitoring::{
    routes, GlobalInfo, ServerChannelsResponse, ServerResponse, Sv1ClientsResponse,
    Sv2ClientsResponse,
};

// Every monitoring HTTP endpoint has a typed production struct in
// `stratum_apps::monitoring` (`GlobalInfo`, `HealthResponse`, `RootResponse`,
// `ServerResponse`, `ServerChannelsResponse`, `Sv2ClientsResponse`, ...).
// Tests deserialize directly into those types via `fetch_api_typed` /
// `fetch_api_with_status_typed` rather than indexing into untyped JSON.
//
// Note on async: `minreq` is a synchronous HTTP client with no async variant,
// so all fetch helpers wrap it in `spawn_blocking` to avoid stalling the
// tokio runtime.

/// Fetch the raw Prometheus text-format metrics from a component's `/metrics` endpoint.
/// Uses `spawn_blocking` because `minreq` has no async variant.
/// Path is taken from `stratum_apps::monitoring::routes::METRICS`.
pub async fn fetch_metrics(monitoring_addr: SocketAddr) -> String {
    let url = format!("http://{}{}", monitoring_addr, routes::METRICS);
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

/// Fetch a JSON API endpoint and parse the response into a typed struct.
pub async fn fetch_api_typed<T: serde::de::DeserializeOwned>(
    monitoring_addr: SocketAddr,
    path: &str,
) -> T {
    let body = fetch_api(monitoring_addr, path).await;
    serde_json::from_str(&body).unwrap_or_else(|e| {
        panic!(
            "Failed to parse JSON from {} into {}: {}\nBody: {}",
            path,
            std::any::type_name::<T>(),
            e,
            body
        )
    })
}

/// Fetch a JSON API endpoint returning both the HTTP status code and the parsed body
/// deserialized into the caller-specified type `T`.
///
/// Unlike `fetch_api_typed`, this does **not** panic on non-2xx responses, so it can be
/// used to test error endpoints. For 404/error bodies, parametrize `T` with the
/// production `ErrorResponse` type to keep the assertion fully typed.
pub async fn fetch_api_with_status_typed<T: serde::de::DeserializeOwned + Send + 'static>(
    monitoring_addr: SocketAddr,
    path: &str,
) -> (i32, T) {
    let url = format!("http://{}{}", monitoring_addr, path);
    let type_name = std::any::type_name::<T>();
    tokio::task::spawn_blocking(move || {
        let (status, bytes) = crate::utils::http::make_get_request_with_status(&url, 5);
        let body = String::from_utf8(bytes).expect("api response should be valid UTF-8");
        let value: T = serde_json::from_str(&body).unwrap_or_else(|e| {
            panic!(
                "Failed to parse JSON from {} (status {}) into {}: {}\nBody: {}",
                url, status, type_name, e, body
            )
        });
        (status, value)
    })
    .await
    .expect("spawn_blocking for fetch_api_with_status_typed panicked")
}

/// Poll a JSON API endpoint until a numeric field at `json_pointer` (RFC 6901, e.g.
/// `"/sv2_clients/total_clients"`) reaches `>= min`. Returns the full JSON value once
/// satisfied. Panics if the condition is not met within `timeout`.
///
/// This is the JSON equivalent of `poll_until_metric_gte` — use it for endpoints whose
/// data only appears after the monitoring snapshot cache has refreshed.
pub async fn poll_until_api_field_gte(
    monitoring_addr: SocketAddr,
    path: &str,
    json_pointer: &str,
    min: f64,
    timeout: Duration,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        // Fetch via `_typed::<serde_json::Value>` so that transient non-2xx responses
        // (e.g. 404 before the snapshot cache has populated) are retried instead of
        // panicking. The pointer-based polling here is intentionally untyped because
        // it polls many different endpoints by JSON path; typed pollers are provided
        // for the common cases below.
        let (status, json) =
            fetch_api_with_status_typed::<serde_json::Value>(monitoring_addr, path).await;
        if (200..300).contains(&status) {
            if let Some(val) = json.pointer(json_pointer) {
                let num = val.as_f64().unwrap_or(0.0);
                if num >= min {
                    return json;
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "JSON field '{}' at {} never reached >= {} within {:?}. Last status: {}. Last response:\n{}",
                json_pointer,
                path,
                min,
                timeout,
                status,
                serde_json::to_string_pretty(&json).unwrap_or_default()
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Internal: poll `path` until `condition` is met, returning the parsed `T`.
/// Retries on non-2xx responses (endpoint may not be ready yet).
async fn poll_until<T, F>(
    monitoring_addr: SocketAddr,
    path: &'static str,
    timeout: Duration,
    condition: F,
    timeout_msg: &'static str,
) -> T
where
    T: serde::de::DeserializeOwned,
    F: Fn(&T) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let (status, body) = {
            let url = format!("http://{}{}", monitoring_addr, path);
            tokio::task::spawn_blocking(move || {
                crate::utils::http::make_get_request_with_status(&url, 5)
            })
            .await
            .expect("spawn_blocking panicked")
        };
        if (200..300).contains(&status) {
            let body_str = String::from_utf8(body).expect("response should be valid UTF-8");
            if let Ok(resp) = serde_json::from_str::<T>(&body_str) {
                if condition(&resp) {
                    return resp;
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("{} within {:?}", timeout_msg, timeout);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Poll `/api/v1/global` until `sv2_clients.total_clients >= min`.
/// Returns the parsed `GlobalInfo` once satisfied.
pub async fn poll_until_global_sv2_clients_gte(
    monitoring_addr: SocketAddr,
    min: usize,
    timeout: Duration,
) -> GlobalInfo {
    poll_until(
        monitoring_addr,
        routes::GLOBAL,
        timeout,
        move |r: &GlobalInfo| {
            r.sv2_clients
                .as_ref()
                .is_some_and(|c| c.total_clients >= min)
        },
        "GlobalInfo sv2_clients.total_clients never reached >= expected",
    )
    .await
}

/// Poll `/api/v1/global` until `sv1_clients.total_clients >= min`.
/// Returns the parsed `GlobalInfo` once satisfied.
pub async fn poll_until_global_sv1_clients_gte(
    monitoring_addr: SocketAddr,
    min: usize,
    timeout: Duration,
) -> GlobalInfo {
    poll_until(
        monitoring_addr,
        routes::GLOBAL,
        timeout,
        move |r: &GlobalInfo| {
            r.sv1_clients
                .as_ref()
                .is_some_and(|c| c.total_clients >= min)
        },
        "GlobalInfo sv1_clients.total_clients never reached >= expected",
    )
    .await
}

/// Poll `/api/v1/clients` until `total >= min`.
/// Returns the parsed `Sv2ClientsResponse` once satisfied.
pub async fn poll_until_clients_total_gte(
    monitoring_addr: SocketAddr,
    min: usize,
    timeout: Duration,
) -> Sv2ClientsResponse {
    poll_until(
        monitoring_addr,
        routes::CLIENTS,
        timeout,
        move |r: &Sv2ClientsResponse| r.total >= min,
        "Sv2ClientsResponse total never reached >= expected",
    )
    .await
}

/// Poll `/api/v1/sv1/clients` until `total >= min`.
/// Returns the parsed `Sv1ClientsResponse` once satisfied.
pub async fn poll_until_sv1_clients_total_gte(
    monitoring_addr: SocketAddr,
    min: usize,
    timeout: Duration,
) -> Sv1ClientsResponse {
    poll_until(
        monitoring_addr,
        routes::SV1_CLIENTS,
        timeout,
        move |r: &Sv1ClientsResponse| r.total >= min,
        "Sv1ClientsResponse total never reached >= expected",
    )
    .await
}

/// Poll `/api/v1/server` until `extended_channels_count >= min`.
/// Returns the parsed `ServerResponse` once satisfied.
pub async fn poll_until_server_channels_gte(
    monitoring_addr: SocketAddr,
    min: usize,
    timeout: Duration,
) -> ServerResponse {
    poll_until(
        monitoring_addr,
        routes::SERVER,
        timeout,
        move |r: &ServerResponse| r.extended_channels_count >= min,
        "ServerResponse extended_channels_count never reached >= expected",
    )
    .await
}

/// Poll `/api/v1/server/channels` until `total_extended >= min`.
/// Returns the parsed `ServerChannelsResponse` once satisfied.
pub async fn poll_until_server_channels_extended_gte(
    monitoring_addr: SocketAddr,
    min: usize,
    timeout: Duration,
) -> ServerChannelsResponse {
    poll_until(
        monitoring_addr,
        routes::SERVER_CHANNELS,
        timeout,
        move |r: &ServerChannelsResponse| r.total_extended >= min,
        "ServerChannelsResponse total_extended never reached >= expected",
    )
    .await
}

/// Poll `/api/v1/server/channels` until the first extended channel has
/// `shares_acknowledged >= shares_min`. This is stricter than
/// `poll_until_server_channels_extended_gte`: a channel can exist in the
/// snapshot before any shares have been processed by the monitoring cache.
/// Returns the parsed `ServerChannelsResponse` once satisfied.
pub async fn poll_until_server_channel_shares_gte(
    monitoring_addr: SocketAddr,
    shares_min: u32,
    timeout: Duration,
) -> ServerChannelsResponse {
    poll_until(
        monitoring_addr,
        routes::SERVER_CHANNELS,
        timeout,
        move |r: &ServerChannelsResponse| {
            r.extended_channels
                .first()
                .is_some_and(|ch| ch.shares_acknowledged >= shares_min)
        },
        "ServerChannelsResponse first extended channel shares_acknowledged never reached >= expected",
    )
    .await
}

/// A Prometheus metric selector: a metric name plus an optional set of label matchers.
///
/// Label matching is order-independent — the selector matches any exposition line
/// whose label set is a superset of the requested labels. A selector with no labels
/// matches any line for that metric (bare or labeled).
///
/// # Examples
///
/// ```
/// # use integration_tests_sv2::prometheus_metrics_assertions::Metric;
/// // Bare name (implicit via From<&str>):
/// let _: Metric = "sv2_clients_total".into();
///
/// // Specific labeled series:
/// let _ = Metric::with_labels(
///     "sv2_server_shares_accepted_total",
///     &[("channel_id", "1"), ("user_identity", "user1")],
/// );
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Metric<'a> {
    pub name: &'a str,
    pub labels: &'a [(&'a str, &'a str)],
}

impl<'a> Metric<'a> {
    /// Create a selector for a metric by bare name (matches any labels).
    pub const fn new(name: &'a str) -> Self {
        Self { name, labels: &[] }
    }

    /// Create a selector with specific label matchers. Matches lines whose label
    /// set is a superset of `labels`, regardless of label ordering.
    pub const fn with_labels(name: &'a str, labels: &'a [(&'a str, &'a str)]) -> Self {
        Self { name, labels }
    }

    /// Try to match a single Prometheus exposition line. Returns the parsed value
    /// if the line matches this selector, otherwise `None`.
    fn match_line(&self, line: &str) -> Option<f64> {
        let rest = line.strip_prefix(self.name)?;
        // The name must be a complete token: next char is whitespace, '{', or EOL.
        // This prevents e.g. `sv2_clients_total_extra` from matching `sv2_clients_total`.
        let is_labeled = rest.starts_with('{');
        let is_bare = rest.chars().next().is_none_or(|c| c.is_ascii_whitespace());
        if !is_labeled && !is_bare {
            return None;
        }

        // Parse the labels (if any) and the value portion.
        let (line_labels, value_part) = if is_labeled {
            let inner = rest.strip_prefix('{')?;
            let (block, after) = inner.split_once('}')?;
            (parse_label_block(block), after)
        } else {
            (HashMap::new(), rest)
        };

        // Selector labels must all appear on the line with matching values.
        for (k, v) in self.labels {
            if line_labels.get(*k).map(String::as_str) != Some(*v) {
                return None;
            }
        }

        value_part.split_whitespace().next()?.parse().ok()
    }
}

impl<'a> From<&'a str> for Metric<'a> {
    fn from(name: &'a str) -> Self {
        Metric::new(name)
    }
}

impl fmt::Display for Metric<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)?;
        if !self.labels.is_empty() {
            f.write_str("{")?;
            for (i, (k, v)) in self.labels.iter().enumerate() {
                if i > 0 {
                    f.write_str(",")?;
                }
                write!(f, "{}=\"{}\"", k, v)?;
            }
            f.write_str("}")?;
        }
        Ok(())
    }
}

/// Parse the inside of a Prometheus label block like `k1="v1",k2="v2"` into a map.
/// Supports the subset emitted by the `prometheus` crate: simple `k="v"` pairs with
/// no escape sequences in values (sufficient for our metrics).
fn parse_label_block(block: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let block = block.trim();
    if block.is_empty() {
        return out;
    }
    for pair in block.split(',') {
        let pair = pair.trim();
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        let v = v.trim().trim_start_matches('"').trim_end_matches('"');
        out.insert(k.trim().to_string(), v.to_string());
    }
    out
}

/// Parse a specific metric value from Prometheus text format.
/// Returns `None` if no line matches the selector.
pub(crate) fn parse_metric_value<'a, M: Into<Metric<'a>>>(
    metrics_text: &str,
    metric: M,
) -> Option<f64> {
    let metric = metric.into();
    for line in metrics_text.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(v) = metric.match_line(line) {
            return Some(v);
        }
    }
    None
}

/// Assert that a metric is present and its value satisfies the given predicate.
#[track_caller]
pub(crate) fn assert_metric<'a, M, F>(
    metrics_text: &str,
    metric: M,
    predicate: F,
    description: &str,
) where
    M: Into<Metric<'a>>,
    F: Fn(f64) -> bool,
{
    let metric = metric.into();
    match parse_metric_value(metrics_text, metric) {
        Some(v) => {
            assert!(
                predicate(v),
                "Metric '{}' has value {} but expected: {}",
                metric,
                v,
                description
            );
        }
        None => {
            panic!(
                "Metric '{}' not found in metrics output. Expected: {}",
                metric, description
            );
        }
    }
}

/// Assert that a metric is present with the exact given value.
pub fn assert_metric_eq<'a, M: Into<Metric<'a>>>(metrics_text: &str, metric: M, expected: f64) {
    assert_metric(
        metrics_text,
        metric,
        |v| (v - expected).abs() < f64::EPSILON,
        &format!("== {}", expected),
    );
}

/// Assert that no exposition line matches the selector.
///
/// For a bare-name selector (`Metric::new("name")` or `"name".into()`), this means
/// the metric name does not appear at all. For a labeled selector, it means no line
/// with matching labels exists — other series for the same metric name are allowed.
#[track_caller]
pub fn assert_metric_not_present<'a, M: Into<Metric<'a>>>(metrics_text: &str, metric: M) {
    let metric = metric.into();
    for line in metrics_text.lines() {
        if line.starts_with('#') {
            continue;
        }
        if metric.match_line(line).is_some() {
            panic!(
                "Metric '{}' was found in metrics output but was expected to be absent. Line: {}",
                metric, line
            );
        }
    }
}

/// Assert that at least one exposition line matches the selector.
#[track_caller]
pub fn assert_metric_present<'a, M: Into<Metric<'a>>>(metrics_text: &str, metric: M) {
    let metric = metric.into();
    for line in metrics_text.lines() {
        if line.starts_with('#') {
            continue;
        }
        if metric.match_line(line).is_some() {
            return;
        }
    }
    panic!(
        "Metric '{}' was expected to be present but was not found in metrics output",
        metric
    );
}

/// Poll `/metrics` until a line matching `metric` has value >= `min`, or panic after
/// `timeout`. Polls every 100ms to react quickly while tolerating cache refresh jitter.
///
/// Returns the full metrics text from the successful scrape so callers can make additional
/// assertions without a second fetch.
pub async fn poll_until_metric_gte<'a, M: Into<Metric<'a>>>(
    monitoring_addr: SocketAddr,
    metric: M,
    min: f64,
    timeout: Duration,
) -> String {
    let metric = metric.into();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let metrics = fetch_metrics(monitoring_addr).await;
        if let Some(v) = parse_metric_value(&metrics, metric) {
            if v >= min {
                return metrics;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "Metric '{}' never reached >= {} within {:?}. Last /metrics response:\n{}",
                metric, min, timeout, metrics
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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
                Metric::with_labels("sv2_server_channels", &[("channel_type", "extended")])
            ),
            Some(1.0)
        );
        assert_eq!(
            parse_metric_value(
                SAMPLE_METRICS,
                Metric::with_labels("sv2_server_channels", &[("channel_type", "standard")])
            ),
            Some(0.0)
        );
    }

    #[test]
    fn test_label_order_independence() {
        // Selector requesting labels in opposite order to the exposition line
        // must still match — the prometheus crate emits in BTreeMap order today,
        // but tests should not silently break if that ever changes.
        assert_eq!(
            parse_metric_value(
                SAMPLE_METRICS,
                Metric::with_labels(
                    "sv2_client_shares_accepted_total",
                    &[("user_identity", "user1"), ("channel_id", "1")],
                )
            ),
            Some(5.0)
        );
    }

    #[test]
    fn test_label_subset_match() {
        // Querying only a subset of labels still matches.
        assert_eq!(
            parse_metric_value(
                SAMPLE_METRICS,
                Metric::with_labels("sv2_client_shares_accepted_total", &[("channel_id", "1")])
            ),
            Some(5.0)
        );
    }

    #[test]
    fn test_label_mismatch_returns_none() {
        assert_eq!(
            parse_metric_value(
                SAMPLE_METRICS,
                Metric::with_labels("sv2_server_channels", &[("channel_type", "nonexistent")])
            ),
            None
        );
    }

    #[test]
    fn test_bare_selector_matches_labeled_line() {
        // A bare-name selector matches any series for that metric (returns the
        // first one found).
        assert_eq!(
            parse_metric_value(SAMPLE_METRICS, "sv2_server_channels"),
            Some(1.0)
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
    fn test_no_false_prefix_match() {
        // sv2_clients_total should not match sv2_clients_total_extra
        let metrics = "sv2_clients_total_extra 99\n";
        assert_metric_not_present(metrics, "sv2_clients_total");
    }
}
