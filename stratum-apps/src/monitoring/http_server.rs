//! HTTP server for exposing monitoring data using Axum

use super::{
    client::{
        ClientInfo, ClientMetadata, ClientsMonitoring, ClientsSummary, ExtendedChannelInfo,
        StandardChannelInfo,
    },
    prometheus_metrics::PrometheusMetrics,
    server::{
        ServerExtendedChannelInfo, ServerMonitoring, ServerStandardChannelInfo, ServerSummary,
    },
    sv1::{Sv1ClientInfo, Sv1ClientsMonitoring, Sv1ClientsSummary},
    ChannelMetrics, GlobalInfo,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::get,
    Router,
};
use prometheus::{Encoder, TextEncoder};
use serde::Deserialize;
use std::{
    collections::HashSet,
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tracing::info;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "SRI Monitoring API",
        version = "0.1.0",
        description = "HTTP JSON API for monitoring SV2 applications"
    ),
    paths(
        handle_health,
        handle_global,
        handle_server,
        handle_server_channels,
        handle_clients,
        handle_client_by_id,
        handle_client_channels,
        handle_sv1_clients,
        handle_sv1_client_by_id,
    ),
    components(schemas(
        GlobalInfo,
        ServerSummary,
        ClientsSummary,
        ServerExtendedChannelInfo,
        ServerStandardChannelInfo,
        ClientInfo,
        ClientMetadata,
        ExtendedChannelInfo,
        StandardChannelInfo,
        Sv1ClientInfo,
        Sv1ClientsSummary,
        HealthResponse,
        ErrorResponse,
        ServerResponse,
        ServerChannelsResponse,
        ClientsResponse,
        ClientResponse,
        ClientChannelsResponse,
        Sv1ClientsResponse,
    )),
    tags(
        (name = "health", description = "Health check endpoints"),
        (name = "global", description = "Global statistics"),
        (name = "server", description = "Server (upstream) monitoring"),
        (name = "clients", description = "Clients (downstream) monitoring"),
        (name = "sv1", description = "Sv1 clients monitoring (Translator Proxy only)")
    )
)]
struct ApiDoc;

/// Shared state for all HTTP handlers
#[derive(Clone)]
struct ServerState {
    server_monitoring: Option<Arc<dyn ServerMonitoring + Send + Sync + 'static>>,
    clients_monitoring: Option<Arc<dyn ClientsMonitoring + Send + Sync + 'static>>,
    sv1_monitoring: Option<Arc<dyn Sv1ClientsMonitoring + Send + Sync + 'static>>,
    start_time: u64,
    metrics: PrometheusMetrics,
    // Track active label combinations to clean up stale metrics
    server_channel_labels: Arc<Mutex<HashSet<(String, String)>>>,
    client_channel_labels: Arc<Mutex<HashSet<(String, String, String)>>>,
}

const DEFAULT_LIMIT: usize = 25;
const MAX_LIMIT: usize = 100;

#[derive(Deserialize, IntoParams)]
struct Pagination {
    /// Offset for pagination (default: 0)
    #[serde(default)]
    offset: usize,
    /// Limit for pagination (default: 25, max: 100)
    #[serde(default)]
    limit: Option<usize>,
}

impl Pagination {
    fn effective_limit(&self) -> usize {
        self.limit
            .map(|l| l.min(MAX_LIMIT))
            .unwrap_or(DEFAULT_LIMIT)
    }
}

fn paginate<T: Clone>(items: &[T], params: &Pagination) -> (usize, Vec<T>) {
    let total = items.len();
    let limit = params.effective_limit();
    let offset = params.offset.min(total);
    let sliced = items
        .iter()
        .skip(offset)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();
    (total, sliced)
}

/// HTTP server that exposes monitoring data as JSON
pub struct MonitoringServer {
    bind_address: SocketAddr,
    state: ServerState,
}

impl MonitoringServer {
    /// Create a new monitoring server
    ///
    /// Returns a server that exposes monitoring data via HTTP JSON API. Chain with
    /// `with_sv1_monitoring()` for SV1 support, then call `run()` to start.
    pub fn new(
        bind_address: SocketAddr,
        server_monitoring: Option<Arc<dyn ServerMonitoring + Send + Sync + 'static>>,
        clients_monitoring: Option<Arc<dyn ClientsMonitoring + Send + Sync + 'static>>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Only register metrics for available monitoring types
        let metrics = PrometheusMetrics::new(
            server_monitoring.is_some(),
            clients_monitoring.is_some(),
            false, // SV1 metrics added later via with_sv1_monitoring
        )?;

        Ok(Self {
            bind_address,
            state: ServerState {
                server_monitoring,
                clients_monitoring,
                sv1_monitoring: None,
                start_time,
                metrics,
                server_channel_labels: Arc::new(Mutex::new(HashSet::new())),
                client_channel_labels: Arc::new(Mutex::new(HashSet::new())),
            },
        })
    }

    /// Add SV1 client monitoring (optional, for Translator Proxy only)
    pub fn with_sv1_monitoring(
        mut self,
        sv1_monitoring: Arc<dyn Sv1ClientsMonitoring + Send + Sync + 'static>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        self.state.sv1_monitoring = Some(sv1_monitoring);

        // Re-create metrics with SV1 enabled
        self.state.metrics = PrometheusMetrics::new(
            self.state.server_monitoring.is_some(),
            self.state.clients_monitoring.is_some(),
            true, // Enable SV1 metrics
        )?;

        Ok(self)
    }

    /// Run the monitoring server until the shutdown signal completes
    ///
    /// Starts an HTTP server that exposes monitoring data as JSON.
    /// The server shuts down gracefully when `shutdown_signal` completes.
    ///
    /// Automatically exposes:
    /// - Swagger UI at `/swagger-ui`
    /// - OpenAPI spec at `/api-docs/openapi.json`
    /// - Prometheus metrics at `/metrics`
    pub async fn run(
        self,
        shutdown_signal: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting monitoring server on http://{}", self.bind_address);

        // Versioned JSON API under /api/v1
        let api_v1 = Router::new()
            .route("/health", get(handle_health))
            .route("/global", get(handle_global))
            .route("/server", get(handle_server))
            .route("/server/channels", get(handle_server_channels))
            .route("/clients", get(handle_clients))
            .route("/clients/{client_id}", get(handle_client_by_id))
            .route("/clients/{client_id}/channels", get(handle_client_channels))
            .route("/sv1/clients", get(handle_sv1_clients))
            .route("/sv1/clients/{client_id}", get(handle_sv1_client_by_id));

        let app = Router::new()
            .route("/", get(handle_root))
            .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
            .nest("/api/v1", api_v1)
            .route("/metrics", get(handle_prometheus_metrics))
            .with_state(self.state);

        let listener = TcpListener::bind(self.bind_address).await?;

        info!(
            "Swagger UI available at http://{}/swagger-ui",
            self.bind_address
        );
        info!(
            "Prometheus metrics available at http://{}/metrics",
            self.bind_address
        );

        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                shutdown_signal.await;
                info!("Monitoring server received shutdown signal, stopping...");
            })
            .await?;

        info!("Monitoring server stopped");
        Ok(())
    }
}

// Response types - used for both actual responses and OpenAPI documentation
#[derive(serde::Serialize, ToSchema)]
struct HealthResponse {
    status: String,
    timestamp: u64,
}

#[derive(serde::Serialize, ToSchema)]
struct ErrorResponse {
    error: String,
}

#[derive(serde::Serialize, ToSchema)]
struct ServerResponse {
    extended_channels_count: usize,
    standard_channels_count: usize,
    total_hashrate: f32,
}

#[derive(serde::Serialize, ToSchema)]
struct ServerChannelsResponse {
    offset: usize,
    limit: usize,
    total_extended: usize,
    total_standard: usize,
    extended_channels: Vec<ServerExtendedChannelInfo>,
    standard_channels: Vec<ServerStandardChannelInfo>,
}

#[derive(serde::Serialize, ToSchema)]
struct ClientsResponse {
    offset: usize,
    limit: usize,
    total: usize,
    items: Vec<ClientMetadata>,
}

#[derive(serde::Serialize, ToSchema)]
struct ClientResponse {
    client_id: usize,
    extended_channels_count: usize,
    standard_channels_count: usize,
    total_hashrate: f32,
}

#[derive(serde::Serialize, ToSchema)]
struct ClientChannelsResponse {
    client_id: usize,
    offset: usize,
    limit: usize,
    total_extended: usize,
    total_standard: usize,
    extended_channels: Vec<ExtendedChannelInfo>,
    standard_channels: Vec<StandardChannelInfo>,
}

#[derive(serde::Serialize, ToSchema)]
struct Sv1ClientsResponse {
    offset: usize,
    limit: usize,
    total: usize,
    items: Vec<Sv1ClientInfo>,
}

/// Root endpoint - lists all available APIs
async fn handle_root() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "service": "SRI Monitoring API",
        "version": "0.1.0",
        "endpoints": {
            "/": "This endpoint - API listing",
            "/swagger-ui": "Swagger UI (interactive API documentation)",
            "/api-docs/openapi.json": "OpenAPI specification",
            "/api/v1/health": "Health check",
            "/api/v1/global": "Global statistics",
            "/api/v1/server": "Server metadata",
            "/api/v1/server/channels": "Server channels (paginated)",
            "/api/v1/clients": "All Sv2 clients metadata (paginated)",
            "/api/v1/clients/{id}": "Single Sv2 client metadata",
            "/api/v1/clients/{id}/channels": "Sv2 client channels (paginated)",
            "/api/v1/sv1/clients": "Sv1 clients (Translator Proxy only, paginated)",
            "/api/v1/sv1/clients/{id}": "Single Sv1 client (Translator Proxy only)",
            "/metrics": "Prometheus metrics"
        }
    }))
}

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/api/v1/health",
    tag = "health",
    responses(
        (status = 200, description = "Service is healthy", body = HealthResponse)
    )
)]
async fn handle_health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    })
}

/// Get global statistics
#[utoipa::path(
    get,
    path = "/api/v1/global",
    tag = "global",
    responses(
        (status = 200, description = "Global statistics", body = GlobalInfo)
    )
)]
async fn handle_global(State(state): State<ServerState>) -> Json<GlobalInfo> {
    let uptime_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        - state.start_time;

    let clients = state
        .clients_monitoring
        .as_ref()
        .map(|m| m.get_clients_summary())
        .unwrap_or_else(|| ClientsSummary {
            total_clients: 0,
            total_channels: 0,
            extended_channels: 0,
            standard_channels: 0,
            total_hashrate: 0.0,
        });

    let server = state
        .server_monitoring
        .as_ref()
        .map(|m| m.get_server_summary())
        .unwrap_or_else(|| ServerSummary {
            total_channels: 0,
            extended_channels: 0,
            standard_channels: 0,
            total_hashrate: 0.0,
        });

    Json(GlobalInfo {
        server,
        clients,
        uptime_secs,
    })
}

/// Get server (upstream) metadata - use /server/channels for channel details
#[utoipa::path(
    get,
    path = "/api/v1/server",
    tag = "server",
    responses(
        (status = 200, description = "Server metadata", body = ServerResponse),
        (status = 404, description = "Server monitoring not available", body = ErrorResponse)
    )
)]
async fn handle_server(State(state): State<ServerState>) -> Response {
    match &state.server_monitoring {
        Some(monitoring) => {
            let summary = monitoring.get_server_summary();

            Json(ServerResponse {
                extended_channels_count: summary.extended_channels,
                standard_channels_count: summary.standard_channels,
                total_hashrate: summary.total_hashrate,
            })
            .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Server monitoring not available".to_string(),
            }),
        )
            .into_response(),
    }
}

/// Get server channels (paginated)
#[utoipa::path(
    get,
    path = "/api/v1/server/channels",
    tag = "server",
    params(Pagination),
    responses(
        (status = 200, description = "Server channels (paginated)", body = ServerChannelsResponse),
        (status = 404, description = "Server monitoring not available", body = ErrorResponse)
    )
)]
async fn handle_server_channels(
    Query(params): Query<Pagination>,
    State(state): State<ServerState>,
) -> Response {
    match &state.server_monitoring {
        Some(monitoring) => {
            let server = monitoring.get_server();

            let (total_extended, extended_channels) = paginate(&server.extended_channels, &params);
            let (total_standard, standard_channels) = paginate(&server.standard_channels, &params);

            Json(ServerChannelsResponse {
                offset: params.offset,
                limit: params.effective_limit(),
                total_extended,
                total_standard,
                extended_channels,
                standard_channels,
            })
            .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Server monitoring not available".to_string(),
            }),
        )
            .into_response(),
    }
}

/// Get all clients (downstream) - returns metadata only, use /clients/{id}/channels for channels
#[utoipa::path(
    get,
    path = "/api/v1/clients",
    tag = "clients",
    params(Pagination),
    responses(
        (status = 200, description = "List of clients (metadata only)", body = ClientsResponse),
        (status = 404, description = "Clients monitoring not available", body = ErrorResponse)
    )
)]
async fn handle_clients(
    Query(params): Query<Pagination>,
    State(state): State<ServerState>,
) -> Response {
    match &state.clients_monitoring {
        Some(monitoring) => {
            let clients: Vec<ClientMetadata> = monitoring
                .get_clients()
                .iter()
                .map(|c| c.to_metadata())
                .collect();
            let (total, items) = paginate(&clients, &params);

            Json(ClientsResponse {
                offset: params.offset,
                limit: params.effective_limit(),
                total,
                items,
            })
            .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Clients monitoring not available".to_string(),
            }),
        )
            .into_response(),
    }
}

/// Get a single client by ID - returns metadata only, use /clients/{id}/channels for channels
#[utoipa::path(
    get,
    path = "/api/v1/clients/{client_id}",
    tag = "clients",
    params(
        ("client_id" = usize, Path, description = "Client ID")
    ),
    responses(
        (status = 200, description = "Client metadata", body = ClientResponse),
        (status = 404, description = "Client not found", body = ErrorResponse)
    )
)]
async fn handle_client_by_id(
    Path(client_id): Path<usize>,
    State(state): State<ServerState>,
) -> Response {
    match &state.clients_monitoring {
        Some(monitoring) => match monitoring.get_client_by_id(client_id) {
            Some(client) => Json(ClientResponse {
                client_id,
                extended_channels_count: client.extended_channels.len(),
                standard_channels_count: client.standard_channels.len(),
                total_hashrate: client.total_hashrate(),
            })
            .into_response(),
            None => (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Client {} not found", client_id),
                }),
            )
                .into_response(),
        },
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Clients monitoring not available".to_string(),
            }),
        )
            .into_response(),
    }
}

/// Get channels for a specific client (paginated)
#[utoipa::path(
    get,
    path = "/api/v1/clients/{client_id}/channels",
    tag = "clients",
    params(
        ("client_id" = usize, Path, description = "Client ID"),
        Pagination
    ),
    responses(
        (status = 200, description = "Client channels (paginated)", body = ClientChannelsResponse),
        (status = 404, description = "Client not found", body = ErrorResponse)
    )
)]
async fn handle_client_channels(
    Path(client_id): Path<usize>,
    Query(params): Query<Pagination>,
    State(state): State<ServerState>,
) -> Response {
    match &state.clients_monitoring {
        Some(monitoring) => match monitoring.get_client_by_id(client_id) {
            Some(client) => {
                let (total_extended, extended_channels) =
                    paginate(&client.extended_channels, &params);
                let (total_standard, standard_channels) =
                    paginate(&client.standard_channels, &params);

                Json(ClientChannelsResponse {
                    client_id,
                    offset: params.offset,
                    limit: params.effective_limit(),
                    total_extended,
                    total_standard,
                    extended_channels,
                    standard_channels,
                })
                .into_response()
            }
            None => (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Client {} not found", client_id),
                }),
            )
                .into_response(),
        },
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Clients monitoring not available".to_string(),
            }),
        )
            .into_response(),
    }
}

/// Get Sv1 clients (Translator Proxy only)
#[utoipa::path(
    get,
    path = "/api/v1/sv1/clients",
    tag = "sv1",
    params(Pagination),
    responses(
        (status = 200, description = "List of Sv1 clients", body = Sv1ClientsResponse),
        (status = 404, description = "Sv1 monitoring not available", body = ErrorResponse)
    )
)]
async fn handle_sv1_clients(
    Query(params): Query<Pagination>,
    State(state): State<ServerState>,
) -> Response {
    match &state.sv1_monitoring {
        Some(monitoring) => {
            let clients = monitoring.get_sv1_clients();
            let (total, items) = paginate(&clients, &params);

            Json(Sv1ClientsResponse {
                offset: params.offset,
                limit: params.effective_limit(),
                total,
                items,
            })
            .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "SV1 client monitoring not available".to_string(),
            }),
        )
            .into_response(),
    }
}

/// Get a single Sv1 client by ID
#[utoipa::path(
    get,
    path = "/api/v1/sv1/clients/{client_id}",
    tag = "sv1",
    params(
        ("client_id" = usize, Path, description = "Sv1 client ID")
    ),
    responses(
        (status = 200, description = "Sv1 client details", body = Sv1ClientInfo),
        (status = 404, description = "Sv1 client not found", body = ErrorResponse)
    )
)]
async fn handle_sv1_client_by_id(
    Path(client_id): Path<usize>,
    State(state): State<ServerState>,
) -> Response {
    match &state.sv1_monitoring {
        Some(monitoring) => match monitoring.get_sv1_client_by_id(client_id) {
            Some(client) => Json(client).into_response(),
            None => (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Sv1 client {} not found", client_id),
                }),
            )
                .into_response(),
        },
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "SV1 client monitoring not available".to_string(),
            }),
        )
            .into_response(),
    }
}

/// Generic helper to collect server channel metrics
fn collect_server_channels<C: ChannelMetrics>(
    channels: &[C],
    metrics: &PrometheusMetrics,
    labels: &mut HashSet<(String, String)>,
) {
    for channel in channels {
        let channel_id = channel.channel_id().to_string();
        let user = channel.user_identity().to_string();

        labels.insert((channel_id.clone(), user.clone()));

        if let Some(ref metric) = metrics.sv2_server_shares_accepted_total {
            metric
                .with_label_values(&[&channel_id, &user])
                .set(channel.shares_accepted() as f64);
        }
        if let Some(ref metric) = metrics.sv2_server_channel_hashrate {
            metric
                .with_label_values(&[&channel_id, &user])
                .set(channel.nominal_hashrate() as f64);
        }
    }
}

/// Helper function to collect server channel metrics and track active labels
fn collect_server_channel_metrics(
    server: &super::server::ServerInfo,
    metrics: &PrometheusMetrics,
    labels: &mut HashSet<(String, String)>,
) {
    collect_server_channels(&server.extended_channels, metrics, labels);
    collect_server_channels(&server.standard_channels, metrics, labels);
}

/// Generic helper to collect client channel metrics (works for both extended and standard)
fn collect_client_channels<C: ChannelMetrics>(
    client_id: &str,
    channels: &[C],
    metrics: &PrometheusMetrics,
    labels: &mut HashSet<(String, String, String)>,
) {
    for channel in channels {
        let channel_id = channel.channel_id().to_string();
        let user = channel.user_identity().to_string();

        labels.insert((client_id.to_string(), channel_id.clone(), user.clone()));

        if let Some(ref metric) = metrics.sv2_client_shares_accepted_total {
            metric
                .with_label_values(&[client_id, &channel_id, &user])
                .set(channel.shares_accepted() as f64);
        }
        if let Some(ref metric) = metrics.sv2_client_channel_hashrate {
            metric
                .with_label_values(&[client_id, &channel_id, &user])
                .set(channel.nominal_hashrate() as f64);
        }
        if let Some(ref metric) = metrics.sv2_client_channel_shares_per_minute {
            metric
                .with_label_values(&[client_id, &channel_id, &user])
                .set(channel.shares_per_minute() as f64);
        }
    }
}

/// Generic helper to clean up stale metrics
fn cleanup_stale_metrics<L, F>(old_labels: &HashSet<L>, new_labels: &HashSet<L>, cleanup_fn: F)
where
    L: std::hash::Hash + Eq + Clone,
    F: Fn(&L),
{
    let stale_labels: Vec<_> = old_labels.difference(new_labels).cloned().collect();
    for label in &stale_labels {
        cleanup_fn(label);
    }
}

/// Helper function to clean up stale server metrics
fn cleanup_stale_server_metrics(
    old_labels: &HashSet<(String, String)>,
    new_labels: &HashSet<(String, String)>,
    metrics: &PrometheusMetrics,
) {
    cleanup_stale_metrics(old_labels, new_labels, |(channel_id, user)| {
        if let Some(ref metric) = metrics.sv2_server_shares_accepted_total {
            let _ = metric.remove_label_values(&[channel_id, user]);
        }
        if let Some(ref metric) = metrics.sv2_server_channel_hashrate {
            let _ = metric.remove_label_values(&[channel_id, user]);
        }
    });
}

/// Helper function to clean up stale client metrics
fn cleanup_stale_client_metrics(
    old_labels: &HashSet<(String, String, String)>,
    new_labels: &HashSet<(String, String, String)>,
    metrics: &PrometheusMetrics,
) {
    cleanup_stale_metrics(old_labels, new_labels, |(client_id, channel_id, user)| {
        if let Some(ref metric) = metrics.sv2_client_shares_accepted_total {
            let _ = metric.remove_label_values(&[client_id, channel_id, user]);
        }
        if let Some(ref metric) = metrics.sv2_client_channel_hashrate {
            let _ = metric.remove_label_values(&[client_id, channel_id, user]);
        }
        if let Some(ref metric) = metrics.sv2_client_channel_shares_per_minute {
            let _ = metric.remove_label_values(&[client_id, channel_id, user]);
        }
    });
}

/// Handler for Prometheus metrics endpoint
async fn handle_prometheus_metrics(State(state): State<ServerState>) -> Response {
    let uptime_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        - state.start_time;
    state.metrics.sv2_uptime_seconds.set(uptime_secs as f64);

    // Clean up stale metrics before repopulating
    // We track which label combinations are currently active, and remove any that are no longer present
    let mut new_server_labels: HashSet<(String, String)> = HashSet::new();
    let mut new_client_labels: HashSet<(String, String, String)> = HashSet::new();

    // Collect server metrics
    if let Some(monitoring) = &state.server_monitoring {
        let summary = monitoring.get_server_summary();
        if let Some(ref metric) = state.metrics.sv2_server_channels_total {
            metric.set(summary.total_channels as f64);
        }
        if let Some(ref metric) = state.metrics.sv2_server_channels_extended {
            metric.set(summary.extended_channels as f64);
        }
        if let Some(ref metric) = state.metrics.sv2_server_channels_standard {
            metric.set(summary.standard_channels as f64);
        }
        if let Some(ref metric) = state.metrics.sv2_server_hashrate_total {
            metric.set(summary.total_hashrate as f64);
        }

        let server = monitoring.get_server();
        collect_server_channel_metrics(&server, &state.metrics, &mut new_server_labels);
    }

    // Collect clients metrics
    if let Some(monitoring) = &state.clients_monitoring {
        let summary = monitoring.get_clients_summary();
        if let Some(ref metric) = state.metrics.sv2_clients_total {
            metric.set(summary.total_clients as f64);
        }
        if let Some(ref metric) = state.metrics.sv2_client_channels_total {
            metric.set(summary.total_channels as f64);
        }
        if let Some(ref metric) = state.metrics.sv2_client_channels_extended {
            metric.set(summary.extended_channels as f64);
        }
        if let Some(ref metric) = state.metrics.sv2_client_channels_standard {
            metric.set(summary.standard_channels as f64);
        }
        if let Some(ref metric) = state.metrics.sv2_client_hashrate_total {
            metric.set(summary.total_hashrate as f64);
        }

        let clients = monitoring.get_clients();
        for client in &clients {
            let client_id = client.client_id.to_string();
            collect_client_channels(
                &client_id,
                &client.extended_channels,
                &state.metrics,
                &mut new_client_labels,
            );
            collect_client_channels(
                &client_id,
                &client.standard_channels,
                &state.metrics,
                &mut new_client_labels,
            );
        }
    }

    // Collect SV1 client metrics
    if let Some(monitoring) = &state.sv1_monitoring {
        let summary = monitoring.get_sv1_clients_summary();
        if let Some(ref metric) = state.metrics.sv1_clients_total {
            metric.set(summary.total_clients as f64);
        }
        if let Some(ref metric) = state.metrics.sv1_hashrate_total {
            metric.set(summary.total_hashrate as f64);
        }
    }

    // Clean up stale metrics by removing label combinations that are no longer active
    // This prevents memory leaks when miners reconnect on different channels
    if let Ok(mut old_server_labels) = state.server_channel_labels.lock() {
        cleanup_stale_server_metrics(&old_server_labels, &new_server_labels, &state.metrics);
        *old_server_labels = new_server_labels;
    }

    if let Ok(mut old_client_labels) = state.client_channel_labels.lock() {
        cleanup_stale_client_metrics(&old_client_labels, &new_client_labels, &state.metrics);
        *old_client_labels = new_client_labels;
    }

    // Encode and return metrics
    let encoder = TextEncoder::new();
    let metric_families = state.metrics.registry.gather();
    let mut buffer = Vec::new();

    match encoder.encode(&metric_families, &mut buffer) {
        Ok(_) => match String::from_utf8(buffer) {
            Ok(metrics_text) => (StatusCode::OK, metrics_text).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("UTF-8 error: {}", e),
                }),
            )
                .into_response(),
        },
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Encoding error: {}", e),
            }),
        )
            .into_response(),
    }
}
