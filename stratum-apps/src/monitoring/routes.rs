//! Canonical path constants and helpers for the monitoring HTTP API.
//!
//! Centralising these constants here lets the server router, in-crate unit
//! tests, and downstream integration tests all reference the same strings.
//! New endpoints must be added here so every consumer stays in sync.

// ── Top-level paths ─────────────────────────────────────────────────

pub const ROOT: &str = "/";
pub const METRICS: &str = "/metrics";
pub const SWAGGER_UI: &str = "/swagger-ui";
pub const OPENAPI_SPEC: &str = "/api-docs/openapi.json";

// ── /api/v1 prefix ──────────────────────────────────────────────────

/// Common prefix for all versioned JSON API endpoints.
pub const API_V1_PREFIX: &str = "/api/v1";

/// Path segments relative to [`API_V1_PREFIX`], intended for use with
/// `axum::Router::nest(API_V1_PREFIX, ...)`. Kept in their own module so the
/// namespace (not a name prefix) signals that these strings are *not* full
/// URL paths and are not interchangeable with the full-path constants below.
pub mod segments {
    pub const HEALTH: &str = "/health";
    pub const GLOBAL: &str = "/global";
    pub const SERVER: &str = "/server";
    pub const SERVER_CHANNELS: &str = "/server/channels";
    pub const CLIENTS: &str = "/clients";
    pub const CLIENT_BY_ID: &str = "/clients/{client_id}";
    pub const CLIENT_CHANNELS: &str = "/clients/{client_id}/channels";
    pub const SV1_CLIENTS: &str = "/sv1/clients";
    pub const SV1_CLIENT_BY_ID: &str = "/sv1/clients/{client_id}";
}

// ── Full paths under /api/v1 ────────────────────────────────────────

pub const HEALTH: &str = "/api/v1/health";
pub const GLOBAL: &str = "/api/v1/global";
pub const SERVER: &str = "/api/v1/server";
pub const SERVER_CHANNELS: &str = "/api/v1/server/channels";
pub const CLIENTS: &str = "/api/v1/clients";
pub const SV1_CLIENTS: &str = "/api/v1/sv1/clients";

// Templated full paths (with `{client_id}` placeholder) — used in API
// listings exposed by the root endpoint and anywhere a human-readable
// path-with-placeholder is needed.
pub const CLIENT_BY_ID_PATTERN: &str = "/api/v1/clients/{client_id}";
pub const CLIENT_CHANNELS_PATTERN: &str = "/api/v1/clients/{client_id}/channels";
pub const SV1_CLIENT_BY_ID_PATTERN: &str = "/api/v1/sv1/clients/{client_id}";

/// Path for `/api/v1/clients/{id}` with the id substituted in.
pub fn client_by_id(id: usize) -> String {
    format!("/api/v1/clients/{id}")
}

/// Path for `/api/v1/clients/{id}/channels` with the id substituted in.
pub fn client_channels(id: usize) -> String {
    format!("/api/v1/clients/{id}/channels")
}

/// Path for `/api/v1/sv1/clients/{id}` with the id substituted in.
pub fn sv1_client_by_id(id: usize) -> String {
    format!("/api/v1/sv1/clients/{id}")
}
