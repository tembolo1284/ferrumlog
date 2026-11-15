//! The plain HTTP endpoints: `/metrics` for Prometheus and `/healthz` for
//! load balancers and container health checks.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

use crate::metrics::Metrics;
use crate::shutdown::Shutdown;

#[derive(Clone)]
struct HttpState {
    metrics: Arc<Metrics>,
    shutdown: Shutdown,
}

/// Builds the router for the HTTP endpoints.
pub(crate) fn router(metrics: Arc<Metrics>, shutdown: Shutdown) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(health_handler))
        .with_state(HttpState { metrics, shutdown })
}

async fn metrics_handler(State(state): State<HttpState>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(),
    )
}

/// Reports 200 while the node is serving and 503 once it is shutting down,
/// so a load balancer stops sending it traffic.
async fn health_handler(State(state): State<HttpState>) -> impl IntoResponse {
    if state.shutdown.is_triggered() {
        (StatusCode::SERVICE_UNAVAILABLE, "shutting down\n")
    } else {
        (StatusCode::OK, "ok\n")
    }
}
