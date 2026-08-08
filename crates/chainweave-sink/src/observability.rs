use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde::Serialize;
use thiserror::Error;
use tokio::{net::TcpListener, sync::RwLock};

#[derive(Debug, Clone, Default)]
pub struct HealthState {
    inner: Arc<RwLock<StatusSnapshot>>,
}

#[derive(Debug, Clone, Default)]
struct StatusSnapshot {
    healthy: bool,
    ready: bool,
}

#[derive(Debug)]
pub struct ObservabilityServer {
    listener: TcpListener,
    router: Router,
}

#[derive(Debug, Error)]
pub enum ObservabilityError {
    #[error("failed to install Prometheus recorder: {0}")]
    Metrics(String),
    #[error("failed to bind observability server on {address}: {source}")]
    Bind {
        address: SocketAddr,
        source: std::io::Error,
    },
    #[error("observability server failed: {0}")]
    Serve(#[from] std::io::Error),
}

#[derive(Debug, Serialize)]
struct StatusBody {
    status: &'static str,
}

impl HealthState {
    pub async fn mark_healthy(&self, healthy: bool) {
        self.inner.write().await.healthy = healthy;
    }

    pub async fn mark_ready(&self, ready: bool) {
        self.inner.write().await.ready = ready;
    }
}

impl ObservabilityServer {
    /// Creates the metrics recorder and binds the observability listener.
    ///
    /// # Errors
    ///
    /// Returns an error if the process-wide recorder is already installed or the address cannot
    /// be bound.
    pub async fn bind(
        address: SocketAddr,
        health: HealthState,
    ) -> Result<Self, ObservabilityError> {
        let metrics = PrometheusBuilder::new()
            .install_recorder()
            .map_err(|error| ObservabilityError::Metrics(error.to_string()))?;
        let router = router(health, metrics);
        let listener = TcpListener::bind(address)
            .await
            .map_err(|source| ObservabilityError::Bind { address, source })?;
        Ok(Self { listener, router })
    }

    /// Returns the bound listener address.
    ///
    /// # Errors
    ///
    /// Returns the listener's operating-system error when its address is unavailable.
    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    /// Serves requests until the server fails or its task is cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error when Axum cannot continue serving the listener.
    pub async fn serve(self) -> Result<(), ObservabilityError> {
        axum::serve(self.listener, self.router)
            .await
            .map_err(ObservabilityError::Serve)
    }
}

fn router(health: HealthState, metrics: PrometheusHandle) -> Router {
    Router::new()
        .route("/health", get(health_endpoint))
        .route("/ready", get(readiness_endpoint))
        .route("/metrics", get(move || async move { metrics.render() }))
        .with_state(health)
}

async fn health_endpoint(State(state): State<HealthState>) -> Response {
    status_response(state.inner.read().await.healthy)
}

async fn readiness_endpoint(State(state): State<HealthState>) -> Response {
    status_response(state.inner.read().await.ready)
}

fn status_response(ok: bool) -> Response {
    let code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let status = if ok { "ok" } else { "unavailable" };
    (code, Json(StatusBody { status })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_and_readiness_are_fail_closed_until_marked_available() {
        let state = HealthState::default();
        assert_eq!(
            health_endpoint(State(state.clone())).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            readiness_endpoint(State(state.clone())).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        state.mark_healthy(true).await;
        state.mark_ready(true).await;
        assert_eq!(
            health_endpoint(State(state.clone())).await.status(),
            StatusCode::OK
        );
        assert_eq!(
            readiness_endpoint(State(state)).await.status(),
            StatusCode::OK
        );
    }
}
