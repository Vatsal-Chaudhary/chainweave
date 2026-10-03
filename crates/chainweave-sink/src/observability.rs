use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use metrics::gauge;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde::Serialize;
use thiserror::Error;
use tokio::{net::TcpListener, sync::RwLock};

#[derive(Debug, Clone, Default)]
pub struct HealthState {
    inner: Arc<RwLock<StatusSnapshot>>,
}

#[derive(Debug, Clone)]
struct StatusSnapshot {
    healthy: bool,
    ready: bool,
    readiness: String,
    primary_rpc_url: Option<String>,
    verifier_rpc_url: Option<String>,
    current_lag_blocks: u64,
    reconnect_count: u64,
    queue_depths: QueueDepths,
    unreconciled_gap_count: u64,
    verifier_disagreement_count: u64,
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
    ready: bool,
    readiness: String,
    primary_rpc_url: Option<String>,
    verifier_rpc_url: Option<String>,
    current_lag_blocks: u64,
    reconnect_count: u64,
    queue_depths: QueueDepths,
    unreconciled_gap_count: u64,
    verifier_disagreement_count: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct QueueDepths {
    pub wakeups: usize,
    pub fetch: usize,
    pub coordinate: usize,
    pub decode: usize,
    pub write: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveStatusSnapshot {
    pub healthy: bool,
    pub ready: bool,
    pub readiness: String,
    pub primary_rpc_url: Option<String>,
    pub verifier_rpc_url: Option<String>,
    pub current_lag_blocks: u64,
    pub reconnect_count: u64,
    pub queue_depths: QueueDepths,
    pub unreconciled_gap_count: u64,
    pub verifier_disagreement_count: u64,
}

impl Default for StatusSnapshot {
    fn default() -> Self {
        Self {
            healthy: false,
            ready: false,
            readiness: "unavailable".to_owned(),
            primary_rpc_url: None,
            verifier_rpc_url: None,
            current_lag_blocks: 0,
            reconnect_count: 0,
            queue_depths: QueueDepths::default(),
            unreconciled_gap_count: 0,
            verifier_disagreement_count: 0,
        }
    }
}

impl From<LiveStatusSnapshot> for StatusSnapshot {
    fn from(snapshot: LiveStatusSnapshot) -> Self {
        Self {
            healthy: snapshot.healthy,
            ready: snapshot.ready,
            readiness: snapshot.readiness,
            primary_rpc_url: snapshot.primary_rpc_url,
            verifier_rpc_url: snapshot.verifier_rpc_url,
            current_lag_blocks: snapshot.current_lag_blocks,
            reconnect_count: snapshot.reconnect_count,
            queue_depths: snapshot.queue_depths,
            unreconciled_gap_count: snapshot.unreconciled_gap_count,
            verifier_disagreement_count: snapshot.verifier_disagreement_count,
        }
    }
}

impl From<StatusSnapshot> for StatusBody {
    fn from(snapshot: StatusSnapshot) -> Self {
        let status = if snapshot.healthy {
            "ok"
        } else {
            "unavailable"
        };
        Self {
            status,
            ready: snapshot.ready,
            readiness: snapshot.readiness,
            primary_rpc_url: snapshot.primary_rpc_url,
            verifier_rpc_url: snapshot.verifier_rpc_url,
            current_lag_blocks: snapshot.current_lag_blocks,
            reconnect_count: snapshot.reconnect_count,
            queue_depths: snapshot.queue_depths,
            unreconciled_gap_count: snapshot.unreconciled_gap_count,
            verifier_disagreement_count: snapshot.verifier_disagreement_count,
        }
    }
}

impl HealthState {
    pub async fn mark_healthy(&self, healthy: bool) {
        let mut snapshot = self.inner.write().await;
        snapshot.healthy = healthy;
        publish_metrics(&snapshot);
    }

    pub async fn mark_ready(&self, ready: bool) {
        let mut snapshot = self.inner.write().await;
        snapshot.ready = ready;
        snapshot.readiness = if ready { "ready" } else { "unavailable" }.to_owned();
        publish_metrics(&snapshot);
    }

    pub async fn update_live_status(&self, status: LiveStatusSnapshot) {
        let snapshot = StatusSnapshot::from(status);
        publish_metrics(&snapshot);
        *self.inner.write().await = snapshot;
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
    let snapshot = state.inner.read().await.clone();
    status_response(snapshot.healthy, snapshot)
}

async fn readiness_endpoint(State(state): State<HealthState>) -> Response {
    let snapshot = state.inner.read().await.clone();
    status_response(snapshot.ready, snapshot)
}

fn status_response(ok: bool, snapshot: StatusSnapshot) -> Response {
    let code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(StatusBody::from(snapshot))).into_response()
}

fn publish_metrics(snapshot: &StatusSnapshot) {
    gauge!("chainweave_live_ready").set(if snapshot.ready { 1.0 } else { 0.0 });
    gauge!("chainweave_live_lag_blocks").set(snapshot.current_lag_blocks as f64);
    gauge!("chainweave_live_reconnect_count").set(snapshot.reconnect_count as f64);
    gauge!("chainweave_live_unreconciled_gap_count")
        .set(snapshot.unreconciled_gap_count as f64);
    gauge!("chainweave_live_verifier_disagreement_count")
        .set(snapshot.verifier_disagreement_count as f64);
    gauge!("chainweave_live_queue_depth", "queue" => "wakeups")
        .set(snapshot.queue_depths.wakeups as f64);
    gauge!("chainweave_live_queue_depth", "queue" => "fetch")
        .set(snapshot.queue_depths.fetch as f64);
    gauge!("chainweave_live_queue_depth", "queue" => "coordinate")
        .set(snapshot.queue_depths.coordinate as f64);
    gauge!("chainweave_live_queue_depth", "queue" => "decode")
        .set(snapshot.queue_depths.decode as f64);
    gauge!("chainweave_live_queue_depth", "queue" => "write")
        .set(snapshot.queue_depths.write as f64);
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

    #[tokio::test]
    async fn live_status_snapshot_drives_health_ready_and_metrics_fields() {
        let state = HealthState::default();
        state
            .update_live_status(LiveStatusSnapshot {
                healthy: true,
                ready: false,
                readiness: "degraded".to_owned(),
                primary_rpc_url: Some("https://rpc.example/v3/redacted".to_owned()),
                verifier_rpc_url: None,
                current_lag_blocks: 3,
                reconnect_count: 2,
                queue_depths: QueueDepths {
                    wakeups: 1,
                    fetch: 2,
                    coordinate: 3,
                    decode: 4,
                    write: 5,
                },
                unreconciled_gap_count: 1,
                verifier_disagreement_count: 7,
            })
            .await;

        assert_eq!(
            health_endpoint(State(state.clone())).await.status(),
            StatusCode::OK
        );
        assert_eq!(
            readiness_endpoint(State(state.clone())).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        let snapshot = state.inner.read().await.clone();
        assert_eq!(snapshot.readiness, "degraded");
        assert_eq!(snapshot.current_lag_blocks, 3);
        assert_eq!(snapshot.reconnect_count, 2);
        assert_eq!(snapshot.queue_depths.write, 5);
        assert_eq!(snapshot.verifier_disagreement_count, 7);
    }
}
