use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use metrics::{counter, gauge};
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
    health_state: String,
    degraded_components: Vec<String>,
    correctness_stop_reason: Option<String>,
    primary_rpc_url: Option<String>,
    verifier_rpc_url: Option<String>,
    current_lag_blocks: u64,
    reconnect_count: u64,
    wakeup_depth: usize,
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
    health_state: String,
    degraded_components: Vec<String>,
    correctness_stop_reason: Option<String>,
    primary_rpc_url: Option<String>,
    verifier_rpc_url: Option<String>,
    current_lag_blocks: u64,
    reconnect_count: u64,
    wakeup_depth: usize,
    unreconciled_gap_count: u64,
    verifier_disagreement_count: u64,
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
    pub wakeup_depth: usize,
    pub unreconciled_gap_count: u64,
    pub verifier_disagreement_count: u64,
}

impl Default for StatusSnapshot {
    fn default() -> Self {
        Self {
            healthy: false,
            ready: false,
            readiness: "unavailable".to_owned(),
            health_state: "unavailable".to_owned(),
            degraded_components: Vec::new(),
            correctness_stop_reason: None,
            primary_rpc_url: None,
            verifier_rpc_url: None,
            current_lag_blocks: 0,
            reconnect_count: 0,
            wakeup_depth: 0,
            unreconciled_gap_count: 0,
            verifier_disagreement_count: 0,
        }
    }
}

impl From<LiveStatusSnapshot> for StatusSnapshot {
    fn from(snapshot: LiveStatusSnapshot) -> Self {
        let health_state = live_health_state(snapshot.healthy, snapshot.ready, &snapshot.readiness);
        Self {
            healthy: snapshot.healthy,
            ready: snapshot.ready,
            readiness: snapshot.readiness,
            health_state,
            degraded_components: Vec::new(),
            correctness_stop_reason: None,
            primary_rpc_url: snapshot.primary_rpc_url,
            verifier_rpc_url: snapshot.verifier_rpc_url,
            current_lag_blocks: snapshot.current_lag_blocks,
            reconnect_count: snapshot.reconnect_count,
            wakeup_depth: snapshot.wakeup_depth,
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
            health_state: snapshot.health_state,
            degraded_components: snapshot.degraded_components,
            correctness_stop_reason: snapshot.correctness_stop_reason,
            primary_rpc_url: snapshot.primary_rpc_url,
            verifier_rpc_url: snapshot.verifier_rpc_url,
            current_lag_blocks: snapshot.current_lag_blocks,
            reconnect_count: snapshot.reconnect_count,
            wakeup_depth: snapshot.wakeup_depth,
            unreconciled_gap_count: snapshot.unreconciled_gap_count,
            verifier_disagreement_count: snapshot.verifier_disagreement_count,
        }
    }
}

impl HealthState {
    pub async fn mark_healthy(&self, healthy: bool) {
        let mut snapshot = self.inner.write().await;
        snapshot.healthy = healthy;
        if healthy {
            snapshot.health_state = if snapshot.ready {
                "ready".to_owned()
            } else {
                "unavailable".to_owned()
            };
            snapshot.correctness_stop_reason = None;
        } else {
            snapshot.health_state = "unavailable".to_owned();
        }
        publish_metrics(&snapshot);
    }

    pub async fn mark_ready(&self, ready: bool) {
        let mut snapshot = self.inner.write().await;
        snapshot.ready = ready;
        snapshot.readiness = if ready { "ready" } else { "unavailable" }.to_owned();
        snapshot.health_state = snapshot.readiness.clone();
        if ready {
            snapshot.healthy = true;
            snapshot.degraded_components.clear();
            snapshot.correctness_stop_reason = None;
        }
        publish_metrics(&snapshot);
    }

    pub async fn mark_degraded(&self, component: &str) {
        let mut snapshot = self.inner.write().await;
        snapshot.healthy = true;
        snapshot.ready = false;
        snapshot.readiness = "degraded".to_owned();
        snapshot.health_state = "degraded".to_owned();
        snapshot.correctness_stop_reason = None;
        if !snapshot
            .degraded_components
            .iter()
            .any(|known| known == component)
        {
            snapshot.degraded_components.push(component.to_owned());
        }
        publish_metrics(&snapshot);
    }

    pub async fn mark_correctness_stop(&self, reason: &str) {
        let mut snapshot = self.inner.write().await;
        snapshot.healthy = false;
        snapshot.ready = false;
        snapshot.readiness = "correctness-stop".to_owned();
        snapshot.health_state = "correctness-stop".to_owned();
        snapshot.degraded_components.clear();
        snapshot.correctness_stop_reason = Some(reason.to_owned());
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
        publish_required_metric_defaults();
        let router = router(health, metrics);
        let listener = TcpListener::bind(address)
            .await
            .map_err(|source| ObservabilityError::Bind { address, source })?;
        Ok(Self { listener, router })
    }

    /// Binds an observability listener backed by a local metrics recorder handle.
    ///
    /// This is useful for tests and embedded callers that cannot install the process-global
    /// metrics recorder more than once.
    ///
    /// # Errors
    ///
    /// Returns an error if the address cannot be bound.
    pub async fn bind_with_local_recorder(
        address: SocketAddr,
        health: HealthState,
    ) -> Result<Self, ObservabilityError> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let metrics = recorder.handle();
        publish_required_metric_defaults();
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

fn live_health_state(healthy: bool, ready: bool, readiness: &str) -> String {
    if !healthy {
        "unavailable".to_owned()
    } else if ready {
        "ready".to_owned()
    } else if readiness == "degraded" {
        "degraded".to_owned()
    } else {
        "unavailable".to_owned()
    }
}

fn publish_required_metric_defaults() {
    gauge!("chainweave_current_lag_blocks").set(0.0);
    gauge!("chainweave_checkpoint_height").set(0.0);
    counter!("chainweave_reorg_total").increment(0);
    gauge!("chainweave_reorg_depth_blocks").set(0.0);
    gauge!("chainweave_outbox_unpublished_oldest_age_seconds").set(0.0);
    gauge!("chainweave_outbox_unpublished_count").set(0.0);
    counter!("chainweave_kafka_delivery_failures_total").increment(0);
    counter!("chainweave_decode_failures_total").increment(0);
}

fn publish_metrics(snapshot: &StatusSnapshot) {
    gauge!("chainweave_live_ready").set(if snapshot.ready { 1.0 } else { 0.0 });
    gauge!("chainweave_current_lag_blocks").set(snapshot.current_lag_blocks as f64);
    gauge!("chainweave_live_lag_blocks").set(snapshot.current_lag_blocks as f64);
    gauge!("chainweave_live_reconnect_count").set(snapshot.reconnect_count as f64);
    gauge!("chainweave_live_unreconciled_gap_count").set(snapshot.unreconciled_gap_count as f64);
    gauge!("chainweave_live_verifier_disagreement_count")
        .set(snapshot.verifier_disagreement_count as f64);
    gauge!("chainweave_live_wakeup_depth").set(snapshot.wakeup_depth as f64);
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
                wakeup_depth: 1,
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
        assert_eq!(snapshot.wakeup_depth, 1);
        assert_eq!(snapshot.verifier_disagreement_count, 7);
    }

    #[tokio::test]
    async fn health_distinguishes_degraded_from_correctness_stop() {
        let state = HealthState::default();

        state.mark_degraded("rpc").await;
        let degraded = health_endpoint(State(state.clone())).await;
        assert_eq!(degraded.status(), StatusCode::OK);
        let snapshot = state.inner.read().await.clone();
        assert_eq!(snapshot.health_state, "degraded");
        assert_eq!(snapshot.degraded_components, vec!["rpc"]);
        assert_eq!(snapshot.correctness_stop_reason, None);

        state.mark_correctness_stop("unresolved_ancestry").await;
        let stopped = health_endpoint(State(state.clone())).await;
        assert_eq!(stopped.status(), StatusCode::SERVICE_UNAVAILABLE);
        let snapshot = state.inner.read().await.clone();
        assert_eq!(snapshot.health_state, "correctness-stop");
        assert_eq!(
            snapshot.correctness_stop_reason.as_deref(),
            Some("unresolved_ancestry")
        );
        assert!(snapshot.degraded_components.is_empty());
    }

    #[test]
    fn prometheus_render_exposes_core_observability_series() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            publish_required_metric_defaults();
            publish_metrics(&StatusSnapshot::from(LiveStatusSnapshot {
                healthy: true,
                ready: true,
                readiness: "ready".to_owned(),
                primary_rpc_url: None,
                verifier_rpc_url: None,
                current_lag_blocks: 5,
                reconnect_count: 0,
                wakeup_depth: 0,
                unreconciled_gap_count: 0,
                verifier_disagreement_count: 0,
            }));
        });
        let metrics = handle.render();

        for series in [
            "chainweave_current_lag_blocks",
            "chainweave_checkpoint_height",
            "chainweave_reorg_total",
            "chainweave_reorg_depth_blocks",
            "chainweave_outbox_unpublished_oldest_age_seconds",
            "chainweave_outbox_unpublished_count",
            "chainweave_kafka_delivery_failures_total",
            "chainweave_decode_failures_total",
        ] {
            assert!(metrics.contains(series), "missing metric series {series}");
        }
    }
}
