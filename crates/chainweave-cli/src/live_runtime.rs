use std::{
    future::Future,
    panic::AssertUnwindSafe,
    time::{Duration, Instant},
};

use chainweave_core::{
    AppConfig, BackfillRange, BlockHash, BlockHeader, ChainError, ChainIdentity, ConfigError,
    FetchedRange, LiveStartPoint, OrderedCommitCoordinator, RetryPolicy, RpcBudget, RpcMethod,
    ValidationProfile, redact_url,
};
use chainweave_rpc::{
    ContractLogFilter, RpcClient, RpcError, capture_target_head_with_retry,
    fetch_header_by_hash_with_retry, fetch_header_by_number_with_retry, new_head_wakeup_channel,
    retry_rpc_request, spawn_new_heads_wakeup,
};
use chainweave_sink::{
    HealthState, IndexedBlock, LiveStatusSnapshot, ObservabilityError, ObservabilityServer,
    PostgresBackfillCommitter, PostgresChainWriter, PostgresStateError, QueueDepths,
    ReconciliationError, ReconciliationSource, ReconciliationSummary,
};
use futures::FutureExt;
use thiserror::Error;
use tokio::{signal, sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use url::Url;

#[derive(Debug, Error)]
pub enum LiveRuntimeError {
    #[error("configuration validation failed: {0}")]
    Config(#[from] ConfigError),
    #[error("RPC endpoint {endpoint} failed: {source}")]
    RpcEndpoint { endpoint: String, source: RpcError },
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error(transparent)]
    Sink(#[from] PostgresStateError),
    #[error("database migration failed: {0}")]
    Migration(String),
    #[error("observability server failed: {0}")]
    Observability(#[from] ObservabilityError),
    #[error("startup reconciliation failed: {0}")]
    Reconciliation(#[from] ReconciliationError),
    #[error(transparent)]
    Backfill(#[from] chainweave_core::BackfillError),
    #[error("checkpoint height {0} has no canonical header")]
    MissingCheckpointHeader(u64),
    #[error("invalid live RPC budget: {0}")]
    Budget(String),
    #[error("invalid reconnect policy: {0}")]
    ReconnectPolicy(String),
    #[error("invalid reconnect order: {0}")]
    ReconnectOrder(String),
    #[error(transparent)]
    Pipeline(#[from] LivePipelineError),
}

#[derive(Debug, Clone)]
pub struct LiveCommandOptions {
    pub start_block: Option<u64>,
    pub poll_interval: Duration,
    pub rpc_timeout: Duration,
    pub budget_window: Duration,
    pub budget_cost_units_per_window: u64,
    pub shutdown_timeout: Duration,
}

impl Default for LiveCommandOptions {
    fn default() -> Self {
        Self {
            start_block: None,
            poll_interval: Duration::from_secs(12),
            rpc_timeout: Duration::from_secs(30),
            budget_window: Duration::from_secs(60),
            budget_cost_units_per_window: 1_200,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeLiveConfig {
    pub retry_policy: RetryPolicy,
    pub rpc_timeout: Duration,
    pub budget_window: Duration,
    pub budget_cost_units_per_window: u64,
    pub filter: Option<ContractLogFilter>,
}

#[derive(Debug, Clone)]
pub struct RpcLiveSource {
    client: RpcClient,
    config: RuntimeLiveConfig,
    budget: WindowedLiveBudget,
}

impl RpcLiveSource {
    /// Builds an RPC-backed live source that uses the shared retry vocabulary and a windowed
    /// request budget. Budget exhaustion sleeps until the current window rolls over.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured budget cap is invalid.
    pub fn new(client: RpcClient, config: RuntimeLiveConfig) -> Result<Self, LiveRuntimeError> {
        Ok(Self {
            client,
            budget: WindowedLiveBudget::new(
                config.budget_cost_units_per_window,
                config.budget_window,
                Instant::now(),
            )?,
            config,
        })
    }

    /// Polls the current canonical head and returns its full header.
    ///
    /// # Errors
    ///
    /// Returns an RPC error when the head or header cannot be fetched through the shared retry
    /// policy.
    pub async fn poll_head(&mut self) -> Result<BlockHeader, LiveRuntimeError> {
        self.budget
            .record_or_backoff(RpcMethod::GetBlockByNumber)
            .await?;
        let head = capture_target_head_with_retry(
            &self.client,
            self.config.retry_policy,
            self.config.rpc_timeout,
        )
        .await?;
        self.header_by_number(head.number).await
    }

    /// Fetches one block by canonical height and attaches raw logs fetched by block hash.
    ///
    /// # Errors
    ///
    /// Returns an RPC error if the block, logs, or hash-anchored validation fail.
    pub async fn block_by_number(&mut self, height: u64) -> Result<IndexedBlock, LiveRuntimeError> {
        self.budget
            .record_or_backoff(RpcMethod::GetBlockByNumber)
            .await?;
        let mut block = retry_rpc_request(
            self.config.retry_policy,
            self.config.rpc_timeout,
            || RpcError::Request(format!("timed out fetching block at height {height}")),
            || self.client.fetch_block_by_number(height),
        )
        .await?;

        self.budget.record_or_backoff(RpcMethod::GetLogs).await?;
        block.logs = retry_rpc_request(
            self.config.retry_policy,
            self.config.rpc_timeout,
            || {
                RpcError::Request(format!(
                    "timed out fetching logs by block hash {:?}",
                    block.header.hash
                ))
            },
            || {
                self.client
                    .fetch_logs_by_block_hash(block.header.hash, self.config.filter)
            },
        )
        .await?;
        Ok(block)
    }

    /// Fetches one block by hash with hash-anchored logs.
    ///
    /// # Errors
    ///
    /// Returns an RPC error if the block is unavailable or returned logs do not match the hash.
    pub async fn block_by_hash(
        &mut self,
        hash: BlockHash,
    ) -> Result<IndexedBlock, LiveRuntimeError> {
        self.budget
            .record_or_backoff(RpcMethod::GetBlockByHash)
            .await?;
        self.budget.record_or_backoff(RpcMethod::GetLogs).await?;
        retry_rpc_request(
            self.config.retry_policy,
            self.config.rpc_timeout,
            || RpcError::Request(format!("timed out fetching block by hash {hash:?}")),
            || self.client.fetch_block_by_hash(hash),
        )
        .await
        .map_err(Into::into)
    }

    /// Fetches one header by number through the shared retry policy.
    ///
    /// # Errors
    ///
    /// Returns an RPC error when the header cannot be fetched.
    pub async fn header_by_number(&mut self, height: u64) -> Result<BlockHeader, LiveRuntimeError> {
        self.budget
            .record_or_backoff(RpcMethod::GetBlockByNumber)
            .await?;
        fetch_header_by_number_with_retry(
            &self.client,
            height,
            self.config.retry_policy,
            self.config.rpc_timeout,
        )
        .await
        .map_err(Into::into)
    }

    /// Fetches one optional header by hash through the shared retry policy.
    ///
    /// # Errors
    ///
    /// Returns an RPC error when the hash lookup fails.
    pub async fn header_by_hash(
        &mut self,
        hash: BlockHash,
    ) -> Result<Option<BlockHeader>, LiveRuntimeError> {
        self.budget
            .record_or_backoff(RpcMethod::GetBlockByHash)
            .await?;
        fetch_header_by_hash_with_retry(
            &self.client,
            hash,
            self.config.retry_policy,
            self.config.rpc_timeout,
        )
        .await
        .map_err(Into::into)
    }
}

#[derive(Debug)]
struct RpcReconciliationSource {
    primary: RpcLiveSource,
    verifier: Option<RpcLiveSource>,
    verifier_disagreements: u64,
    last_head: Option<BlockHeader>,
}

impl RpcReconciliationSource {
    fn new(primary: RpcLiveSource, verifier: Option<RpcLiveSource>) -> Self {
        Self {
            primary,
            verifier,
            verifier_disagreements: 0,
            last_head: None,
        }
    }

    const fn verifier_disagreements(&self) -> u64 {
        self.verifier_disagreements
    }

    const fn last_head(&self) -> Option<BlockHeader> {
        self.last_head
    }

    async fn verify_head(&mut self, primary: BlockHeader) -> Result<(), LiveRuntimeError> {
        let Some(verifier) = &mut self.verifier else {
            return Ok(());
        };
        match verifier.header_by_number(primary.height).await {
            Ok(verifier) if verifier.hash != primary.hash => {
                self.verifier_disagreements = self.verifier_disagreements.saturating_add(1);
            }
            Ok(_) => {}
            Err(_) => {
                self.verifier_disagreements = self.verifier_disagreements.saturating_add(1);
            }
        }
        Ok(())
    }
}

impl ReconciliationSource for RpcReconciliationSource {
    async fn head(&mut self) -> Result<IndexedBlock, ReconciliationError> {
        let header = self
            .primary
            .poll_head()
            .await
            .map_err(|error| ReconciliationError::Source(error.to_string()))?;
        self.last_head = Some(header);
        self.verify_head(header)
            .await
            .map_err(|error| ReconciliationError::Source(error.to_string()))?;
        self.primary
            .block_by_number(header.height)
            .await
            .map_err(|error| ReconciliationError::Source(error.to_string()))
    }

    async fn block_by_height(
        &mut self,
        height: u64,
    ) -> Result<Option<IndexedBlock>, ReconciliationError> {
        self.primary
            .block_by_number(height)
            .await
            .map(Some)
            .map_err(|error| ReconciliationError::Source(error.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct PostgresLiveStore {
    writer: PostgresChainWriter,
}

/// Runs the real live runtime until SIGINT/SIGTERM or a fail-closed correctness error.
///
/// # Errors
///
/// Returns an error when configuration, RPC identity, Postgres state, reconciliation, or the
/// observability server fails.
pub async fn run_live(
    config: &AppConfig,
    options: LiveCommandOptions,
) -> Result<(), LiveRuntimeError> {
    config.validate(ValidationProfile::Workers)?;
    let retry_policy = RetryPolicy::new(
        8,
        Duration::from_millis(500),
        Duration::from_secs(30),
        Duration::from_millis(500),
    )?;
    let runtime_config = RuntimeLiveConfig {
        retry_policy,
        rpc_timeout: options.rpc_timeout,
        budget_window: options.budget_window,
        budget_cost_units_per_window: options.budget_cost_units_per_window,
        filter: None,
    };
    let primary_redacted = redact_url(&config.rpc.primary_url);
    let verifier_redacted = config.rpc.verifier_url.as_ref().map(redact_url);
    let health = HealthState::default();
    health
        .update_live_status(initial_status(
            &primary_redacted,
            verifier_redacted.as_deref(),
        ))
        .await;
    let observability =
        ObservabilityServer::bind(config.server.listen_addr, health.clone()).await?;
    let observability_addr = observability
        .local_addr()
        .map_err(ObservabilityError::Serve)?;
    let shutdown = CancellationToken::new();
    spawn_shutdown_signal(shutdown.clone());
    let observability_task = tokio::spawn(observability.serve());

    info!(
        primary_rpc = %primary_redacted,
        verifier_rpc = verifier_redacted.as_deref().unwrap_or("none"),
        observability_addr = %observability_addr,
        "starting live runner"
    );

    let primary_client = connect_rpc(&config.rpc.primary_url).await?;
    let primary_head =
        capture_target_head_with_retry(&primary_client, retry_policy, options.rpc_timeout).await?;
    if let Some(expected) = &config.expected_chain {
        RpcClient::verify_identity(&primary_head, expected)?;
    }

    let verifier_client = if let Some(url) = &config.rpc.verifier_url {
        let verifier_client = connect_rpc(url).await?;
        let verifier_head =
            capture_target_head_with_retry(&verifier_client, retry_policy, options.rpc_timeout)
                .await?;
        let expected = ChainIdentity {
            chain_id: primary_head.chain_id,
            genesis_hash: primary_head.genesis_hash.to_string(),
        };
        RpcClient::verify_identity(&verifier_head, &expected)?;
        Some(verifier_client)
    } else {
        None
    };

    let database_url = config.database_url.as_deref().ok_or_else(|| {
        ConfigError::Invalid("database_url is required before live mode".to_owned())
    })?;
    let writer = PostgresChainWriter::connect(database_url, primary_head.chain_id).await?;
    writer
        .run_migrations()
        .await
        .map_err(|error| LiveRuntimeError::Migration(error.to_string()))?;
    writer
        .ensure_chain_identity(primary_head.genesis_block_hash())
        .await?;
    let checkpoint = writer.checkpoint().await?;
    let start_point = checkpoint
        .as_ref()
        .map(|_| LiveStartPoint::DurableCheckpoint)
        .or_else(|| options.start_block.map(LiveStartPoint::Explicit));
    AppConfig::validate_live_start(start_point)?;

    let policy = ReconnectPolicy::new(
        Duration::from_secs(1),
        Duration::from_secs(30),
        Duration::from_millis(500),
        Duration::from_secs(30),
    )?;
    let mut reconnect = ReconnectLoop::new(policy);
    let (wakeups, mut wakeup_rx) = new_head_wakeup_channel();
    let mut new_heads_task = if is_ws_url(&config.rpc.primary_url) {
        reconnect.record_subscribed_for_wakeups();
        Some(spawn_new_heads_wakeup(
            config.rpc.primary_url.clone(),
            wakeups.clone(),
        ))
    } else {
        reconnect.record_subscribed_for_wakeups();
        None
    };

    let primary_source = RpcLiveSource::new(primary_client, runtime_config.clone())?;
    let verifier_source = verifier_client
        .map(|client| RpcLiveSource::new(client, runtime_config.clone()))
        .transpose()?;
    let mut source = RpcReconciliationSource::new(primary_source, verifier_source);
    let mut poll = tokio::time::interval(options.poll_interval);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    reconcile_and_publish(
        &writer,
        &mut source,
        &options,
        config.indexer.max_reorg_depth,
        &mut reconnect,
        &health,
        &StatusContext::new(config, &primary_redacted, verifier_redacted.as_deref()),
        &wakeup_rx,
        None,
    )
    .await?;

    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                reconnect.record_disconnect();
                publish_status(
                    &writer,
                    &source,
                    &reconnect,
                    &health,
                    &StatusContext::new(config, &primary_redacted, verifier_redacted.as_deref()),
                    &wakeup_rx,
                    0,
                ).await?;
                if let Some(task) = &new_heads_task {
                    task.abort();
                }
                observability_task.abort();
                tokio::time::sleep(options.shutdown_timeout.min(Duration::from_millis(50))).await;
                return Ok(());
            }
            _ = poll.tick() => {
                reconcile_and_publish(
                    &writer,
                    &mut source,
                    &options,
                    config.indexer.max_reorg_depth,
                    &mut reconnect,
                    &health,
                    &StatusContext::new(config, &primary_redacted, verifier_redacted.as_deref()),
                    &wakeup_rx,
                    None,
                ).await?;
            }
            maybe_wakeup = wakeup_rx.recv() => {
                if maybe_wakeup.is_some() {
                    reconcile_and_publish(
                        &writer,
                        &mut source,
                        &options,
                        config.indexer.max_reorg_depth,
                        &mut reconnect,
                        &health,
                        &StatusContext::new(config, &primary_redacted, verifier_redacted.as_deref()),
                        &wakeup_rx,
                        None,
                    ).await?;
                }
            }
            joined = async {
                match &mut new_heads_task {
                    Some(task) => Some(task.await),
                    None => std::future::pending().await,
                }
            } => {
                reconnect.record_disconnect();
                publish_status(
                    &writer,
                    &source,
                    &reconnect,
                    &health,
                    &StatusContext::new(config, &primary_redacted, verifier_redacted.as_deref()),
                    &wakeup_rx,
                    0,
                ).await?;
                warn!(result = ?joined, "newHeads subscription ended; reconnecting after backoff");
                let ReconnectAction::RetryAfter(delay) = reconnect.record_availability_failure() else {
                    return Ok(());
                };
                tokio::time::sleep(delay).await;
                reconnect.record_subscribed_for_wakeups();
                new_heads_task = if is_ws_url(&config.rpc.primary_url) {
                    Some(spawn_new_heads_wakeup(
                        config.rpc.primary_url.clone(),
                        wakeups.clone(),
                    ))
                } else {
                    None
                };
                reconcile_and_publish(
                    &writer,
                    &mut source,
                    &options,
                    config.indexer.max_reorg_depth,
                    &mut reconnect,
                    &health,
                    &StatusContext::new(config, &primary_redacted, verifier_redacted.as_deref()),
                    &wakeup_rx,
                    None,
                ).await?;
            }
        }
    }
}

struct StatusContext<'a> {
    primary_rpc_url: &'a str,
    verifier_rpc_url: Option<&'a str>,
    queue_capacities: QueueDepths,
}

impl<'a> StatusContext<'a> {
    fn new(
        config: &AppConfig,
        primary_rpc_url: &'a str,
        verifier_rpc_url: Option<&'a str>,
    ) -> Self {
        Self {
            primary_rpc_url,
            verifier_rpc_url,
            queue_capacities: QueueDepths {
                wakeups: 1,
                fetch: config.queues.fetch,
                coordinate: config.queues.coordinate,
                decode: config.queues.decode,
                write: config.queues.write,
            },
        }
    }
}

async fn reconcile_and_publish(
    writer: &PostgresChainWriter,
    source: &mut RpcReconciliationSource,
    options: &LiveCommandOptions,
    max_reorg_depth: u64,
    reconnect: &mut ReconnectLoop,
    health: &HealthState,
    context: &StatusContext<'_>,
    wakeup_rx: &mpsc::Receiver<()>,
    unreconciled_gap_count: Option<u64>,
) -> Result<(), LiveRuntimeError> {
    reconnect.record_subscribed_for_wakeups();
    let summary = reconcile_once(writer, source, options, max_reorg_depth).await?;
    reconnect.record_reconciled_to_head()?;
    reconnect.record_ready_after_reconnect_at(Instant::now())?;
    let gap_count = unreconciled_gap_count.unwrap_or_else(|| {
        summary
            .final_checkpoint
            .as_ref()
            .and_then(|checkpoint| {
                source
                    .last_head()
                    .map(|head| head.height.saturating_sub(checkpoint.last_height))
            })
            .unwrap_or(0)
    });
    publish_status(
        writer, source, reconnect, health, context, wakeup_rx, gap_count,
    )
    .await
}

async fn reconcile_once(
    writer: &PostgresChainWriter,
    source: &mut RpcReconciliationSource,
    options: &LiveCommandOptions,
    max_reorg_depth: u64,
) -> Result<ReconciliationSummary, LiveRuntimeError> {
    if writer.checkpoint().await?.is_some() {
        return writer
            .reconcile_to_head(source, max_reorg_depth)
            .await
            .map_err(Into::into);
    }
    let Some(start_block) = options.start_block else {
        AppConfig::validate_live_start(None)?;
        unreachable!("validate_live_start always errors for None");
    };
    bootstrap_from_explicit_start(writer, source, start_block).await
}

async fn bootstrap_from_explicit_start(
    writer: &PostgresChainWriter,
    source: &mut RpcReconciliationSource,
    start_block: u64,
) -> Result<ReconciliationSummary, LiveRuntimeError> {
    let head = source.head().await?;
    let mut parent_anchor: Option<BlockHeader> = None;
    let store = PostgresLiveStore::new(writer.clone());
    let mut applied_blocks = 0;
    if start_block > head.header.height {
        return Ok(ReconciliationSummary {
            final_checkpoint: writer.checkpoint().await?,
            ..ReconciliationSummary::default()
        });
    }
    for height in start_block..=head.header.height {
        let block = if height == head.header.height {
            head.clone()
        } else {
            source
                .block_by_height(height)
                .await?
                .ok_or(ReconciliationError::MissingSourceHeight(height))?
        };
        if let Some(parent) = parent_anchor
            && block.header.parent_hash != parent.hash
        {
            return Err(ReconciliationError::BrokenSourceLink {
                child_height: height,
            }
            .into());
        }
        let header = block.header;
        store.commit_one_block(parent_anchor, block).await?;
        parent_anchor = Some(header);
        applied_blocks += 1;
    }
    Ok(ReconciliationSummary {
        rolled_back_blocks: 0,
        applied_blocks,
        final_checkpoint: writer.checkpoint().await?,
    })
}

async fn publish_status(
    writer: &PostgresChainWriter,
    source: &RpcReconciliationSource,
    reconnect: &ReconnectLoop,
    health: &HealthState,
    context: &StatusContext<'_>,
    wakeup_rx: &mpsc::Receiver<()>,
    unreconciled_gap_count: u64,
) -> Result<(), LiveRuntimeError> {
    let checkpoint = writer.checkpoint().await?;
    let current_lag_blocks = source
        .last_head()
        .and_then(|head| {
            checkpoint
                .as_ref()
                .map(|checkpoint| head.height.saturating_sub(checkpoint.last_height))
        })
        .unwrap_or(0);
    let verifier_disagreements = source.verifier_disagreements();
    let reconnect_ready = reconnect.readiness() == RuntimeReadiness::Ready;
    let ready = reconnect_ready && verifier_disagreements == 0 && unreconciled_gap_count == 0;
    let readiness = if ready {
        "ready"
    } else if reconnect_ready {
        "degraded"
    } else {
        "unavailable"
    };
    health
        .update_live_status(LiveStatusSnapshot {
            healthy: !reconnect.is_halted(),
            ready,
            readiness: readiness.to_owned(),
            primary_rpc_url: Some(context.primary_rpc_url.to_owned()),
            verifier_rpc_url: context.verifier_rpc_url.map(ToOwned::to_owned),
            current_lag_blocks,
            reconnect_count: reconnect.attempts(),
            queue_depths: QueueDepths {
                wakeups: wakeup_rx.len(),
                fetch: 0.min(context.queue_capacities.fetch),
                coordinate: 0.min(context.queue_capacities.coordinate),
                decode: 0.min(context.queue_capacities.decode),
                write: 0.min(context.queue_capacities.write),
            },
            unreconciled_gap_count,
            verifier_disagreement_count: verifier_disagreements,
        })
        .await;
    Ok(())
}

fn initial_status(primary_rpc_url: &str, verifier_rpc_url: Option<&str>) -> LiveStatusSnapshot {
    LiveStatusSnapshot {
        healthy: false,
        ready: false,
        readiness: "unavailable".to_owned(),
        primary_rpc_url: Some(primary_rpc_url.to_owned()),
        verifier_rpc_url: verifier_rpc_url.map(ToOwned::to_owned),
        current_lag_blocks: 0,
        reconnect_count: 0,
        queue_depths: QueueDepths::default(),
        unreconciled_gap_count: 0,
        verifier_disagreement_count: 0,
    }
}

async fn connect_rpc(url: &Url) -> Result<RpcClient, LiveRuntimeError> {
    RpcClient::connect(url)
        .await
        .map_err(|source| LiveRuntimeError::RpcEndpoint {
            endpoint: redact_url(url),
            source,
        })
}

fn is_ws_url(url: &Url) -> bool {
    matches!(url.scheme(), "ws" | "wss")
}

fn spawn_shutdown_signal(shutdown: CancellationToken) {
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        shutdown.cancel();
    });
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = async {
            match signal::unix::signal(signal::unix::SignalKind::terminate()) {
                Ok(mut stream) => {
                    stream.recv().await;
                }
                Err(_) => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            result = signal::ctrl_c() => {
                let _ = result;
            }
            () = terminate => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}

impl PostgresLiveStore {
    #[must_use]
    pub const fn new(writer: PostgresChainWriter) -> Self {
        Self { writer }
    }

    #[must_use]
    pub const fn writer(&self) -> &PostgresChainWriter {
        &self.writer
    }

    /// Reads the durable checkpoint as a full canonical header.
    ///
    /// # Errors
    ///
    /// Returns a database error or a fail-closed error when checkpoint state references no
    /// canonical block row.
    pub async fn checkpoint_header(&self) -> Result<Option<BlockHeader>, LiveRuntimeError> {
        let Some(checkpoint) = self.writer.checkpoint().await? else {
            return Ok(None);
        };
        self.writer
            .canonical_header_at_height(checkpoint.last_height)
            .await?
            .ok_or(LiveRuntimeError::MissingCheckpointHeader(
                checkpoint.last_height,
            ))
            .map(Some)
    }

    /// Reads a canonical header at one height.
    ///
    /// # Errors
    ///
    /// Returns a database error if the lookup fails.
    pub async fn canonical_header_at_height(
        &self,
        height: u64,
    ) -> Result<Option<BlockHeader>, LiveRuntimeError> {
        self.writer
            .canonical_header_at_height(height)
            .await
            .map_err(Into::into)
    }

    /// Reads a canonical header by hash.
    ///
    /// # Errors
    ///
    /// Returns a database error if the lookup fails.
    pub async fn canonical_header_by_hash(
        &self,
        hash: BlockHash,
    ) -> Result<Option<BlockHeader>, LiveRuntimeError> {
        self.writer
            .canonical_header_by_hash(hash)
            .await
            .map_err(Into::into)
    }

    /// Commits exactly one fetched live block through the existing ordered coordinator and
    /// Postgres writer path. The writer remains the only durable mutator.
    ///
    /// # Errors
    ///
    /// Returns a backfill/order error or a database writer error if the block cannot commit.
    pub async fn commit_one_block(
        &self,
        parent_anchor: Option<BlockHeader>,
        block: IndexedBlock,
    ) -> Result<(), LiveRuntimeError> {
        let height = block.header.height;
        let fetched = FetchedRange::new(
            BackfillRange::new(height, height)?,
            vec![block.header],
            vec![block],
        )?;
        let mut coordinator = OrderedCommitCoordinator::new(height, height, parent_anchor)?;
        let mut committer = PostgresBackfillCommitter::new(self.writer.clone(), parent_anchor);
        coordinator.push_async(fetched, &mut committer).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct WindowedLiveBudget {
    max_cost_units: u64,
    window: Duration,
    started_at: Instant,
    budget: RpcBudget,
}

impl WindowedLiveBudget {
    /// Creates a time-windowed live budget.
    ///
    /// # Errors
    ///
    /// Returns an error when the window or cost cap is zero.
    pub fn new(
        max_cost_units: u64,
        window: Duration,
        now: Instant,
    ) -> Result<Self, LiveRuntimeError> {
        if window.is_zero() {
            return Err(LiveRuntimeError::Budget(
                "live budget window must be nonzero".to_owned(),
            ));
        }
        Ok(Self {
            max_cost_units,
            window,
            started_at: now,
            budget: RpcBudget::new(u64::MAX, max_cost_units)?,
        })
    }

    /// Records one RPC method at an injected time. Returns the required backoff delay when the
    /// current window is exhausted.
    ///
    /// # Errors
    ///
    /// Returns an error when a reset budget cannot be created.
    pub fn record_at(
        &mut self,
        method: RpcMethod,
        now: Instant,
    ) -> Result<Option<Duration>, LiveRuntimeError> {
        if now.duration_since(self.started_at) >= self.window {
            self.reset(now)?;
        }
        if self.budget.record(method, method.default_cost()).is_ok() {
            return Ok(None);
        }
        let elapsed = now.duration_since(self.started_at);
        Ok(Some(self.window.saturating_sub(elapsed)))
    }

    async fn record_or_backoff(&mut self, method: RpcMethod) -> Result<(), LiveRuntimeError> {
        loop {
            match self.record_at(method, Instant::now())? {
                Some(delay) => tokio::time::sleep(delay).await,
                None => return Ok(()),
            }
        }
    }

    fn reset(&mut self, now: Instant) -> Result<(), LiveRuntimeError> {
        self.started_at = now;
        self.budget = RpcBudget::new(u64::MAX, self.max_cost_units)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeReadiness {
    Ready,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectAction {
    RetryAfter(Duration),
    Halt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectPolicy {
    pub base_delay: Duration,
    pub max_delay: Duration,
    /// Deterministic per-attempt spread added after the capped exponential delay. This is not
    /// random jitter; it gives repeatable tests and avoids synchronized reconnects enough for M4.
    pub deterministic_spread: Duration,
    pub min_healthy_duration: Duration,
}

impl ReconnectPolicy {
    /// Builds the unbounded reconnect policy used by live WebSocket supervision.
    ///
    /// # Errors
    ///
    /// Returns an error when delays are zero or the cap is below the base delay.
    pub fn new(
        base_delay: Duration,
        max_delay: Duration,
        deterministic_spread: Duration,
        min_healthy_duration: Duration,
    ) -> Result<Self, LiveRuntimeError> {
        if base_delay.is_zero() || max_delay.is_zero() {
            return Err(LiveRuntimeError::ReconnectPolicy(
                "reconnect delays must be nonzero".to_owned(),
            ));
        }
        if min_healthy_duration.is_zero() {
            return Err(LiveRuntimeError::ReconnectPolicy(
                "reconnect healthy duration must be nonzero".to_owned(),
            ));
        }
        if base_delay > max_delay {
            return Err(LiveRuntimeError::ReconnectPolicy(
                "reconnect base delay must not exceed max delay".to_owned(),
            ));
        }
        Ok(Self {
            base_delay,
            max_delay,
            deterministic_spread,
            min_healthy_duration,
        })
    }

    #[must_use]
    pub fn delay_for_attempt(self, attempt: u64) -> Duration {
        let exponent = attempt.saturating_sub(1).min(31);
        let multiplier = 1_u64.checked_shl(exponent as u32).unwrap_or(u64::MAX);
        let exponential = saturating_duration_mul(self.base_delay, multiplier).min(self.max_delay);
        exponential.saturating_add(self.spread_for_attempt(attempt))
    }

    fn spread_for_attempt(self, attempt: u64) -> Duration {
        let spread_nanos = self
            .deterministic_spread
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        if spread_nanos == 0 {
            return Duration::ZERO;
        }
        let mixed = attempt
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        Duration::from_nanos(mixed % (spread_nanos + 1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectLoop {
    policy: ReconnectPolicy,
    attempt: u64,
    readiness: RuntimeReadiness,
    halted: bool,
    healthy_since: Option<Instant>,
    subscribed_to_wakeups: bool,
    reconciled_after_subscribe: bool,
}

impl ReconnectLoop {
    #[must_use]
    pub const fn new(policy: ReconnectPolicy) -> Self {
        Self {
            policy,
            attempt: 0,
            readiness: RuntimeReadiness::Unavailable,
            halted: false,
            healthy_since: None,
            subscribed_to_wakeups: false,
            reconciled_after_subscribe: false,
        }
    }

    #[must_use]
    pub const fn readiness(&self) -> RuntimeReadiness {
        self.readiness
    }

    #[must_use]
    pub const fn attempts(&self) -> u64 {
        self.attempt
    }

    #[must_use]
    pub const fn is_halted(&self) -> bool {
        self.halted
    }

    pub fn record_disconnect(&mut self) {
        self.record_disconnect_at(Instant::now());
    }

    pub fn record_disconnect_at(&mut self, now: Instant) {
        self.reset_backoff_if_min_healthy_elapsed(now);
        self.readiness = RuntimeReadiness::Unavailable;
        self.healthy_since = None;
        self.subscribed_to_wakeups = false;
        self.reconciled_after_subscribe = false;
    }

    pub fn record_connected_needs_reconciliation(&mut self) {
        self.readiness = RuntimeReadiness::Unavailable;
        self.healthy_since = None;
        self.subscribed_to_wakeups = false;
        self.reconciled_after_subscribe = false;
    }

    pub fn record_catch_up_complete(&mut self) {
        self.record_catch_up_complete_at(Instant::now());
    }

    pub fn record_catch_up_complete_at(&mut self, now: Instant) {
        if !self.halted {
            self.readiness = RuntimeReadiness::Ready;
            self.healthy_since = Some(now);
        }
    }

    pub fn record_subscribed_for_wakeups(&mut self) {
        self.readiness = RuntimeReadiness::Unavailable;
        self.healthy_since = None;
        self.subscribed_to_wakeups = true;
        self.reconciled_after_subscribe = false;
    }

    pub fn record_reconciled_to_head(&mut self) -> Result<(), LiveRuntimeError> {
        if !self.subscribed_to_wakeups {
            return Err(LiveRuntimeError::ReconnectOrder(
                "must subscribe to head wakeups before reconciliation".to_owned(),
            ));
        }
        self.readiness = RuntimeReadiness::Unavailable;
        self.reconciled_after_subscribe = true;
        Ok(())
    }

    pub fn record_ready_after_reconnect_at(
        &mut self,
        now: Instant,
    ) -> Result<(), LiveRuntimeError> {
        if !self.subscribed_to_wakeups || !self.reconciled_after_subscribe {
            return Err(LiveRuntimeError::ReconnectOrder(
                "must subscribe and reconcile before marking live tracker ready".to_owned(),
            ));
        }
        self.subscribed_to_wakeups = false;
        self.reconciled_after_subscribe = false;
        self.record_catch_up_complete_at(now);
        Ok(())
    }

    #[must_use]
    pub fn record_availability_failure(&mut self) -> ReconnectAction {
        if self.halted {
            return ReconnectAction::Halt;
        }
        self.readiness = RuntimeReadiness::Unavailable;
        self.healthy_since = None;
        self.subscribed_to_wakeups = false;
        self.reconciled_after_subscribe = false;
        self.attempt = self.attempt.saturating_add(1);
        ReconnectAction::RetryAfter(self.policy.delay_for_attempt(self.attempt))
    }

    #[must_use]
    pub fn record_correctness_error(&mut self) -> ReconnectAction {
        self.readiness = RuntimeReadiness::Unavailable;
        self.healthy_since = None;
        self.subscribed_to_wakeups = false;
        self.reconciled_after_subscribe = false;
        self.halted = true;
        ReconnectAction::Halt
    }

    #[must_use]
    pub fn record_pipeline_error(&mut self, error: &LivePipelineError) -> ReconnectAction {
        match error.classify() {
            LivePipelineErrorClass::Transient => self.record_availability_failure(),
            LivePipelineErrorClass::Correctness => self.record_correctness_error(),
        }
    }

    fn reset_backoff_if_min_healthy_elapsed(&mut self, now: Instant) {
        if self.healthy_since.is_some_and(|healthy_since| {
            now.duration_since(healthy_since) >= self.policy.min_healthy_duration
        }) {
            self.attempt = 0;
        }
    }
}

fn saturating_duration_mul(duration: Duration, multiplier: u64) -> Duration {
    let nanos = duration
        .as_nanos()
        .saturating_mul(u128::from(multiplier))
        .min(u128::from(u64::MAX));
    Duration::from_nanos(nanos as u64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LivePipelineStage {
    Fetch,
    Coordinate,
    RawPassThrough,
    Write,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LivePipelineError {
    #[error("live pipeline correctness error: {0}")]
    Chain(#[from] ChainError),
    #[error("live pipeline transient RPC error: {0}")]
    RpcTransient(String),
    #[error("live pipeline transient WebSocket error: {0}")]
    WebSocketTransient(String),
    #[error("live pipeline transient database error: {0}")]
    DatabaseTransient(String),
    #[error("live pipeline channel for {0:?} closed")]
    ChannelClosed(LivePipelineStage),
    #[error("live pipeline shutdown while {0:?} was waiting")]
    Shutdown(LivePipelineStage),
    #[error("live pipeline stage {0:?} panicked")]
    StagePanicked(LivePipelineStage),
    #[error("live pipeline stage {0:?} was cancelled")]
    StageCancelled(LivePipelineStage),
    #[error("live pipeline stage {0:?} exited before shutdown")]
    StageExitedEarly(LivePipelineStage),
    #[error("live pipeline supervisor task was cancelled")]
    SupervisorCancelled,
    #[error("live pipeline queue capacity must be nonzero")]
    InvalidQueueCapacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LivePipelineErrorClass {
    Transient,
    Correctness,
}

impl LivePipelineError {
    #[must_use]
    pub const fn classify(&self) -> LivePipelineErrorClass {
        match self {
            Self::RpcTransient(_) | Self::WebSocketTransient(_) | Self::DatabaseTransient(_) => {
                LivePipelineErrorClass::Transient
            }
            Self::Chain(_)
            | Self::ChannelClosed(_)
            | Self::Shutdown(_)
            | Self::StagePanicked(_)
            | Self::StageCancelled(_)
            | Self::StageExitedEarly(_)
            | Self::SupervisorCancelled
            | Self::InvalidQueueCapacity => LivePipelineErrorClass::Correctness,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivePipelineConfig {
    pub fetch_queue: usize,
    pub coordinate_queue: usize,
    pub raw_queue: usize,
    pub write_queue: usize,
}

impl LivePipelineConfig {
    #[must_use]
    pub const fn new(
        fetch_queue: usize,
        coordinate_queue: usize,
        raw_queue: usize,
        write_queue: usize,
    ) -> Self {
        Self {
            fetch_queue,
            coordinate_queue,
            raw_queue,
            write_queue,
        }
    }
}

pub struct LivePipelineQueues<Fetch, Coordinate, Raw, Write> {
    pub fetch_tx: mpsc::Sender<Fetch>,
    pub fetch_rx: mpsc::Receiver<Fetch>,
    pub coordinate_tx: mpsc::Sender<Coordinate>,
    pub coordinate_rx: mpsc::Receiver<Coordinate>,
    pub raw_tx: mpsc::Sender<Raw>,
    pub raw_rx: mpsc::Receiver<Raw>,
    pub write_tx: mpsc::Sender<Write>,
    pub write_rx: mpsc::Receiver<Write>,
}

/// Builds the bounded live pipeline queues in fetch -> coordinate -> raw pass-through -> write
/// order. Each queue must have an explicit nonzero capacity.
///
/// # Errors
///
/// Returns an error when any queue capacity is zero.
pub fn bounded_live_pipeline_queues<Fetch, Coordinate, Raw, Write>(
    config: LivePipelineConfig,
) -> Result<LivePipelineQueues<Fetch, Coordinate, Raw, Write>, LivePipelineError> {
    if config.fetch_queue == 0
        || config.coordinate_queue == 0
        || config.raw_queue == 0
        || config.write_queue == 0
    {
        return Err(LivePipelineError::InvalidQueueCapacity);
    }

    let (fetch_tx, fetch_rx) = mpsc::channel(config.fetch_queue);
    let (coordinate_tx, coordinate_rx) = mpsc::channel(config.coordinate_queue);
    let (raw_tx, raw_rx) = mpsc::channel(config.raw_queue);
    let (write_tx, write_rx) = mpsc::channel(config.write_queue);

    Ok(LivePipelineQueues {
        fetch_tx,
        fetch_rx,
        coordinate_tx,
        coordinate_rx,
        raw_tx,
        raw_rx,
        write_tx,
        write_rx,
    })
}

/// Sends one item to the next bounded stage, waiting behind normal backpressure until the next
/// stage has capacity or shutdown is requested.
///
/// # Errors
///
/// Returns a pipeline error when the queue closes or shutdown is requested.
pub async fn send_with_shutdown<T>(
    stage: LivePipelineStage,
    sender: &mpsc::Sender<T>,
    item: T,
    shutdown: &CancellationToken,
) -> Result<(), LivePipelineError>
where
    T: Send + 'static,
{
    tokio::select! {
        () = shutdown.cancelled() => Err(LivePipelineError::Shutdown(stage)),
        result = sender.send(item) => result.map_err(|_| LivePipelineError::ChannelClosed(stage)),
    }
}

/// Runs the M4 raw decode stage as a strict pass-through. ABI decoding is intentionally deferred.
///
/// # Errors
///
/// Returns a pipeline error when shutdown, timeout, or downstream closure occurs.
pub async fn raw_pass_through_stage<T>(
    mut receiver: mpsc::Receiver<T>,
    sender: mpsc::Sender<T>,
    shutdown: CancellationToken,
) -> Result<(), LivePipelineError>
where
    T: Send + 'static,
{
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Err(LivePipelineError::Shutdown(LivePipelineStage::RawPassThrough)),
            item = receiver.recv() => {
                let Some(item) = item else {
                    return Ok(());
                };
                send_with_shutdown(
                    LivePipelineStage::RawPassThrough,
                    &sender,
                    item,
                    &shutdown,
                )
                .await?;
            }
        }
    }
}

/// Runs the final write stage. This is the only pipeline stage that receives a mutating closure.
///
/// # Errors
///
/// Returns a pipeline error when shutdown is requested or the writer closure fails.
pub async fn writer_stage<T, Write, WriteFuture>(
    mut receiver: mpsc::Receiver<T>,
    mut write: Write,
    shutdown: CancellationToken,
) -> Result<(), LivePipelineError>
where
    T: Send + 'static,
    Write: FnMut(T) -> WriteFuture,
    WriteFuture: Future<Output = Result<(), LivePipelineError>>,
{
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Err(LivePipelineError::Shutdown(LivePipelineStage::Write)),
            item = receiver.recv() => {
                let Some(item) = item else {
                    return Ok(());
                };
                write(item).await?;
            }
        }
    }
}

pub struct LivePipelineSupervisor {
    shutdown: CancellationToken,
    stages: JoinSet<(LivePipelineStage, Result<(), LivePipelineError>)>,
}

impl LivePipelineSupervisor {
    #[must_use]
    pub fn new(shutdown: CancellationToken) -> Self {
        Self {
            shutdown,
            stages: JoinSet::new(),
        }
    }

    pub fn spawn<Fut>(&mut self, stage: LivePipelineStage, future: Fut)
    where
        Fut: Future<Output = Result<(), LivePipelineError>> + Send + 'static,
    {
        self.stages.spawn(async move {
            let result = AssertUnwindSafe(future).catch_unwind().await;
            let result = match result {
                Ok(result) => result,
                Err(_) => Err(LivePipelineError::StagePanicked(stage)),
            };
            (stage, result)
        });
    }

    /// Waits for the first stage to finish, then cancels and drains the rest of the pipeline.
    /// A clean stage exit before shutdown is treated as a pipeline failure because live stages
    /// should run until cancellation or an explicit error.
    ///
    /// # Errors
    ///
    /// Returns the first stage error, panic, premature exit, or supervisor cancellation.
    pub async fn run_until_first_exit(mut self) -> Result<(), LivePipelineError> {
        let Some(joined) = self.stages.join_next().await else {
            return Ok(());
        };

        let first_result = match joined {
            Ok((_stage, Ok(()))) if self.shutdown.is_cancelled() => Ok(()),
            Ok((stage, Ok(()))) => Err(LivePipelineError::StageExitedEarly(stage)),
            Ok((_stage, Err(error))) => Err(error),
            Err(error) if error.is_cancelled() => Err(LivePipelineError::SupervisorCancelled),
            Err(error) if error.is_panic() => {
                Err(LivePipelineError::StagePanicked(LivePipelineStage::Fetch))
            }
            Err(_) => Err(LivePipelineError::SupervisorCancelled),
        };

        self.shutdown.cancel();
        while self.stages.join_next().await.is_some() {}
        first_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::Notify;

    #[test]
    fn windowed_live_budget_returns_delay_until_window_rollover() {
        let start = Instant::now();
        let mut budget = WindowedLiveBudget::new(1, Duration::from_secs(60), start).unwrap();

        assert_eq!(
            budget
                .record_at(RpcMethod::GetBlockByNumber, start)
                .unwrap(),
            None
        );
        assert_eq!(
            budget
                .record_at(RpcMethod::GetBlockByNumber, start + Duration::from_secs(2))
                .unwrap(),
            Some(Duration::from_secs(58))
        );
        assert_eq!(
            budget
                .record_at(RpcMethod::GetBlockByNumber, start + Duration::from_secs(60))
                .unwrap(),
            None
        );
    }

    #[test]
    fn reconnect_backoff_is_unbounded_capped_and_uses_deterministic_spread() {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(10),
            Duration::from_millis(500),
            Duration::from_secs(30),
        )
        .unwrap();
        let mut reconnect = ReconnectLoop::new(policy);

        let ReconnectAction::RetryAfter(first) = reconnect.record_availability_failure() else {
            panic!("availability failures retry");
        };
        let ReconnectAction::RetryAfter(second) = reconnect.record_availability_failure() else {
            panic!("availability failures retry");
        };
        assert!(first >= Duration::from_secs(1));
        assert!(second > first);

        let mut last = second;
        let mut observed_spread_past_cap = false;
        for _ in 0..128 {
            let ReconnectAction::RetryAfter(delay) = reconnect.record_availability_failure() else {
                panic!("availability failures keep retrying");
            };
            assert!(delay <= Duration::from_millis(10_500));
            observed_spread_past_cap |= delay > Duration::from_secs(10);
            last = delay;
        }
        assert!(last >= Duration::from_secs(10));
        assert!(observed_spread_past_cap);
        assert_eq!(reconnect.attempts(), 130);
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Unavailable);
    }

    #[test]
    fn reconnect_policy_validation_has_specific_error_variant() {
        assert!(matches!(
            ReconnectPolicy::new(
                Duration::ZERO,
                Duration::from_secs(10),
                Duration::ZERO,
                Duration::from_secs(30),
            ),
            Err(LiveRuntimeError::ReconnectPolicy(_))
        ));
    }

    #[test]
    fn reconnect_backoff_resets_only_after_minimum_healthy_duration() {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(10),
            Duration::ZERO,
            Duration::from_secs(5),
        )
        .unwrap();
        let mut reconnect = ReconnectLoop::new(policy);
        let start = Instant::now();

        assert_eq!(
            reconnect.record_availability_failure(),
            ReconnectAction::RetryAfter(Duration::from_secs(1))
        );
        reconnect.record_catch_up_complete_at(start);
        assert_eq!(reconnect.attempts(), 1);
        reconnect.record_disconnect_at(start + Duration::from_secs(2));
        assert_eq!(
            reconnect.record_availability_failure(),
            ReconnectAction::RetryAfter(Duration::from_secs(2))
        );
        reconnect.record_catch_up_complete_at(start + Duration::from_secs(3));
        reconnect.record_disconnect_at(start + Duration::from_secs(4));
        assert_eq!(
            reconnect.record_availability_failure(),
            ReconnectAction::RetryAfter(Duration::from_secs(4))
        );

        reconnect.record_catch_up_complete_at(start + Duration::from_secs(10));
        reconnect.record_disconnect_at(start + Duration::from_secs(16));
        assert_eq!(
            reconnect.record_availability_failure(),
            ReconnectAction::RetryAfter(Duration::from_secs(1))
        );
        assert_eq!(reconnect.attempts(), 1);
    }

    #[test]
    fn reconnect_readiness_stays_unavailable_until_catch_up_completes() {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::from_secs(30),
        )
        .unwrap();
        let mut reconnect = ReconnectLoop::new(policy);

        assert_eq!(reconnect.readiness(), RuntimeReadiness::Unavailable);
        reconnect.record_disconnect();
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Unavailable);
        reconnect.record_connected_needs_reconciliation();
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Unavailable);
        reconnect.record_catch_up_complete();
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Ready);
        assert_eq!(reconnect.attempts(), 0);
    }

    #[test]
    fn reconnect_order_subscribes_then_reconciles_then_marks_ready() {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::from_secs(30),
        )
        .unwrap();
        let mut reconnect = ReconnectLoop::new(policy);
        let now = Instant::now();

        assert!(matches!(
            reconnect.record_reconciled_to_head(),
            Err(LiveRuntimeError::ReconnectOrder(_))
        ));

        reconnect.record_subscribed_for_wakeups();
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Unavailable);
        assert!(matches!(
            reconnect.record_ready_after_reconnect_at(now),
            Err(LiveRuntimeError::ReconnectOrder(_))
        ));

        reconnect.record_reconciled_to_head().unwrap();
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Unavailable);
        reconnect.record_ready_after_reconnect_at(now).unwrap();
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Ready);
    }

    #[test]
    fn correctness_error_halts_reconnect_loop() {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::from_secs(30),
        )
        .unwrap();
        let mut reconnect = ReconnectLoop::new(policy);

        assert_eq!(reconnect.record_correctness_error(), ReconnectAction::Halt);
        assert!(reconnect.is_halted());
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Unavailable);
        assert_eq!(
            reconnect.record_availability_failure(),
            ReconnectAction::Halt
        );
    }

    #[test]
    fn max_depth_pipeline_error_halts_reconnect_without_retry() {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::from_secs(30),
        )
        .unwrap();
        let mut reconnect = ReconnectLoop::new(policy);
        let error = LivePipelineError::Chain(ChainError::MaxDepthExceeded { max_depth: 3 });

        assert_eq!(error.classify(), LivePipelineErrorClass::Correctness);
        assert_eq!(
            reconnect.record_pipeline_error(&error),
            ReconnectAction::Halt
        );
        assert!(reconnect.is_halted());
        assert_eq!(reconnect.attempts(), 0);
    }

    #[test]
    fn bounded_pipeline_queues_use_configured_capacities() {
        let queues: LivePipelineQueues<u64, u64, u64, u64> =
            bounded_live_pipeline_queues(LivePipelineConfig::new(1, 2, 3, 4)).unwrap();

        assert_eq!(queues.fetch_tx.max_capacity(), 1);
        assert_eq!(queues.coordinate_tx.max_capacity(), 2);
        assert_eq!(queues.raw_tx.max_capacity(), 3);
        assert_eq!(queues.write_tx.max_capacity(), 4);
    }

    #[test]
    fn bounded_pipeline_rejects_zero_capacity() {
        let result =
            bounded_live_pipeline_queues::<u64, u64, u64, u64>(LivePipelineConfig::new(1, 0, 1, 1));

        assert!(matches!(
            result,
            Err(LivePipelineError::InvalidQueueCapacity)
        ));
    }

    #[tokio::test]
    async fn full_queue_send_waits_for_capacity_without_failing() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(10_u64).await.unwrap();
        let shutdown = CancellationToken::new();
        let send_shutdown = shutdown.clone();

        let blocked_send = tokio::spawn(async move {
            send_with_shutdown(LivePipelineStage::Fetch, &sender, 11_u64, &send_shutdown).await
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!blocked_send.is_finished());
        assert_eq!(receiver.recv().await, Some(10));
        assert_eq!(blocked_send.await.unwrap(), Ok(()));
        assert_eq!(receiver.recv().await, Some(11));
    }

    #[tokio::test]
    async fn shutdown_cancels_full_channel_send_without_hanging() {
        let (sender, _receiver) = mpsc::channel(1);
        sender.send(10_u64).await.unwrap();
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let result = tokio::time::timeout(
            Duration::from_millis(50),
            send_with_shutdown(LivePipelineStage::Coordinate, &sender, 11_u64, &shutdown),
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            Err(LivePipelineError::Shutdown(LivePipelineStage::Coordinate))
        );
    }

    #[tokio::test]
    async fn raw_pass_through_preserves_event_order() {
        let (raw_tx, raw_rx) = mpsc::channel(2);
        let (write_tx, mut write_rx) = mpsc::channel(2);
        let shutdown = CancellationToken::new();

        raw_tx.send(31_u64).await.unwrap();
        raw_tx.send(32_u64).await.unwrap();
        drop(raw_tx);

        raw_pass_through_stage(raw_rx, write_tx, shutdown)
            .await
            .unwrap();

        assert_eq!(write_rx.recv().await, Some(31));
        assert_eq!(write_rx.recv().await, Some(32));
        assert_eq!(write_rx.recv().await, None);
    }

    #[tokio::test]
    async fn writer_stage_is_the_only_mutating_stage() {
        let (write_tx, write_rx) = mpsc::channel(2);
        write_tx.send(41_u64).await.unwrap();
        write_tx.send(42_u64).await.unwrap();
        drop(write_tx);
        let mutation_count = Arc::new(AtomicUsize::new(0));
        let mutation_count_for_writer = Arc::clone(&mutation_count);

        writer_stage(
            write_rx,
            move |item| {
                let mutation_count = Arc::clone(&mutation_count_for_writer);
                async move {
                    assert!(item == 41 || item == 42);
                    mutation_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(mutation_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn shutdown_waits_for_in_flight_write_to_finish() {
        let (write_tx, write_rx) = mpsc::channel(1);
        write_tx.send(41_u64).await.unwrap();
        drop(write_tx);
        let shutdown = CancellationToken::new();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let completed = Arc::new(AtomicUsize::new(0));
        let started_for_writer = Arc::clone(&started);
        let release_for_writer = Arc::clone(&release);
        let completed_for_writer = Arc::clone(&completed);
        let writer_shutdown = shutdown.clone();

        let task = tokio::spawn(async move {
            writer_stage(
                write_rx,
                move |_| {
                    let started = Arc::clone(&started_for_writer);
                    let release = Arc::clone(&release_for_writer);
                    let completed = Arc::clone(&completed_for_writer);
                    async move {
                        started.notify_one();
                        release.notified().await;
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                writer_shutdown,
            )
            .await
        });

        started.notified().await;
        shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!task.is_finished());
        assert_eq!(completed.load(Ordering::SeqCst), 0);

        release.notify_one();
        let result = task.await.unwrap();
        assert!(
            result == Ok(())
                || result == Err(LivePipelineError::Shutdown(LivePipelineStage::Write))
        );
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pipeline_supervisor_reports_upstream_panic_over_downstream_clean_exit() {
        let shutdown = CancellationToken::new();
        let (sender, mut receiver) = mpsc::channel::<u64>(1);
        let mut supervisor = LivePipelineSupervisor::new(shutdown);

        supervisor.spawn(LivePipelineStage::Fetch, async move {
            drop(sender);
            panic!("fetch stage panic is supervised");
        });
        supervisor.spawn(LivePipelineStage::Write, async move {
            assert_eq!(receiver.recv().await, None);
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(())
        });

        assert_eq!(
            supervisor.run_until_first_exit().await,
            Err(LivePipelineError::StagePanicked(LivePipelineStage::Fetch))
        );
    }
}
