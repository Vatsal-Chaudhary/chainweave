use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, Instant},
};

use chainweave_core::{
    AppConfig, BackfillRange, BlockHash, BlockHeader, ChainBatch, ChainError, ConfigError,
    FetchedRange, LiveConfig, LiveError, LiveReport, LiveSink, LiveSource, LiveSourceErrorKind,
    LiveStartPoint, LiveTracker, OrderedCommitCoordinator, RangeCommitSink, RetryPolicy, RpcBudget,
    RpcMethod, ValidationProfile, VerifierStatus, redact_url,
};
use chainweave_rpc::{
    ContractLogFilter, NewHeadWakeupSender, RpcClient, RpcError, capture_target_head_with_retry,
    fetch_header_by_hash_with_retry, fetch_header_by_number_with_retry, new_head_wakeup_channel,
    retry_rpc_request, spawn_new_heads_wakeup,
};
use chainweave_sink::{
    AbiRegistry, DurableChainBatch, HealthState, IndexedBlock, LiveStatusSnapshot,
    ObservabilityError, ObservabilityServer, PostgresBackfillCommitter, PostgresChainWriter,
    PostgresStateError,
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
    Task(#[from] LiveTaskError),
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
    pub abi_registry: Option<Arc<AbiRegistry>>,
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
        self.decode_block_logs(&mut block);
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
        let mut block = retry_rpc_request(
            self.config.retry_policy,
            self.config.rpc_timeout,
            || RpcError::Request(format!("timed out fetching block by hash {hash:?}")),
            || self.client.fetch_block_by_hash(hash),
        )
        .await?;
        self.decode_block_logs(&mut block);
        Ok(block)
    }

    fn decode_block_logs(&self, block: &mut IndexedBlock) {
        if let Some(registry) = &self.config.abi_registry {
            registry.decode_logs_in_place(&mut block.logs);
        }
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct PostgresLiveStore {
    writer: PostgresChainWriter,
}

struct BlockingTrackerSource {
    source: RpcReconciliationSource,
    handle: tokio::runtime::Handle,
    shutdown: CancellationToken,
}

impl BlockingTrackerSource {
    fn new(
        source: RpcReconciliationSource,
        handle: tokio::runtime::Handle,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            source,
            handle,
            shutdown,
        }
    }

    fn into_inner(self) -> RpcReconciliationSource {
        self.source
    }
}

impl LiveSource for BlockingTrackerSource {
    type Block = IndexedBlock;
    type Error = String;

    fn current_head(&mut self) -> Result<BlockHeader, Self::Error> {
        let handle = self.handle.clone();
        let shutdown = self.shutdown.clone();
        let header = block_on_with_shutdown(&handle, shutdown, self.source.primary.poll_head())?;
        self.source.last_head = Some(header);
        let shutdown = self.shutdown.clone();
        block_on_with_shutdown(&handle, shutdown, self.source.verify_head(header))?;
        Ok(header)
    }

    fn block_by_number(&mut self, height: u64) -> Result<Self::Block, Self::Error> {
        let handle = self.handle.clone();
        let shutdown = self.shutdown.clone();
        block_on_with_shutdown(
            &handle,
            shutdown,
            self.source.primary.block_by_number(height),
        )
    }

    fn block_by_hash(&mut self, hash: BlockHash) -> Result<Self::Block, Self::Error> {
        let handle = self.handle.clone();
        let shutdown = self.shutdown.clone();
        block_on_with_shutdown(&handle, shutdown, self.source.primary.block_by_hash(hash))
    }

    fn header_by_number(&mut self, height: u64) -> Result<Option<BlockHeader>, Self::Error> {
        let handle = self.handle.clone();
        let shutdown = self.shutdown.clone();
        block_on_with_shutdown(
            &handle,
            shutdown,
            self.source.primary.header_by_number(height),
        )
        .map(Some)
    }

    fn header_by_hash(&mut self, hash: BlockHash) -> Result<Option<BlockHeader>, Self::Error> {
        let handle = self.handle.clone();
        let shutdown = self.shutdown.clone();
        block_on_with_shutdown(&handle, shutdown, self.source.primary.header_by_hash(hash))
    }

    fn block_header(block: &Self::Block) -> BlockHeader {
        block.header
    }

    fn block_logs_match_header(_block: &Self::Block) -> bool {
        true
    }

    fn source_error_kind(error: &Self::Error) -> LiveSourceErrorKind {
        if source_error_requests_rereconcile(error) {
            LiveSourceErrorKind::Reconcile
        } else {
            LiveSourceErrorKind::Transient
        }
    }

    fn verify_recent(
        &mut self,
        height: u64,
        hash: BlockHash,
    ) -> Result<VerifierStatus, Self::Error> {
        let Some(verifier) = &mut self.source.verifier else {
            return Ok(VerifierStatus::Unavailable);
        };
        let handle = self.handle.clone();
        let shutdown = self.shutdown.clone();
        match block_on_with_shutdown(&handle, shutdown, verifier.header_by_number(height)) {
            Ok(verifier) if verifier.hash == hash => Ok(VerifierStatus::Match),
            Ok(_) => {
                self.source.verifier_disagreements =
                    self.source.verifier_disagreements.saturating_add(1);
                Ok(VerifierStatus::Disagree)
            }
            Err(_) => Ok(VerifierStatus::Unavailable),
        }
    }
}

struct BlockingPostgresLiveSink {
    store: PostgresLiveStore,
    handle: tokio::runtime::Handle,
    shutdown: CancellationToken,
    parent_anchor: Option<BlockHeader>,
}

impl BlockingPostgresLiveSink {
    fn new(
        store: PostgresLiveStore,
        handle: tokio::runtime::Handle,
        shutdown: CancellationToken,
        parent_anchor: Option<BlockHeader>,
    ) -> Self {
        Self {
            store,
            handle,
            shutdown,
            parent_anchor,
        }
    }

    fn block_on<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, LiveRuntimeError>>,
    ) -> Result<T, String> {
        block_on_with_shutdown(&self.handle, self.shutdown.clone(), future)
    }
}

fn block_on_with_shutdown<T>(
    handle: &tokio::runtime::Handle,
    shutdown: CancellationToken,
    future: impl std::future::Future<Output = Result<T, LiveRuntimeError>>,
) -> Result<T, String> {
    block_on_runtime_with_shutdown(handle, shutdown, future).map_err(|error| error.to_string())
}

fn block_on_runtime_with_shutdown<T>(
    handle: &tokio::runtime::Handle,
    shutdown: CancellationToken,
    future: impl std::future::Future<Output = Result<T, LiveRuntimeError>>,
) -> Result<T, LiveRuntimeError> {
    handle.block_on(async move {
        tokio::select! {
            () = shutdown.cancelled() => Err(LiveRuntimeError::Task(LiveTaskError::TaskCancelled(LiveTask::Tracker))),
            result = future => result,
        }
    })
}

impl RangeCommitSink<IndexedBlock> for BlockingPostgresLiveSink {
    type Error = String;

    fn commit_range(&mut self, fetched: FetchedRange<IndexedBlock>) -> Result<(), Self::Error> {
        let header = fetched
            .headers
            .last()
            .copied()
            .ok_or_else(|| "cannot commit empty live range".to_owned())?;
        let block = fetched
            .logs
            .into_iter()
            .next()
            .ok_or_else(|| "missing live block payload".to_owned())?;
        self.block_on(self.store.commit_one_block(self.parent_anchor, block))?;
        self.parent_anchor = Some(header);
        Ok(())
    }
}

impl LiveSink<IndexedBlock> for BlockingPostgresLiveSink {
    fn checkpoint(&self) -> Option<BlockHeader> {
        self.handle
            .block_on(self.store.checkpoint_header())
            .ok()
            .flatten()
    }

    fn canonical_header_at_height(&self, height: u64) -> Option<BlockHeader> {
        self.handle
            .block_on(self.store.canonical_header_at_height(height))
            .ok()
            .flatten()
    }

    fn canonical_header_by_hash(&self, hash: BlockHash) -> Option<BlockHeader> {
        self.handle
            .block_on(self.store.canonical_header_by_hash(hash))
            .ok()
            .flatten()
    }

    fn commit_batch(
        &mut self,
        batch: ChainBatch,
        apply_blocks: Vec<IndexedBlock>,
    ) -> Result<(), Self::Error> {
        let durable = DurableChainBatch::from_chain_batch(&batch, apply_blocks)
            .map_err(|error| error.to_string())?;
        self.handle
            .block_on(self.store.writer().apply_batch(&durable))
            .map_err(|error| error.to_string())?;
        self.parent_anchor = self.checkpoint();
        Ok(())
    }
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
        abi_registry: super::abi_registry_from_config(config).map_err(ConfigError::Invalid)?,
    };
    let (primary_redacted, verifier_redacted) = live_endpoint_labels(config);
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
    let mut supervisor = LiveTaskSupervisor::new(shutdown.clone());
    supervisor.spawn(
        LiveTask::HealthServer,
        health_server_task(observability, shutdown.clone()),
    );

    info!(
        primary_rpc = %primary_redacted,
        verifier_rpc = verifier_redacted.as_deref().unwrap_or("none"),
        observability_addr = %observability_addr,
        "starting live runner"
    );

    let primary_client = match connect_rpc(&config.rpc.primary_url).await {
        Ok(client) => client,
        Err(error) => {
            health.mark_degraded("rpc").await;
            return Err(error);
        }
    };
    let primary_head =
        match capture_target_head_with_retry(&primary_client, retry_policy, options.rpc_timeout)
            .await
        {
            Ok(head) => head,
            Err(error) => {
                health.mark_degraded("rpc").await;
                return Err(error.into());
            }
        };
    if let Some(expected) = &config.expected_chain {
        if let Err(error) = RpcClient::verify_identity(&primary_head, expected) {
            health
                .mark_correctness_stop("chain_identity_mismatch")
                .await;
            return Err(error.into());
        }
    }

    let verifier_client = if let Some(url) = &config.rpc.verifier_url {
        let verifier_client = match connect_rpc(url).await {
            Ok(client) => client,
            Err(error) => {
                health.mark_degraded("rpc").await;
                return Err(error);
            }
        };
        let verifier_head = match capture_target_head_with_retry(
            &verifier_client,
            retry_policy,
            options.rpc_timeout,
        )
        .await
        {
            Ok(head) => head,
            Err(error) => {
                health.mark_degraded("rpc").await;
                return Err(error.into());
            }
        };
        if let Err(error) = verify_verifier_chain_identity(
            primary_head.chain_id,
            primary_head.genesis_hash.to_string(),
            verifier_head.chain_id,
            verifier_head.genesis_hash.to_string(),
        ) {
            health
                .mark_correctness_stop("chain_identity_mismatch")
                .await;
            return Err(error.into());
        }
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
    if let Err(error) = writer
        .ensure_chain_identity(primary_head.genesis_block_hash())
        .await
    {
        if matches!(error, PostgresStateError::ChainIdentityMismatch { .. }) {
            health
                .mark_correctness_stop("chain_identity_mismatch")
                .await;
        }
        return Err(error.into());
    }
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
    let reconnect = ReconnectLoop::new(policy);
    let (wakeups, wakeup_rx) = new_head_wakeup_channel();
    if is_ws_url(&config.rpc.primary_url) {
        supervisor.spawn(
            LiveTask::Wakeups,
            websocket_wakeup_task(
                config.rpc.primary_url.clone(),
                wakeups.clone(),
                health.clone(),
                shutdown.clone(),
            ),
        );
    }

    let primary_source = RpcLiveSource::new(primary_client, runtime_config.clone())?;
    let verifier_source = verifier_client
        .map(|client| RpcLiveSource::new(client, runtime_config.clone()))
        .transpose()?;
    let source = RpcReconciliationSource::new(primary_source, verifier_source);
    let context = StatusContext::new(&primary_redacted, verifier_redacted.as_deref());
    supervisor.spawn(
        LiveTask::Poll,
        poll_wakeup_task(options.poll_interval, wakeups.clone(), shutdown.clone()),
    );
    supervisor.spawn(
        LiveTask::Tracker,
        tracker_task(
            writer,
            source,
            options.clone(),
            config.indexer.max_reorg_depth,
            reconnect,
            health,
            context,
            wakeup_rx,
            shutdown.clone(),
        ),
    );

    run_live_supervisor(supervisor, options.shutdown_timeout).await
}

async fn run_live_supervisor(
    supervisor: LiveTaskSupervisor,
    _shutdown_timeout: Duration,
) -> Result<(), LiveRuntimeError> {
    supervisor.run_until_first_exit().await.map_err(Into::into)
}

#[derive(Debug, Clone)]
struct StatusContext {
    primary_rpc_url: String,
    verifier_rpc_url: Option<String>,
}

impl StatusContext {
    fn new(primary_rpc_url: &str, verifier_rpc_url: Option<&str>) -> Self {
        Self {
            primary_rpc_url: primary_rpc_url.to_owned(),
            verifier_rpc_url: verifier_rpc_url.map(ToOwned::to_owned),
        }
    }
}

async fn health_server_task(
    observability: ObservabilityServer,
    shutdown: CancellationToken,
) -> Result<(), LiveTaskError> {
    tokio::select! {
        () = shutdown.cancelled() => Ok(()),
        result = observability.serve() => {
            result.map_err(|error| LiveTaskError::TaskFailed(LiveTask::HealthServer, error.to_string()))
        }
    }
}

async fn websocket_wakeup_task(
    ws_url: Url,
    wakeups: NewHeadWakeupSender,
    health: HealthState,
    shutdown: CancellationToken,
) -> Result<(), LiveTaskError> {
    loop {
        let mut task = spawn_new_heads_wakeup(ws_url.clone(), wakeups.clone());
        tokio::select! {
            () = shutdown.cancelled() => {
                task.abort();
                let _ = task.await;
                return Ok(());
            }
            joined = &mut task => {
                match joined {
                    Ok(Ok(())) => return Ok(()),
                    Ok(Err(error)) => {
                        health.mark_degraded("rpc").await;
                        warn!(error = %error, "newHeads subscription ended; reconnecting");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    Err(error) if error.is_cancelled() => {
                        return Err(LiveTaskError::TaskCancelled(LiveTask::Wakeups));
                    }
                    Err(error) if error.is_panic() => {
                        return Err(LiveTaskError::TaskPanicked(LiveTask::Wakeups));
                    }
                    Err(error) => {
                        return Err(LiveTaskError::WebSocketTransient(error.to_string()));
                    }
                }
            }
        }
    }
}

async fn poll_wakeup_task(
    poll_interval: Duration,
    wakeups: NewHeadWakeupSender,
    shutdown: CancellationToken,
) -> Result<(), LiveTaskError> {
    let mut poll = tokio::time::interval(poll_interval);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            _ = poll.tick() => {
                let _ = wakeups.try_wake();
            }
        }
    }
}

async fn tracker_task(
    writer: PostgresChainWriter,
    mut source: RpcReconciliationSource,
    options: LiveCommandOptions,
    max_reorg_depth: u64,
    mut reconnect: ReconnectLoop,
    health: HealthState,
    context: StatusContext,
    mut wakeup_rx: mpsc::Receiver<()>,
    shutdown: CancellationToken,
) -> Result<(), LiveTaskError> {
    reconcile_and_publish(
        &writer,
        &mut source,
        &options,
        max_reorg_depth,
        &mut reconnect,
        &health,
        &context,
        &wakeup_rx,
        &shutdown,
        None,
    )
    .await
    .map_err(live_runtime_error_to_task)?;

    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                reconnect.record_disconnect();
                publish_status(
                    &writer,
                    &source,
                    &reconnect,
                    &health,
                    &context,
                    &wakeup_rx,
                    0,
                )
                .await
                .map_err(live_runtime_error_to_task)?;
                return Ok(());
            }
            maybe_wakeup = wakeup_rx.recv() => {
                let Some(()) = maybe_wakeup else {
                    return Ok(());
                };
                reconcile_and_publish(
                    &writer,
                    &mut source,
                    &options,
                    max_reorg_depth,
                    &mut reconnect,
                    &health,
                    &context,
                    &wakeup_rx,
                    &shutdown,
                    None,
                )
                .await
                .map_err(live_runtime_error_to_task)?;
            }
        }
    }
}

fn live_runtime_error_to_task(error: LiveRuntimeError) -> LiveTaskError {
    match error {
        LiveRuntimeError::Task(error) => error,
        LiveRuntimeError::RpcEndpoint { source, .. } | LiveRuntimeError::Rpc(source) => {
            LiveTaskError::RpcTransient(source.to_string())
        }
        LiveRuntimeError::Sink(error) => LiveTaskError::DatabaseTransient(error.to_string()),
        LiveRuntimeError::Backfill(error) => LiveTaskError::DatabaseTransient(error.to_string()),
        error => LiveTaskError::TaskFailed(LiveTask::Tracker, error.to_string()),
    }
}

fn live_tracker_error_to_runtime(error: LiveError) -> LiveRuntimeError {
    match error {
        LiveError::Cancelled => {
            LiveRuntimeError::Task(LiveTaskError::TaskCancelled(LiveTask::Tracker))
        }
        LiveError::Chain(error) => LiveRuntimeError::Task(LiveTaskError::Chain(error)),
        LiveError::Reconcile(error) | LiveError::Source(error) => {
            LiveRuntimeError::Task(LiveTaskError::RpcTransient(error))
        }
        LiveError::Sink(error) => LiveRuntimeError::Task(LiveTaskError::DatabaseTransient(error)),
        LiveError::Backfill(error) => {
            LiveRuntimeError::Task(LiveTaskError::DatabaseTransient(error.to_string()))
        }
    }
}

fn source_error_requests_rereconcile(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    let hash_anchored_block_not_found = lower.contains("eth_getlogs")
        && (lower.contains("blockhash") || lower.contains("block hash"))
        && lower.contains("block not found");
    hash_anchored_block_not_found || lower.contains("rpc log block hash does not match")
}

async fn reconcile_and_publish(
    writer: &PostgresChainWriter,
    source: &mut RpcReconciliationSource,
    options: &LiveCommandOptions,
    max_reorg_depth: u64,
    reconnect: &mut ReconnectLoop,
    health: &HealthState,
    context: &StatusContext,
    wakeup_rx: &mpsc::Receiver<()>,
    shutdown: &CancellationToken,
    unreconciled_gap_count: Option<u64>,
) -> Result<(), LiveRuntimeError> {
    reconnect.record_subscribed_for_wakeups();
    let report = match reconcile_once(writer, source, options, max_reorg_depth, shutdown).await {
        Ok(report) => report,
        Err(error) => {
            if live_runtime_error_is_rpc_degradation(&error) {
                let _ = reconnect.record_availability_failure();
                health.mark_degraded("rpc").await;
            }
            return Err(error);
        }
    };
    if let Some(reason) = report.halted {
        let reason = correctness_stop_reason(reason);
        let _ = reconnect.record_correctness_error();
        health.mark_correctness_stop(reason).await;
        return Err(LiveRuntimeError::Task(LiveTaskError::TaskFailed(
            LiveTask::Tracker,
            format!("correctness-stop: {reason}"),
        )));
    }
    reconnect.record_reconciled_to_head()?;
    reconnect.record_ready_after_reconnect_at(Instant::now())?;
    let gap_count = unreconciled_gap_count.unwrap_or_else(|| {
        report
            .final_checkpoint
            .and_then(|checkpoint| {
                source
                    .last_head()
                    .map(|head| head.height.saturating_sub(checkpoint.height))
            })
            .unwrap_or(0)
    });
    publish_status(
        writer, source, reconnect, health, context, wakeup_rx, gap_count,
    )
    .await
}

fn correctness_stop_reason(reason: chainweave_core::LiveHaltReason) -> &'static str {
    match reason {
        chainweave_core::LiveHaltReason::MaxReorgDepth => "unresolved_ancestry",
        chainweave_core::LiveHaltReason::FinalizedBoundary => "finalized_boundary_violation",
        chainweave_core::LiveHaltReason::MissingExplicitStart
        | chainweave_core::LiveHaltReason::Correctness => "correctness_stop",
    }
}

fn live_runtime_error_is_rpc_degradation(error: &LiveRuntimeError) -> bool {
    matches!(
        error,
        LiveRuntimeError::RpcEndpoint { .. }
            | LiveRuntimeError::Rpc(_)
            | LiveRuntimeError::Task(LiveTaskError::RpcTransient(_))
            | LiveRuntimeError::Task(LiveTaskError::WebSocketTransient(_))
    )
}

async fn reconcile_once(
    writer: &PostgresChainWriter,
    source: &mut RpcReconciliationSource,
    options: &LiveCommandOptions,
    max_reorg_depth: u64,
    shutdown: &CancellationToken,
) -> Result<LiveReport, LiveRuntimeError> {
    let checkpoint = PostgresLiveStore::new(writer.clone())
        .checkpoint_header()
        .await?;
    let live_config = LiveConfig {
        max_reorg_depth,
        live_budget_cost_units_per_minute: options.budget_cost_units_per_window,
        poll_interval: options.poll_interval,
        explicit_start: options.start_block,
        finalized_height: None,
        budget_window: options.budget_window,
        retry_policy: RetryPolicy::new(
            8,
            Duration::from_millis(500),
            Duration::from_secs(30),
            Duration::from_millis(500),
        )?,
    };
    let tracker = LiveTracker::new(live_config);
    let store = PostgresLiveStore::new(writer.clone());
    let source_owned = source.clone();
    let handle = tokio::runtime::Handle::current();
    let shutdown_for_blocking = shutdown.clone();
    let joined = tokio::task::spawn_blocking(move || {
        let mut tracker_source =
            BlockingTrackerSource::new(source_owned, handle.clone(), shutdown_for_blocking.clone());
        let mut tracker_sink =
            BlockingPostgresLiveSink::new(store, handle, shutdown_for_blocking.clone(), checkpoint);
        let result = tracker
            .run_with_cancellation(
                &mut tracker_source,
                &mut tracker_sink,
                std::iter::empty(),
                || shutdown_for_blocking.is_cancelled(),
            )
            .map_err(live_tracker_error_to_runtime);
        (result, tracker_source.into_inner())
    })
    .await
    .map_err(|error| {
        if error.is_panic() {
            LiveRuntimeError::Task(LiveTaskError::TaskPanicked(LiveTask::Tracker))
        } else {
            LiveRuntimeError::Task(LiveTaskError::TaskCancelled(LiveTask::Tracker))
        }
    })?;
    let (result, source_after) = joined;
    *source = source_after;
    result
}

async fn publish_status(
    writer: &PostgresChainWriter,
    source: &RpcReconciliationSource,
    reconnect: &ReconnectLoop,
    health: &HealthState,
    context: &StatusContext,
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
    let (ready, readiness) =
        readiness_from_reconnect(reconnect, verifier_disagreements, unreconciled_gap_count);
    health
        .update_live_status(LiveStatusSnapshot {
            healthy: !reconnect.is_halted(),
            ready,
            readiness: readiness.to_owned(),
            primary_rpc_url: Some(context.primary_rpc_url.to_owned()),
            verifier_rpc_url: context.verifier_rpc_url.clone(),
            current_lag_blocks,
            reconnect_count: reconnect.attempts(),
            wakeup_depth: wakeup_rx.len(),
            unreconciled_gap_count,
            verifier_disagreement_count: verifier_disagreements,
        })
        .await;
    Ok(())
}

fn live_endpoint_labels(config: &AppConfig) -> (String, Option<String>) {
    (
        redact_url(&config.rpc.primary_url),
        config.rpc.verifier_url.as_ref().map(redact_url),
    )
}

fn verify_verifier_chain_identity(
    primary_chain_id: u64,
    primary_genesis_hash: String,
    verifier_chain_id: u64,
    verifier_genesis_hash: String,
) -> Result<(), RpcError> {
    if verifier_chain_id != primary_chain_id {
        return Err(RpcError::ChainIdMismatch {
            expected: primary_chain_id,
            actual: verifier_chain_id,
        });
    }
    if verifier_genesis_hash != primary_genesis_hash {
        return Err(RpcError::InvalidGenesis(format!(
            "verifier genesis hash {verifier_genesis_hash} does not match primary {primary_genesis_hash}"
        )));
    }
    Ok(())
}

fn readiness_from_reconnect(
    reconnect: &ReconnectLoop,
    verifier_disagreements: u64,
    unreconciled_gap_count: u64,
) -> (bool, &'static str) {
    let reconnect_ready = reconnect.readiness() == RuntimeReadiness::Ready;
    let ready = reconnect_ready && verifier_disagreements == 0 && unreconciled_gap_count == 0;
    let readiness = if ready {
        "ready"
    } else if reconnect_ready {
        "degraded"
    } else {
        "unavailable"
    };
    (ready, readiness)
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
        wakeup_depth: 0,
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
    pub fn record_task_error(&mut self, error: &LiveTaskError) -> ReconnectAction {
        match error.classify() {
            LiveTaskErrorClass::Transient => self.record_availability_failure(),
            LiveTaskErrorClass::Correctness => self.record_correctness_error(),
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

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LiveTaskError {
    #[error("live runtime correctness error: {0}")]
    Chain(#[from] ChainError),
    #[error("live runtime transient RPC error: {0}")]
    RpcTransient(String),
    #[error("live runtime transient WebSocket error: {0}")]
    WebSocketTransient(String),
    #[error("live runtime transient database error: {0}")]
    DatabaseTransient(String),
    #[error("live runtime task {0:?} panicked")]
    TaskPanicked(LiveTask),
    #[error("live runtime task {0:?} was cancelled")]
    TaskCancelled(LiveTask),
    #[error("live runtime task {0:?} exited before shutdown")]
    TaskExitedEarly(LiveTask),
    #[error("live runtime task {0:?} failed: {1}")]
    TaskFailed(LiveTask, String),
    #[error("live runtime supervisor task was cancelled")]
    SupervisorCancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveTaskErrorClass {
    Transient,
    Correctness,
}

impl LiveTaskError {
    #[must_use]
    pub const fn classify(&self) -> LiveTaskErrorClass {
        match self {
            Self::RpcTransient(_) | Self::WebSocketTransient(_) | Self::DatabaseTransient(_) => {
                LiveTaskErrorClass::Transient
            }
            Self::Chain(_)
            | Self::TaskPanicked(_)
            | Self::TaskCancelled(_)
            | Self::TaskExitedEarly(_)
            | Self::TaskFailed(_, _)
            | Self::SupervisorCancelled => LiveTaskErrorClass::Correctness,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveTask {
    HealthServer,
    Wakeups,
    Poll,
    Tracker,
}

pub struct LiveTaskSupervisor {
    shutdown: CancellationToken,
    tasks: JoinSet<(LiveTask, Result<(), LiveTaskError>)>,
}

impl LiveTaskSupervisor {
    #[must_use]
    pub fn new(shutdown: CancellationToken) -> Self {
        Self {
            shutdown,
            tasks: JoinSet::new(),
        }
    }

    pub fn spawn<Fut>(&mut self, task: LiveTask, future: Fut)
    where
        Fut: Future<Output = Result<(), LiveTaskError>> + Send + 'static,
    {
        self.tasks.spawn(async move {
            let result = AssertUnwindSafe(future).catch_unwind().await;
            let result = match result {
                Ok(result) => result,
                Err(_) => Err(LiveTaskError::TaskPanicked(task)),
            };
            (task, result)
        });
    }

    /// Waits for the first task to finish, then cancels and drains the rest of the runner.
    /// A clean task exit before shutdown is treated as a failure because live tasks
    /// should run until cancellation or an explicit error.
    ///
    /// # Errors
    ///
    /// Returns the first task error, panic, premature exit, or supervisor cancellation.
    pub async fn run_until_first_exit(mut self) -> Result<(), LiveTaskError> {
        let Some(joined) = self.tasks.join_next().await else {
            return Ok(());
        };

        let first_result = match joined {
            Ok((_task, Ok(()))) if self.shutdown.is_cancelled() => Ok(()),
            Ok((task, Ok(()))) => Err(LiveTaskError::TaskExitedEarly(task)),
            Ok((_task, Err(error))) => Err(error),
            Err(error) if error.is_cancelled() => Err(LiveTaskError::SupervisorCancelled),
            Err(error) if error.is_panic() => Err(LiveTaskError::TaskPanicked(LiveTask::Tracker)),
            Err(_) => Err(LiveTaskError::SupervisorCancelled),
        };

        self.shutdown.cancel();
        while self.tasks.join_next().await.is_some() {}
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
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::Notify,
    };

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
    fn live_endpoint_labels_redact_log_inputs() {
        let config = AppConfig {
            rpc: chainweave_core::config::RpcConfig {
                primary_url: Url::parse("wss://rpc.example/v3/path-secret").unwrap(),
                verifier_url: Some(
                    Url::parse("wss://verify.example/mainnet?api_key=query-secret").unwrap(),
                ),
            },
            ..AppConfig::default()
        };

        let (primary, verifier) = live_endpoint_labels(&config);

        assert!(!primary.contains("path-secret"));
        assert!(!verifier.unwrap().contains("query-secret"));
        assert!(primary.contains("redacted"));
    }

    #[test]
    fn hash_anchored_log_block_not_found_requests_rereconcile() {
        assert!(source_error_requests_rereconcile(
            "eth_getLogs blockHash 0xabc failed: block not found"
        ));
        assert!(!source_error_requests_rereconcile(
            "eth_getBlockByNumber failed: block not found"
        ));
    }

    #[test]
    fn readiness_comes_from_reconnect_loop_state() {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::from_secs(30),
        )
        .unwrap();
        let mut reconnect = ReconnectLoop::new(policy);

        assert_eq!(
            readiness_from_reconnect(&reconnect, 0, 0),
            (false, "unavailable")
        );
        reconnect.record_subscribed_for_wakeups();
        reconnect.record_reconciled_to_head().unwrap();
        reconnect
            .record_ready_after_reconnect_at(Instant::now())
            .unwrap();
        assert_eq!(readiness_from_reconnect(&reconnect, 0, 0), (true, "ready"));
        assert_eq!(
            readiness_from_reconnect(&reconnect, 1, 0),
            (false, "degraded")
        );
    }

    #[test]
    fn correctness_stop_reasons_cover_required_health_cases() {
        assert_eq!(
            correctness_stop_reason(chainweave_core::LiveHaltReason::MaxReorgDepth),
            "unresolved_ancestry"
        );
        assert_eq!(
            correctness_stop_reason(chainweave_core::LiveHaltReason::FinalizedBoundary),
            "finalized_boundary_violation"
        );
    }

    #[test]
    fn verifier_chain_identity_mismatch_is_rejected() {
        let error = verify_verifier_chain_identity(
            1,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            2,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            RpcError::ChainIdMismatch {
                expected: 1,
                actual: 2
            }
        ));
    }

    #[tokio::test]
    async fn connection_errors_health_and_metrics_do_not_expose_url_secrets() {
        for url in [
            Url::parse("ws://127.0.0.1:9/v3/path-secret").unwrap(),
            Url::parse("ws://127.0.0.1:9/mainnet?api_key=query-secret").unwrap(),
        ] {
            let error = tokio::time::timeout(Duration::from_secs(2), connect_rpc(&url))
                .await
                .expect("connection attempt should finish")
                .unwrap_err()
                .to_string();
            assert!(!error.contains("path-secret"));
            assert!(!error.contains("query-secret"));
            assert!(error.contains("redacted"));
        }

        let health = HealthState::default();
        health
            .update_live_status(initial_status(
                "wss://rpc.example/v3/redacted",
                Some("wss://verify.example/mainnet?api_key=redacted"),
            ))
            .await;
        let server =
            ObservabilityServer::bind_with_local_recorder("127.0.0.1:0".parse().unwrap(), health)
                .await
                .unwrap();
        let address = server.local_addr().unwrap();
        let server_task = tokio::spawn(server.serve());

        let health_body = http_get(address, "/health").await;
        let metrics_body = http_get(address, "/metrics").await;
        server_task.abort();

        for body in [health_body, metrics_body] {
            assert!(!body.contains("path-secret"));
            assert!(!body.contains("query-secret"));
            assert!(!body.contains("api_key=query-secret"));
        }
    }

    async fn http_get(address: std::net::SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }

    #[tokio::test]
    async fn health_and_wakeup_tasks_stay_responsive_while_tracker_blocks_on_slow_fake_rpc() {
        let shutdown = CancellationToken::new();
        let started = Arc::new(Notify::new());
        let tracker =
            spawn_slow_fake_rpc_on_blocking_tracker_thread(shutdown.clone(), Arc::clone(&started));
        started.notified().await;

        let health = HealthState::default();
        health
            .update_live_status(initial_status("http://127.0.0.1:8545", None))
            .await;
        let server =
            ObservabilityServer::bind_with_local_recorder("127.0.0.1:0".parse().unwrap(), health)
                .await
                .unwrap();
        let address = server.local_addr().unwrap();
        let server_task = tokio::spawn(server.serve());

        let (wakeups, mut wakeup_rx) = new_head_wakeup_channel();
        let wakeup_shutdown = shutdown.clone();
        let wakeup_task = tokio::spawn(poll_wakeup_task(
            Duration::from_millis(5),
            wakeups,
            wakeup_shutdown,
        ));

        let health_body =
            tokio::time::timeout(Duration::from_millis(100), http_get(address, "/health"))
                .await
                .unwrap();
        let wakeup = tokio::time::timeout(Duration::from_millis(100), wakeup_rx.recv())
            .await
            .unwrap();

        assert!(health_body.contains("\"status\""));
        assert_eq!(wakeup, Some(()));
        assert!(!tracker.is_finished());

        shutdown.cancel();
        server_task.abort();
        let _ = wakeup_task.await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(100), tracker)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            result,
            Err(LiveRuntimeError::Task(LiveTaskError::TaskCancelled(
                LiveTask::Tracker
            )))
        ));
    }

    #[tokio::test]
    async fn shutdown_during_slow_fake_rpc_completes_within_shutdown_timeout() {
        let shutdown = CancellationToken::new();
        let started = Arc::new(Notify::new());
        let tracker =
            spawn_slow_fake_rpc_on_blocking_tracker_thread(shutdown.clone(), Arc::clone(&started));
        started.notified().await;

        shutdown.cancel();
        let result = tokio::time::timeout(Duration::from_millis(100), tracker)
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(
            result,
            Err(LiveRuntimeError::Task(LiveTaskError::TaskCancelled(
                LiveTask::Tracker
            )))
        ));
    }

    fn spawn_slow_fake_rpc_on_blocking_tracker_thread(
        shutdown: CancellationToken,
        started: Arc<Notify>,
    ) -> tokio::task::JoinHandle<Result<(), LiveRuntimeError>> {
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            block_on_runtime_with_shutdown(&handle, shutdown, async move {
                started.notify_one();
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(())
            })
        })
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
    fn max_depth_task_error_halts_reconnect_without_retry() {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::from_secs(30),
        )
        .unwrap();
        let mut reconnect = ReconnectLoop::new(policy);
        let error = LiveTaskError::Chain(ChainError::MaxDepthExceeded { max_depth: 3 });

        assert_eq!(error.classify(), LiveTaskErrorClass::Correctness);
        assert_eq!(reconnect.record_task_error(&error), ReconnectAction::Halt);
        assert!(reconnect.is_halted());
        assert_eq!(reconnect.attempts(), 0);
    }

    #[tokio::test]
    async fn task_supervisor_treats_clean_early_exit_as_failure_and_cancels_rest() {
        let shutdown = CancellationToken::new();
        let sibling_cancelled = Arc::new(AtomicUsize::new(0));
        let sibling_cancelled_for_task = Arc::clone(&sibling_cancelled);
        let sibling_shutdown = shutdown.clone();
        let mut supervisor = LiveTaskSupervisor::new(shutdown);

        supervisor.spawn(LiveTask::Poll, async { Ok(()) });
        supervisor.spawn(LiveTask::Tracker, async move {
            sibling_shutdown.cancelled().await;
            sibling_cancelled_for_task.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        assert_eq!(
            supervisor.run_until_first_exit().await,
            Err(LiveTaskError::TaskExitedEarly(LiveTask::Poll))
        );
        assert_eq!(sibling_cancelled.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn live_supervisor_does_not_use_shutdown_timeout_as_runtime_deadline() {
        let shutdown = CancellationToken::new();
        let mut supervisor = LiveTaskSupervisor::new(shutdown.clone());
        let poll_shutdown = shutdown.clone();
        let tracker_shutdown = shutdown.clone();

        supervisor.spawn(LiveTask::Poll, async move {
            poll_shutdown.cancelled().await;
            Ok(())
        });
        supervisor.spawn(LiveTask::Tracker, async move {
            tracker_shutdown.cancelled().await;
            Ok(())
        });

        let result = tokio::time::timeout(
            Duration::from_millis(25),
            run_live_supervisor(supervisor, Duration::from_millis(1)),
        )
        .await;

        assert!(result.is_err());
        shutdown.cancel();
    }
}
