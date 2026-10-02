use std::{
    future::Future,
    time::{Duration, Instant},
};

use chainweave_core::{
    BackfillRange, BlockHash, BlockHeader, FetchedRange, OrderedCommitCoordinator, RetryPolicy,
    RpcBudget, RpcMethod,
};
use chainweave_rpc::{
    ContractLogFilter, RpcClient, RpcError, capture_target_head_with_retry,
    fetch_header_by_hash_with_retry, fetch_header_by_number_with_retry, retry_rpc_request,
};
use chainweave_sink::{
    IndexedBlock, PostgresBackfillCommitter, PostgresChainWriter, PostgresStateError,
};
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum LiveRuntimeError {
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error(transparent)]
    Sink(#[from] PostgresStateError),
    #[error(transparent)]
    Backfill(#[from] chainweave_core::BackfillError),
    #[error("checkpoint height {0} has no canonical header")]
    MissingCheckpointHeader(u64),
    #[error("invalid live RPC budget: {0}")]
    Budget(String),
    #[error(transparent)]
    Pipeline(#[from] LivePipelineError),
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

#[derive(Debug, Clone)]
pub struct PostgresLiveStore {
    writer: PostgresChainWriter,
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
    pub jitter: Duration,
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
        jitter: Duration,
    ) -> Result<Self, LiveRuntimeError> {
        if base_delay.is_zero() || max_delay.is_zero() {
            return Err(LiveRuntimeError::Budget(
                "reconnect delays must be nonzero".to_owned(),
            ));
        }
        if base_delay > max_delay {
            return Err(LiveRuntimeError::Budget(
                "reconnect base delay must not exceed max delay".to_owned(),
            ));
        }
        Ok(Self {
            base_delay,
            max_delay,
            jitter,
        })
    }

    #[must_use]
    pub fn delay_for_attempt(self, attempt: u64) -> Duration {
        let exponent = attempt.saturating_sub(1).min(31);
        let multiplier = 1_u64.checked_shl(exponent as u32).unwrap_or(u64::MAX);
        let exponential = saturating_duration_mul(self.base_delay, multiplier).min(self.max_delay);
        exponential
            .saturating_add(self.jitter_for_attempt(attempt))
            .min(self.max_delay)
    }

    fn jitter_for_attempt(self, attempt: u64) -> Duration {
        let jitter_nanos = self.jitter.as_nanos().min(u128::from(u64::MAX)) as u64;
        if jitter_nanos == 0 {
            return Duration::ZERO;
        }
        let mixed = attempt
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        Duration::from_nanos(mixed % (jitter_nanos + 1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectLoop {
    policy: ReconnectPolicy,
    attempt: u64,
    readiness: RuntimeReadiness,
    halted: bool,
}

impl ReconnectLoop {
    #[must_use]
    pub const fn new(policy: ReconnectPolicy) -> Self {
        Self {
            policy,
            attempt: 0,
            readiness: RuntimeReadiness::Ready,
            halted: false,
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
        self.readiness = RuntimeReadiness::Unavailable;
    }

    pub fn record_connected_needs_reconciliation(&mut self) {
        self.readiness = RuntimeReadiness::Unavailable;
    }

    pub fn record_catch_up_complete(&mut self) {
        if !self.halted {
            self.attempt = 0;
            self.readiness = RuntimeReadiness::Ready;
        }
    }

    #[must_use]
    pub fn record_availability_failure(&mut self) -> ReconnectAction {
        if self.halted {
            return ReconnectAction::Halt;
        }
        self.readiness = RuntimeReadiness::Unavailable;
        self.attempt = self.attempt.saturating_add(1);
        ReconnectAction::RetryAfter(self.policy.delay_for_attempt(self.attempt))
    }

    #[must_use]
    pub fn record_correctness_error(&mut self) -> ReconnectAction {
        self.readiness = RuntimeReadiness::Unavailable;
        self.halted = true;
        ReconnectAction::Halt
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
    #[error("live pipeline channel for {0:?} closed")]
    ChannelClosed(LivePipelineStage),
    #[error("live pipeline send from {stage:?} timed out after {timeout:?}")]
    SendTimedOut {
        stage: LivePipelineStage,
        timeout: Duration,
    },
    #[error("live pipeline shutdown while {0:?} was waiting")]
    Shutdown(LivePipelineStage),
    #[error("live pipeline stage {0:?} panicked")]
    StagePanicked(LivePipelineStage),
    #[error("live pipeline stage {0:?} was cancelled")]
    StageCancelled(LivePipelineStage),
    #[error("live pipeline queue capacity must be nonzero")]
    InvalidQueueCapacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivePipelineConfig {
    pub fetch_queue: usize,
    pub coordinate_queue: usize,
    pub raw_queue: usize,
    pub write_queue: usize,
    pub send_timeout: Duration,
}

impl LivePipelineConfig {
    #[must_use]
    pub const fn new(
        fetch_queue: usize,
        coordinate_queue: usize,
        raw_queue: usize,
        write_queue: usize,
        send_timeout: Duration,
    ) -> Self {
        Self {
            fetch_queue,
            coordinate_queue,
            raw_queue,
            write_queue,
            send_timeout,
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

/// Sends one item to the next bounded stage, returning on shutdown or timeout instead of waiting
/// forever behind a full queue.
///
/// # Errors
///
/// Returns a pipeline error when the queue closes, the send times out, or shutdown is requested.
pub async fn send_with_shutdown<T>(
    stage: LivePipelineStage,
    sender: &mpsc::Sender<T>,
    item: T,
    timeout: Duration,
    shutdown: &CancellationToken,
) -> Result<(), LivePipelineError>
where
    T: Send + 'static,
{
    tokio::select! {
        () = shutdown.cancelled() => Err(LivePipelineError::Shutdown(stage)),
        result = tokio::time::timeout(timeout, sender.send(item)) => match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(LivePipelineError::ChannelClosed(stage)),
            Err(_) => Err(LivePipelineError::SendTimedOut { stage, timeout }),
        },
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
    send_timeout: Duration,
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
                    send_timeout,
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

pub struct SupervisedStage {
    stage: LivePipelineStage,
    handle: JoinHandle<Result<(), LivePipelineError>>,
}

/// Spawns one supervised pipeline stage.
pub fn spawn_supervised_stage<Fut>(stage: LivePipelineStage, future: Fut) -> SupervisedStage
where
    Fut: Future<Output = Result<(), LivePipelineError>> + Send + 'static,
{
    SupervisedStage {
        stage,
        handle: tokio::spawn(future),
    }
}

/// Waits for a supervised stage and converts task panics into correctness-visible pipeline errors.
///
/// # Errors
///
/// Returns the stage error, a panic marker, or a cancellation marker.
pub async fn supervise_stage(stage: SupervisedStage) -> Result<(), LivePipelineError> {
    match stage.handle.await {
        Ok(result) => result,
        Err(error) if error.is_panic() => Err(LivePipelineError::StagePanicked(stage.stage)),
        Err(_) => Err(LivePipelineError::StageCancelled(stage.stage)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
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
    fn reconnect_backoff_is_unbounded_capped_and_jittered() {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(10),
            Duration::from_millis(500),
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
        for _ in 0..128 {
            let ReconnectAction::RetryAfter(delay) = reconnect.record_availability_failure() else {
                panic!("availability failures keep retrying");
            };
            assert!(delay <= Duration::from_secs(10));
            last = delay;
        }
        assert_eq!(last, Duration::from_secs(10));
        assert_eq!(reconnect.attempts(), 130);
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Unavailable);
    }

    #[test]
    fn reconnect_readiness_stays_unavailable_until_catch_up_completes() {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::ZERO,
        )
        .unwrap();
        let mut reconnect = ReconnectLoop::new(policy);

        reconnect.record_disconnect();
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Unavailable);
        reconnect.record_connected_needs_reconciliation();
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Unavailable);
        reconnect.record_catch_up_complete();
        assert_eq!(reconnect.readiness(), RuntimeReadiness::Ready);
        assert_eq!(reconnect.attempts(), 0);
    }

    #[test]
    fn correctness_error_halts_reconnect_loop() {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::ZERO,
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
    fn bounded_pipeline_queues_use_configured_capacities() {
        let queues: LivePipelineQueues<u64, u64, u64, u64> = bounded_live_pipeline_queues(
            LivePipelineConfig::new(1, 2, 3, 4, Duration::from_millis(10)),
        )
        .unwrap();

        assert_eq!(queues.fetch_tx.max_capacity(), 1);
        assert_eq!(queues.coordinate_tx.max_capacity(), 2);
        assert_eq!(queues.raw_tx.max_capacity(), 3);
        assert_eq!(queues.write_tx.max_capacity(), 4);
    }

    #[test]
    fn bounded_pipeline_rejects_zero_capacity() {
        let result = bounded_live_pipeline_queues::<u64, u64, u64, u64>(LivePipelineConfig::new(
            1,
            0,
            1,
            1,
            Duration::from_millis(10),
        ));

        assert!(matches!(
            result,
            Err(LivePipelineError::InvalidQueueCapacity)
        ));
    }

    #[tokio::test]
    async fn full_queue_send_times_out_with_stage_name() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(10_u64).await.unwrap();
        let shutdown = CancellationToken::new();

        let result = send_with_shutdown(
            LivePipelineStage::Fetch,
            &sender,
            11_u64,
            Duration::from_millis(1),
            &shutdown,
        )
        .await;

        assert_eq!(
            result,
            Err(LivePipelineError::SendTimedOut {
                stage: LivePipelineStage::Fetch,
                timeout: Duration::from_millis(1)
            })
        );
        assert_eq!(receiver.recv().await, Some(10));
    }

    #[tokio::test]
    async fn shutdown_cancels_full_channel_send_without_hanging() {
        let (sender, _receiver) = mpsc::channel(1);
        sender.send(10_u64).await.unwrap();
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let result = tokio::time::timeout(
            Duration::from_millis(50),
            send_with_shutdown(
                LivePipelineStage::Coordinate,
                &sender,
                11_u64,
                Duration::from_secs(60),
                &shutdown,
            ),
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

        raw_pass_through_stage(raw_rx, write_tx, Duration::from_millis(50), shutdown)
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
    async fn supervised_stage_reports_panic_with_stage_name() {
        let stage = spawn_supervised_stage(LivePipelineStage::Write, async {
            panic!("writer stage panic is supervised");
        });

        assert_eq!(
            supervise_stage(stage).await,
            Err(LivePipelineError::StagePanicked(LivePipelineStage::Write))
        );
    }
}
