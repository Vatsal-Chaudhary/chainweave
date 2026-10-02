use std::time::{Duration, Instant};

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
