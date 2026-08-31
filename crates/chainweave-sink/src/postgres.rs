use std::{
    collections::{HashMap, HashSet},
    fmt::Write as _,
    future::Future,
    time::Duration,
};

use chainweave_core::{BlockHash, BlockHeader, ChainBatch, ChainEvent, ChainTransition};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::{sync::mpsc, time::timeout};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockStatus {
    Unsafe,
    Safe,
    Finalized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusSource {
    Observed,
    Native,
    Depth,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawLog {
    pub transaction_index: u32,
    pub log_index: u32,
    pub tx_hash: BlockHash,
    pub address: [u8; 20],
    pub topics: Vec<BlockHash>,
    pub data: Vec<u8>,
    pub decoded_event: Option<Value>,
    pub decoder_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedBlock {
    pub header: BlockHeader,
    pub timestamp: OffsetDateTime,
    pub status: BlockStatus,
    pub status_source: StatusSource,
    pub logs: Vec<RawLog>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableChainEvent {
    Apply(IndexedBlock),
    Rollback(BlockHeader),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableChainBatch {
    pub transition: ChainTransition,
    pub common_ancestor: Option<BlockHeader>,
    pub events: Vec<DurableChainEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub last_height: u64,
    pub last_hash: BlockHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEvent {
    pub event_id: i64,
    pub event_kind: String,
    pub block_hash: BlockHash,
    pub block_height: u64,
}

#[derive(Debug, Clone)]
pub struct PostgresChainWriter {
    pool: PgPool,
    chain_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    BeforeCommit,
    AfterCommit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ApplyReport {
    pub applied_blocks: usize,
    pub rolled_back_blocks: usize,
    pub upserted_logs: usize,
    pub appended_outbox_events: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReconciliationSummary {
    pub rolled_back_blocks: usize,
    pub applied_blocks: usize,
    pub final_checkpoint: Option<Checkpoint>,
}

#[derive(Debug, Error)]
pub enum PostgresStateError {
    #[error("chain ID must be nonzero")]
    EmptyChainId,
    #[error("chain ID {0} cannot be represented as a Postgres numeric")]
    InvalidChainId(String),
    #[error("invalid 32-byte hash length: {0}")]
    InvalidHashLength(usize),
    #[error("invalid 20-byte address length: {0}")]
    InvalidAddressLength(usize),
    #[error("block height {0} exceeds Postgres BIGINT")]
    HeightOverflow(u64),
    #[error("log index value {0} exceeds Postgres INT")]
    IntOverflow(u32),
    #[error("apply block for hash {0} is missing from durable batch input")]
    MissingApplyBlock(String),
    #[error("durable batch contains apply block {0} that was not requested by ChainBatch")]
    UnexpectedApplyBlock(String),
    #[error("rollback block {0} is not persisted")]
    MissingRollbackBlock(String),
    #[error("checkpoint target {0} at height {1} is not canonical")]
    NonCanonicalCheckpoint(String, u64),
    #[error("chain identity mismatch for chain ID {chain_id}")]
    ChainIdentityMismatch { chain_id: String },
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Error)]
pub enum ReconciliationError {
    #[error("source did not return a block at height {0}")]
    MissingSourceHeight(u64),
    #[error("persisted checkpoint has no canonical block at height {0}")]
    MissingPersistedCheckpointBlock(u64),
    #[error("startup reconciliation exceeded max walk-back depth {0}")]
    MaxDepthExceeded(u64),
    #[error("source block {child_height} does not link to previous hash")]
    BrokenSourceLink { child_height: u64 },
    #[error("postgres state error: {0}")]
    State(#[from] PostgresStateError),
    #[error("source error: {0}")]
    Source(String),
}

pub trait ReconciliationSource {
    fn head(&mut self) -> impl Future<Output = Result<IndexedBlock, ReconciliationError>> + Send;

    fn block_by_height(
        &mut self,
        height: u64,
    ) -> impl Future<Output = Result<Option<IndexedBlock>, ReconciliationError>> + Send;
}

#[derive(Debug)]
pub struct SerializedWriter {
    sender: Option<mpsc::Sender<DurableChainBatch>>,
    cancel: CancellationToken,
    tracker: TaskTracker,
}

struct RawLogRow {
    transaction_index: i32,
    log_index: i32,
    tx_hash: Vec<u8>,
    address: Vec<u8>,
    topics: Vec<Vec<u8>>,
    data: Vec<u8>,
    decoded_event: Option<Value>,
    decoder_version: Option<String>,
}

#[derive(Debug, Error)]
pub enum WriterQueueError {
    #[error("writer is shutting down")]
    ShuttingDown,
    #[error("writer task is gone")]
    Closed,
}

#[derive(Debug, Error)]
pub enum WriterShutdownError {
    #[error("writer did not drain queued work within {0:?}")]
    Timeout(Duration),
}

impl BlockStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Unsafe => "unsafe",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
        }
    }
}

impl StatusSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Native => "native",
            Self::Depth => "depth",
        }
    }
}

impl DurableChainBatch {
    /// Converts a pure chain transition plus fetched block/log payloads into durable writer input.
    ///
    /// # Errors
    ///
    /// Returns an error when an apply event has no matching block payload or when an unused
    /// block payload is provided.
    pub fn from_chain_batch(
        batch: &ChainBatch,
        apply_blocks: impl IntoIterator<Item = IndexedBlock>,
    ) -> Result<Self, PostgresStateError> {
        let mut blocks = HashMap::new();
        for block in apply_blocks {
            blocks.insert(block.header.hash, block);
        }

        let mut requested = HashSet::new();
        let mut events = Vec::with_capacity(batch.events.len());
        for event in &batch.events {
            match event {
                ChainEvent::Rollback(header) => events.push(DurableChainEvent::Rollback(*header)),
                ChainEvent::Apply(header) => {
                    requested.insert(header.hash);
                    let block = blocks.remove(&header.hash).ok_or_else(|| {
                        PostgresStateError::MissingApplyBlock(hex_hash(&header.hash))
                    })?;
                    events.push(DurableChainEvent::Apply(block));
                }
            }
        }

        if let Some(extra) = blocks.keys().next() {
            return Err(PostgresStateError::UnexpectedApplyBlock(hex_hash(extra)));
        }

        Ok(Self {
            transition: batch.transition,
            common_ancestor: batch.common_ancestor,
            events,
        })
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl PostgresChainWriter {
    /// Opens a Postgres pool for one configured chain.
    ///
    /// # Errors
    ///
    /// Returns an error when the chain ID is invalid or Postgres rejects the connection.
    pub async fn connect(database_url: &str, chain_id: u64) -> Result<Self, PostgresStateError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        Self::new(pool, chain_id)
    }

    /// Creates a writer from an existing pool.
    ///
    /// # Errors
    ///
    /// Returns an error when the chain ID is invalid.
    pub fn new(pool: PgPool, chain_id: u64) -> Result<Self, PostgresStateError> {
        if chain_id == 0 {
            return Err(PostgresStateError::EmptyChainId);
        }
        Ok(Self {
            pool,
            chain_id: chain_id.to_string(),
        })
    }

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Runs checked-in schema migrations.
    ///
    /// # Errors
    ///
    /// Returns an error when a migration cannot be applied.
    pub async fn run_migrations(&self) -> Result<(), sqlx::migrate::MigrateError> {
        sqlx::migrate!("../../migrations").run(&self.pool).await
    }

    /// Records or verifies the durable chain identity.
    ///
    /// # Errors
    ///
    /// Returns an error if the existing database identity disagrees with the configured RPC
    /// genesis hash.
    pub async fn ensure_chain_identity(
        &self,
        genesis_hash: BlockHash,
    ) -> Result<(), PostgresStateError> {
        let result = sqlx::query!(
            r"
            INSERT INTO chain_identity (chain_id, genesis_hash)
            VALUES (($1::text)::numeric, $2)
            ON CONFLICT (chain_id) DO UPDATE
            SET genesis_hash = EXCLUDED.genesis_hash
            WHERE chain_identity.genesis_hash = EXCLUDED.genesis_hash
            ",
            &self.chain_id,
            hash_bytes(&genesis_hash),
        )
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(PostgresStateError::ChainIdentityMismatch {
                chain_id: self.chain_id.clone(),
            });
        }
        Ok(())
    }

    /// Applies a durable chain transition in one database transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if any state transition, raw-log upsert, outbox append, or checkpoint
    /// move cannot be committed atomically.
    pub async fn apply_batch(
        &self,
        batch: &DurableChainBatch,
    ) -> Result<ApplyReport, PostgresStateError> {
        self.apply_batch_inner(batch, None).await
    }

    /// Applies a batch and deliberately parks at a crash point for fault-injection tests.
    ///
    /// # Errors
    ///
    /// Returns database and state-transition errors from the same path as [`Self::apply_batch`].
    pub async fn apply_batch_with_crash_point(
        &self,
        batch: &DurableChainBatch,
        crash_point: CrashPoint,
    ) -> Result<ApplyReport, PostgresStateError> {
        self.apply_batch_inner(batch, Some(crash_point)).await
    }

    async fn apply_batch_inner(
        &self,
        batch: &DurableChainBatch,
        crash_point: Option<CrashPoint>,
    ) -> Result<ApplyReport, PostgresStateError> {
        if batch.is_empty() {
            return Ok(ApplyReport::default());
        }

        let mut tx = self.pool.begin().await?;
        let mut report = ApplyReport::default();
        let mut checkpoint_target = batch.common_ancestor;

        for event in &batch.events {
            match event {
                DurableChainEvent::Rollback(header) => {
                    if self.rollback_block(&mut tx, *header).await? {
                        self.append_outbox(&mut tx, "rollback", *header, None)
                            .await?;
                        report.rolled_back_blocks += 1;
                        report.appended_outbox_events += 1;
                    }
                    checkpoint_target = batch.common_ancestor;
                }
                DurableChainEvent::Apply(block) => {
                    let changed = self.upsert_block(&mut tx, block).await?;
                    let log_count = self.upsert_logs(&mut tx, block).await?;
                    report.upserted_logs += log_count;
                    if changed {
                        self.append_outbox(&mut tx, "apply", block.header, Some(block))
                            .await?;
                        report.applied_blocks += 1;
                        report.appended_outbox_events += 1;
                    }
                    checkpoint_target = Some(block.header);
                }
            }
        }

        if let Some(target) = checkpoint_target {
            self.move_checkpoint(&mut tx, target).await?;
        }

        if crash_point == Some(CrashPoint::BeforeCommit) {
            pending_crash().await;
        }

        tx.commit().await?;

        if crash_point == Some(CrashPoint::AfterCommit) {
            pending_crash().await;
        }

        Ok(report)
    }

    async fn rollback_block(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        header: BlockHeader,
    ) -> Result<bool, PostgresStateError> {
        let exists = sqlx::query!(
            r"
            SELECT TRUE AS present
            FROM blocks
            WHERE chain_id = ($1::text)::numeric AND block_hash = $2
            ",
            &self.chain_id,
            hash_bytes(&header.hash),
        )
        .fetch_optional(tx.as_mut())
        .await?;

        if exists.is_none() {
            return Err(PostgresStateError::MissingRollbackBlock(hex_hash(
                &header.hash,
            )));
        }

        let result = sqlx::query!(
            r"
            UPDATE blocks
            SET is_canonical = FALSE
            WHERE chain_id = ($1::text)::numeric
              AND block_hash = $2
              AND height = $3
              AND is_canonical
            ",
            &self.chain_id,
            hash_bytes(&header.hash),
            pg_height(header.height)?,
        )
        .execute(tx.as_mut())
        .await?;

        Ok(result.rows_affected() == 1)
    }

    async fn upsert_block(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        block: &IndexedBlock,
    ) -> Result<bool, PostgresStateError> {
        let row = sqlx::query!(
            r"
            INSERT INTO blocks (
                chain_id, block_hash, parent_hash, height, timestamp,
                is_canonical, status, status_source
            )
            VALUES (($1::text)::numeric, $2, $3, $4, $5, TRUE, $6, $7)
            ON CONFLICT (chain_id, block_hash) DO UPDATE
            SET is_canonical = TRUE,
                timestamp = EXCLUDED.timestamp,
                status = EXCLUDED.status,
                status_source = EXCLUDED.status_source
            WHERE blocks.is_canonical = FALSE
            RETURNING height
            ",
            &self.chain_id,
            hash_bytes(&block.header.hash),
            hash_bytes(&block.header.parent_hash),
            pg_height(block.header.height)?,
            block.timestamp,
            block.status.as_str(),
            block.status_source.as_str(),
        )
        .fetch_optional(tx.as_mut())
        .await?;

        Ok(row.is_some())
    }

    async fn upsert_logs(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        block: &IndexedBlock,
    ) -> Result<usize, PostgresStateError> {
        let mut count = 0;
        for log in &block.logs {
            let topics = log.topics.iter().map(hash_bytes).collect::<Vec<Vec<u8>>>();
            sqlx::query!(
                r"
                INSERT INTO logs (
                    chain_id, block_hash, block_number, transaction_index,
                    log_index, tx_hash, address, topics, data, decoded_event,
                    decoder_version
                )
                VALUES (($1::text)::numeric, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (chain_id, block_hash, log_index) DO UPDATE
                SET block_number = EXCLUDED.block_number,
                    transaction_index = EXCLUDED.transaction_index,
                    tx_hash = EXCLUDED.tx_hash,
                    address = EXCLUDED.address,
                    topics = EXCLUDED.topics,
                    data = EXCLUDED.data,
                    decoded_event = EXCLUDED.decoded_event,
                    decoder_version = EXCLUDED.decoder_version
                ",
                &self.chain_id,
                hash_bytes(&block.header.hash),
                pg_height(block.header.height)?,
                pg_int(log.transaction_index)?,
                pg_int(log.log_index)?,
                hash_bytes(&log.tx_hash),
                address_bytes(&log.address),
                &topics,
                &log.data,
                log.decoded_event.clone(),
                log.decoder_version.as_deref(),
            )
            .execute(tx.as_mut())
            .await?;
            count += 1;
        }
        Ok(count)
    }

    async fn append_outbox(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        event_kind: &str,
        header: BlockHeader,
        block: Option<&IndexedBlock>,
    ) -> Result<(), PostgresStateError> {
        let payload = json!({
            "schema_version": 1,
            "transition": event_kind,
            "chain_id": &self.chain_id,
            "block_hash": hex_hash(&header.hash),
            "parent_hash": hex_hash(&header.parent_hash),
            "block_height": header.height,
            "status": block.map(|value| value.status.as_str()),
            "status_source": block.map(|value| value.status_source.as_str()),
            "log_count": block.map_or(0, |value| value.logs.len()),
        });

        sqlx::query!(
            r"
            INSERT INTO outbox_events (
                chain_id, event_kind, block_hash, block_height, payload
            )
            VALUES (($1::text)::numeric, $2, $3, $4, $5)
            ",
            &self.chain_id,
            event_kind,
            hash_bytes(&header.hash),
            pg_height(header.height)?,
            payload,
        )
        .execute(tx.as_mut())
        .await?;
        Ok(())
    }

    async fn move_checkpoint(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        target: BlockHeader,
    ) -> Result<(), PostgresStateError> {
        let canonical = sqlx::query!(
            r"
            SELECT TRUE AS present
            FROM blocks
            WHERE chain_id = ($1::text)::numeric
              AND block_hash = $2
              AND height = $3
              AND is_canonical
            ",
            &self.chain_id,
            hash_bytes(&target.hash),
            pg_height(target.height)?,
        )
        .fetch_optional(tx.as_mut())
        .await?;

        if canonical.is_none() {
            return Err(PostgresStateError::NonCanonicalCheckpoint(
                hex_hash(&target.hash),
                target.height,
            ));
        }

        sqlx::query!(
            r"
            INSERT INTO checkpoint (chain_id, last_height, last_hash)
            VALUES (($1::text)::numeric, $2, $3)
            ON CONFLICT (chain_id) DO UPDATE
            SET last_height = EXCLUDED.last_height,
                last_hash = EXCLUDED.last_hash,
                updated_at = now()
            ",
            &self.chain_id,
            pg_height(target.height)?,
            hash_bytes(&target.hash),
        )
        .execute(tx.as_mut())
        .await?;
        Ok(())
    }

    /// Reads the durable checkpoint.
    ///
    /// # Errors
    ///
    /// Returns a database error if the checkpoint cannot be read.
    pub async fn checkpoint(&self) -> Result<Option<Checkpoint>, PostgresStateError> {
        sqlx::query!(
            r"
            SELECT last_height, last_hash
            FROM checkpoint
            WHERE chain_id = ($1::text)::numeric
            ",
            &self.chain_id,
        )
        .fetch_optional(&self.pool)
        .await?
        .map(|row| row_to_checkpoint(row.last_height, row.last_hash))
        .transpose()
    }

    /// Reads the canonical header at a height from persisted state.
    ///
    /// # Errors
    ///
    /// Returns a database error or a conversion error for invalid stored bytes.
    pub async fn canonical_header_at_height(
        &self,
        height: u64,
    ) -> Result<Option<BlockHeader>, PostgresStateError> {
        sqlx::query!(
            r"
            SELECT block_hash, parent_hash, height
            FROM blocks
            WHERE chain_id = ($1::text)::numeric AND height = $2 AND is_canonical
            ",
            &self.chain_id,
            pg_height(height)?,
        )
        .fetch_optional(&self.pool)
        .await?
        .map(|row| row_to_header(row.block_hash, row.parent_hash, row.height))
        .transpose()
    }

    async fn canonical_headers_descending(
        &self,
        from_height: u64,
        to_exclusive: u64,
    ) -> Result<Vec<BlockHeader>, PostgresStateError> {
        let rows = sqlx::query!(
            r"
            SELECT block_hash, parent_hash, height
            FROM blocks
            WHERE chain_id = ($1::text)::numeric
              AND height > $2
              AND height <= $3
              AND is_canonical
            ORDER BY height DESC
            ",
            &self.chain_id,
            pg_height(to_exclusive)?,
            pg_height(from_height)?,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| row_to_header(row.block_hash, row.parent_hash, row.height))
            .collect()
    }

    /// Returns canonical raw logs by joining through the owning canonical blocks.
    ///
    /// # Errors
    ///
    /// Returns a database error or invalid stored hash/address lengths.
    pub async fn canonical_logs(&self) -> Result<Vec<RawLog>, PostgresStateError> {
        let rows = sqlx::query_as!(
            RawLogRow,
            r"
            SELECT
                logs.transaction_index,
                logs.log_index,
                logs.tx_hash,
                logs.address,
                logs.topics,
                logs.data,
                logs.decoded_event,
                logs.decoder_version
            FROM logs
            JOIN blocks
              ON blocks.chain_id = logs.chain_id
             AND blocks.block_hash = logs.block_hash
            WHERE logs.chain_id = ($1::text)::numeric
              AND blocks.is_canonical
            ORDER BY logs.block_number, logs.transaction_index, logs.log_index
            ",
            &self.chain_id,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(row_to_raw_log).collect()
    }

    /// Returns the append-only outbox transition journal.
    ///
    /// # Errors
    ///
    /// Returns a database error or invalid stored hash lengths.
    pub async fn outbox_events(&self) -> Result<Vec<OutboxEvent>, PostgresStateError> {
        let rows = sqlx::query!(
            r"
            SELECT event_id, event_kind, block_hash, block_height
            FROM outbox_events
            WHERE chain_id = ($1::text)::numeric
            ORDER BY event_id
            ",
            &self.chain_id,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                row_to_outbox_event(
                    row.event_id,
                    row.event_kind,
                    row.block_hash,
                    row.block_height,
                )
            })
            .collect()
    }

    /// Reconciles persisted checkpoint state to the source head before live mode begins.
    ///
    /// # Errors
    ///
    /// Returns an error when persisted ancestry and source ancestry cannot be reconciled within
    /// the configured maximum walk-back depth.
    pub async fn reconcile_to_head<S>(
        &self,
        source: &mut S,
        max_walk_back_depth: u64,
    ) -> Result<ReconciliationSummary, ReconciliationError>
    where
        S: ReconciliationSource + Send,
    {
        let head = source.head().await?;
        let checkpoint = self.checkpoint().await?;
        let common = self
            .find_reconciliation_common_ancestor(
                source,
                checkpoint.as_ref(),
                head.header.height,
                max_walk_back_depth,
            )
            .await?;

        let mut events = Vec::new();
        if let Some(checkpoint) = checkpoint {
            let common_height = common.map_or(0, |header| header.height);
            for header in self
                .canonical_headers_descending(checkpoint.last_height, common_height)
                .await?
            {
                events.push(DurableChainEvent::Rollback(header));
            }
        }

        let start_height = common.map_or(0, |header| header.height + 1);
        let mut previous = common.map(|header| header.hash);
        for height in start_height..=head.header.height {
            let block = source
                .block_by_height(height)
                .await?
                .ok_or(ReconciliationError::MissingSourceHeight(height))?;
            if let Some(parent_hash) = previous
                && block.header.parent_hash != parent_hash
            {
                return Err(ReconciliationError::BrokenSourceLink {
                    child_height: height,
                });
            }
            previous = Some(block.header.hash);
            events.push(DurableChainEvent::Apply(block));
        }

        if events.is_empty() {
            return Ok(ReconciliationSummary {
                final_checkpoint: self.checkpoint().await?,
                ..ReconciliationSummary::default()
            });
        }

        let transition = if events
            .iter()
            .any(|event| matches!(event, DurableChainEvent::Rollback(_)))
        {
            ChainTransition::Reorg
        } else if common.is_some() {
            ChainTransition::Gap
        } else {
            ChainTransition::Bootstrap
        };
        let batch = DurableChainBatch {
            transition,
            common_ancestor: common,
            events,
        };
        let report = self.apply_batch(&batch).await?;

        Ok(ReconciliationSummary {
            rolled_back_blocks: report.rolled_back_blocks,
            applied_blocks: report.applied_blocks,
            final_checkpoint: self.checkpoint().await?,
        })
    }

    async fn find_reconciliation_common_ancestor<S>(
        &self,
        source: &mut S,
        checkpoint: Option<&Checkpoint>,
        head_height: u64,
        max_walk_back_depth: u64,
    ) -> Result<Option<BlockHeader>, ReconciliationError>
    where
        S: ReconciliationSource + Send,
    {
        let Some(checkpoint) = checkpoint else {
            return Ok(None);
        };

        let mut height = checkpoint.last_height.min(head_height);
        let mut walked = 0;
        loop {
            let persisted = self
                .canonical_header_at_height(height)
                .await?
                .ok_or(ReconciliationError::MissingPersistedCheckpointBlock(height))?;
            let source = source
                .block_by_height(height)
                .await?
                .ok_or(ReconciliationError::MissingSourceHeight(height))?;

            if persisted == source.header {
                return Ok(Some(persisted));
            }

            walked += 1;
            if walked > max_walk_back_depth || height == 0 {
                return Err(ReconciliationError::MaxDepthExceeded(max_walk_back_depth));
            }
            height -= 1;
        }
    }
}

impl SerializedWriter {
    /// Starts one serialized writer task for a chain.
    #[must_use]
    pub fn spawn(writer: PostgresChainWriter, capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self::spawn_with_receiver(writer, receiver, sender)
    }

    #[must_use]
    pub fn spawn_with_receiver(
        writer: PostgresChainWriter,
        receiver: mpsc::Receiver<DurableChainBatch>,
        sender: mpsc::Sender<DurableChainBatch>,
    ) -> Self {
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        tracker.spawn(writer_loop(writer, receiver, cancel.clone()));
        Self {
            sender: Some(sender),
            cancel,
            tracker,
        }
    }

    /// Enqueues a batch unless shutdown has started.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer is shutting down or the worker task has closed.
    pub async fn submit(&self, batch: DurableChainBatch) -> Result<(), WriterQueueError> {
        if self.cancel.is_cancelled() {
            return Err(WriterQueueError::ShuttingDown);
        }
        let Some(sender) = &self.sender else {
            return Err(WriterQueueError::ShuttingDown);
        };
        sender
            .send(batch)
            .await
            .map_err(|_| WriterQueueError::Closed)
    }

    /// Cancels new work and waits for the current transaction plus queued work to drain.
    ///
    /// # Errors
    ///
    /// Returns a timeout error if the writer does not finish within the supplied duration.
    pub async fn shutdown(mut self, drain_timeout: Duration) -> Result<(), WriterShutdownError> {
        self.cancel.cancel();
        self.sender.take();
        self.tracker.close();
        timeout(drain_timeout, self.tracker.wait())
            .await
            .map_err(|_| WriterShutdownError::Timeout(drain_timeout))
    }
}

async fn writer_loop(
    writer: PostgresChainWriter,
    mut receiver: mpsc::Receiver<DurableChainBatch>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                receiver.close();
                while let Some(batch) = receiver.recv().await {
                    if let Err(error) = writer.apply_batch(&batch).await {
                        error!(%error, "serialized writer failed while draining");
                        break;
                    }
                }
                break;
            }
            maybe_batch = receiver.recv() => {
                let Some(batch) = maybe_batch else {
                    break;
                };
                if let Err(error) = writer.apply_batch(&batch).await {
                    error!(%error, "serialized writer failed");
                    break;
                }
            }
        }
    }
}

async fn pending_crash() {
    warn!("postgres writer reached configured crash point");
    std::future::pending::<()>().await;
}

fn row_to_checkpoint(
    last_height: i64,
    last_hash: Vec<u8>,
) -> Result<Checkpoint, PostgresStateError> {
    let last_height =
        u64::try_from(last_height).map_err(|_| PostgresStateError::HeightOverflow(u64::MAX))?;
    let last_hash = hash_from_vec(last_hash)?;
    Ok(Checkpoint {
        last_height,
        last_hash,
    })
}

fn row_to_header(
    block_hash: Vec<u8>,
    parent_hash: Vec<u8>,
    height: i64,
) -> Result<BlockHeader, PostgresStateError> {
    let height = u64::try_from(height).map_err(|_| PostgresStateError::HeightOverflow(u64::MAX))?;
    Ok(BlockHeader::new(
        hash_from_vec(block_hash)?,
        hash_from_vec(parent_hash)?,
        height,
    ))
}

fn row_to_raw_log(row: RawLogRow) -> Result<RawLog, PostgresStateError> {
    let transaction_index = u32::try_from(row.transaction_index)
        .map_err(|_| PostgresStateError::IntOverflow(u32::MAX))?;
    let log_index =
        u32::try_from(row.log_index).map_err(|_| PostgresStateError::IntOverflow(u32::MAX))?;
    let topics = row
        .topics
        .into_iter()
        .map(hash_from_vec)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RawLog {
        transaction_index,
        log_index,
        tx_hash: hash_from_vec(row.tx_hash)?,
        address: address_from_vec(row.address)?,
        topics,
        data: row.data,
        decoded_event: row.decoded_event,
        decoder_version: row.decoder_version,
    })
}

fn row_to_outbox_event(
    event_id: i64,
    event_kind: String,
    block_hash: Vec<u8>,
    block_height: i64,
) -> Result<OutboxEvent, PostgresStateError> {
    let block_height =
        u64::try_from(block_height).map_err(|_| PostgresStateError::HeightOverflow(u64::MAX))?;
    Ok(OutboxEvent {
        event_id,
        event_kind,
        block_hash: hash_from_vec(block_hash)?,
        block_height,
    })
}

fn pg_height(value: u64) -> Result<i64, PostgresStateError> {
    i64::try_from(value).map_err(|_| PostgresStateError::HeightOverflow(value))
}

fn pg_int(value: u32) -> Result<i32, PostgresStateError> {
    i32::try_from(value).map_err(|_| PostgresStateError::IntOverflow(value))
}

fn hash_bytes(value: &BlockHash) -> Vec<u8> {
    value.to_vec()
}

fn address_bytes(value: &[u8; 20]) -> Vec<u8> {
    value.to_vec()
}

fn hash_from_vec(value: Vec<u8>) -> Result<BlockHash, PostgresStateError> {
    value
        .try_into()
        .map_err(|value: Vec<u8>| PostgresStateError::InvalidHashLength(value.len()))
}

fn address_from_vec(value: Vec<u8>) -> Result<[u8; 20], PostgresStateError> {
    value
        .try_into()
        .map_err(|value: Vec<u8>| PostgresStateError::InvalidAddressLength(value.len()))
}

fn hex_hash(hash: &BlockHash) -> String {
    let mut output = String::with_capacity(66);
    output.push_str("0x");
    for byte in hash {
        write!(&mut output, "{byte:02x}").expect("writing to string cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use std::{env, process::Command, str::FromStr as _, time::Duration};

    use chainweave_core::{BlockHeader, ChainTransition};
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    use super::*;

    const TEST_CHAIN_ID: u64 = 31_337;

    struct TestDb {
        admin_pool: PgPool,
        schema: String,
        writer: PostgresChainWriter,
    }

    #[tokio::test]
    async fn idempotent_replay_leaves_same_canonical_state_and_outbox() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        db.writer.ensure_chain_identity(hash(90)).await.unwrap();
        let batch = batch(
            None,
            [apply_block(0, 0), apply_block(1, 0), apply_block(2, 1)],
        );

        db.writer.apply_batch(&batch).await.unwrap();
        let first_checkpoint = db.writer.checkpoint().await.unwrap();
        let first_logs = db.writer.canonical_logs().await.unwrap();
        let first_outbox = db.writer.outbox_events().await.unwrap();

        db.writer.apply_batch(&batch).await.unwrap();

        assert_eq!(db.writer.checkpoint().await.unwrap(), first_checkpoint);
        assert_eq!(db.writer.canonical_logs().await.unwrap(), first_logs);
        assert_eq!(db.writer.outbox_events().await.unwrap(), first_outbox);
        assert_eq!(first_checkpoint.unwrap().last_height, 2);
        assert_eq!(first_logs.len(), 3);
        assert_eq!(first_outbox.len(), 3);
        db.cleanup().await;
    }

    #[tokio::test]
    async fn reorg_of_a_reorg_derives_canonical_logs_from_blocks() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        db.writer.ensure_chain_identity(hash(90)).await.unwrap();
        let canonical = batch(
            None,
            [
                apply_block(0, 0),
                apply_block(1, 0),
                apply_block(2, 1),
                apply_block(3, 2),
            ],
        );
        db.writer.apply_batch(&canonical).await.unwrap();

        let b2 = block(12, 1);
        let b3 = block(13, 12);
        let first_reorg = DurableChainBatch {
            transition: ChainTransition::Reorg,
            common_ancestor: Some(header(1, 0)),
            events: vec![
                DurableChainEvent::Rollback(header(3, 2)),
                DurableChainEvent::Rollback(header(2, 1)),
                DurableChainEvent::Apply(b2),
                DurableChainEvent::Apply(b3),
            ],
        };
        db.writer.apply_batch(&first_reorg).await.unwrap();

        let c2 = block(22, 1);
        let c3 = block(23, 22);
        let second_reorg = DurableChainBatch {
            transition: ChainTransition::Reorg,
            common_ancestor: Some(header(1, 0)),
            events: vec![
                DurableChainEvent::Rollback(header(13, 12)),
                DurableChainEvent::Rollback(header(12, 1)),
                DurableChainEvent::Apply(c2),
                DurableChainEvent::Apply(c3),
            ],
        };
        db.writer.apply_batch(&second_reorg).await.unwrap();

        let canonical_tx_hashes = db
            .writer
            .canonical_logs()
            .await
            .unwrap()
            .into_iter()
            .map(|log| log.tx_hash[0])
            .collect::<Vec<_>>();
        assert_eq!(canonical_tx_hashes, vec![40, 41, 62, 63]);

        let checkpoint = db.writer.checkpoint().await.unwrap().unwrap();
        assert_eq!(checkpoint.last_height, 3);
        assert_eq!(checkpoint.last_hash, hash(23));
        assert_eq!(db.writer.outbox_events().await.unwrap().len(), 12);
        assert_checkpoint_references_canonical_block(&db.writer).await;
        db.cleanup().await;
    }

    #[tokio::test]
    async fn startup_reconciliation_walks_back_and_fills_missing_heights() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        db.writer.ensure_chain_identity(hash(90)).await.unwrap();
        let canonical = batch(
            None,
            [
                apply_block(0, 0),
                apply_block(1, 0),
                apply_block(2, 1),
                apply_block(3, 2),
            ],
        );
        db.writer.apply_batch(&canonical).await.unwrap();

        let mut source = MemorySource::new([
            block(0, 0),
            block(1, 0),
            block(12, 1),
            block(13, 12),
            block(14, 13),
        ]);
        let summary = db.writer.reconcile_to_head(&mut source, 8).await.unwrap();

        assert_eq!(summary.rolled_back_blocks, 2);
        assert_eq!(summary.applied_blocks, 3);
        assert_eq!(summary.final_checkpoint.unwrap().last_hash, hash(14));
        let canonical_tx_hashes = db
            .writer
            .canonical_logs()
            .await
            .unwrap()
            .into_iter()
            .map(|log| log.tx_hash[0])
            .collect::<Vec<_>>();
        assert_eq!(canonical_tx_hashes, vec![40, 41, 52, 53, 54]);
        db.cleanup().await;
    }

    #[tokio::test]
    async fn kill_restart_crash_recovery_matches_clean_replay() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        db.writer.ensure_chain_identity(hash(90)).await.unwrap();
        run_crash_child(&db, "before_commit").await;
        assert!(db.writer.checkpoint().await.unwrap().is_none());
        assert!(db.writer.outbox_events().await.unwrap().is_empty());

        let clean_batch = crash_batch();
        db.writer.apply_batch(&clean_batch).await.unwrap();
        let clean_checkpoint = db.writer.checkpoint().await.unwrap();
        let clean_logs = db.writer.canonical_logs().await.unwrap();
        let clean_outbox = db.writer.outbox_events().await.unwrap();

        let Some(after_commit_db) = TestDb::create().await else {
            db.cleanup().await;
            return;
        };
        after_commit_db
            .writer
            .ensure_chain_identity(hash(90))
            .await
            .unwrap();
        run_crash_child(&after_commit_db, "after_commit").await;
        after_commit_db
            .writer
            .apply_batch(&clean_batch)
            .await
            .unwrap();

        assert_eq!(
            after_commit_db.writer.checkpoint().await.unwrap(),
            clean_checkpoint
        );
        assert_eq!(
            after_commit_db.writer.canonical_logs().await.unwrap(),
            clean_logs
        );
        assert_eq!(
            semantic_outbox(after_commit_db.writer.outbox_events().await.unwrap()),
            semantic_outbox(clean_outbox)
        );
        assert_checkpoint_references_canonical_block(&after_commit_db.writer).await;
        db.cleanup().await;
        after_commit_db.cleanup().await;
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_queued_batches() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        db.writer.ensure_chain_identity(hash(90)).await.unwrap();
        let serialized = SerializedWriter::spawn(db.writer.clone(), 4);
        serialized
            .submit(batch(None, [apply_block(0, 0)]))
            .await
            .unwrap();
        serialized
            .submit(batch(Some(header(0, 0)), [apply_block(1, 0)]))
            .await
            .unwrap();

        serialized.shutdown(Duration::from_secs(5)).await.unwrap();

        assert_eq!(
            db.writer.checkpoint().await.unwrap().unwrap().last_height,
            1
        );
        db.cleanup().await;
    }

    #[tokio::test]
    async fn crash_helper_entry() {
        let Ok(mode) = env::var("CHAINWEAVE_CRASH_HELPER") else {
            return;
        };
        let database_url = env::var("CHAINWEAVE_TEST_DATABASE_URL").unwrap();
        let schema = env::var("CHAINWEAVE_TEST_SCHEMA").unwrap();
        let pool = pool_for_schema(&database_url, &schema).await.unwrap();
        let writer = PostgresChainWriter::new(pool, TEST_CHAIN_ID).unwrap();
        let crash_point = match mode.as_str() {
            "before_commit" => CrashPoint::BeforeCommit,
            "after_commit" => CrashPoint::AfterCommit,
            other => panic!("unknown crash helper mode {other}"),
        };
        writer
            .apply_batch_with_crash_point(&crash_batch(), crash_point)
            .await
            .unwrap();
    }

    impl TestDb {
        async fn create() -> Option<Self> {
            let Ok(database_url) = env::var("CHAINWEAVE_TEST_DATABASE_URL") else {
                eprintln!("skipping Postgres integration test: CHAINWEAVE_TEST_DATABASE_URL unset");
                return None;
            };
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&database_url)
                .await
                .unwrap();
            let schema = format!(
                "chainweave_test_{}_{}",
                std::process::id(),
                OffsetDateTime::now_utc().unix_timestamp_nanos()
            );
            sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
                .execute(&admin_pool)
                .await
                .unwrap();
            let pool = pool_for_schema(&database_url, &schema).await.unwrap();
            let writer = PostgresChainWriter::new(pool, TEST_CHAIN_ID).unwrap();
            writer.run_migrations().await.unwrap();
            Some(Self {
                admin_pool,
                schema,
                writer,
            })
        }

        async fn cleanup(self) {
            sqlx::query(&format!(r#"DROP SCHEMA "{}" CASCADE"#, self.schema))
                .execute(&self.admin_pool)
                .await
                .unwrap();
        }
    }

    #[derive(Debug)]
    struct MemorySource {
        blocks: Vec<IndexedBlock>,
    }

    impl MemorySource {
        fn new(blocks: impl IntoIterator<Item = IndexedBlock>) -> Self {
            Self {
                blocks: blocks.into_iter().collect(),
            }
        }
    }

    impl ReconciliationSource for MemorySource {
        async fn head(&mut self) -> Result<IndexedBlock, ReconciliationError> {
            self.blocks
                .last()
                .cloned()
                .ok_or(ReconciliationError::MissingSourceHeight(0))
        }

        async fn block_by_height(
            &mut self,
            height: u64,
        ) -> Result<Option<IndexedBlock>, ReconciliationError> {
            Ok(self
                .blocks
                .iter()
                .find(|block| block.header.height == height)
                .cloned())
        }
    }

    async fn pool_for_schema(database_url: &str, schema: &str) -> Result<PgPool, sqlx::Error> {
        let options = PgConnectOptions::from_str(database_url)
            .unwrap()
            .options([("search_path", schema)]);
        PgPoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
    }

    async fn run_crash_child(db: &TestDb, mode: &str) {
        let mut child = Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg("postgres::tests::crash_helper_entry")
            .arg("--nocapture")
            .env("CHAINWEAVE_CRASH_HELPER", mode)
            .env(
                "CHAINWEAVE_TEST_DATABASE_URL",
                env::var("CHAINWEAVE_TEST_DATABASE_URL").unwrap(),
            )
            .env("CHAINWEAVE_TEST_SCHEMA", &db.schema)
            .spawn()
            .unwrap();

        tokio::time::sleep(Duration::from_millis(750)).await;
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(!status.success());
    }

    async fn assert_checkpoint_references_canonical_block(writer: &PostgresChainWriter) {
        let count: i64 = sqlx::query_scalar(
            r"
            SELECT count(*)
            FROM checkpoint
            JOIN blocks
              ON blocks.chain_id = checkpoint.chain_id
             AND blocks.block_hash = checkpoint.last_hash
             AND blocks.height = checkpoint.last_height
            WHERE checkpoint.chain_id = $1::numeric
              AND blocks.is_canonical
            ",
        )
        .bind(&writer.chain_id)
        .fetch_one(writer.pool())
        .await
        .unwrap();
        assert_eq!(count, 1);
    }

    fn semantic_outbox(events: Vec<OutboxEvent>) -> Vec<(String, BlockHash, u64)> {
        events
            .into_iter()
            .map(|event| (event.event_kind, event.block_hash, event.block_height))
            .collect()
    }

    fn crash_batch() -> DurableChainBatch {
        batch(
            None,
            [apply_block(0, 0), apply_block(1, 0), apply_block(2, 1)],
        )
    }

    fn batch<const N: usize>(
        common_ancestor: Option<BlockHeader>,
        blocks: [IndexedBlock; N],
    ) -> DurableChainBatch {
        let transition = if common_ancestor.is_some() {
            ChainTransition::Gap
        } else {
            ChainTransition::Bootstrap
        };
        DurableChainBatch {
            transition,
            common_ancestor,
            events: blocks
                .into_iter()
                .map(DurableChainEvent::Apply)
                .collect::<Vec<_>>(),
        }
    }

    fn apply_block(value: u8, parent: u8) -> IndexedBlock {
        block(value, parent)
    }

    fn block(value: u8, parent: u8) -> IndexedBlock {
        let header = header(value, parent);
        IndexedBlock {
            header,
            timestamp: OffsetDateTime::from_unix_timestamp(1_800_000_000 + i64::from(value))
                .unwrap(),
            status: BlockStatus::Unsafe,
            status_source: StatusSource::Observed,
            logs: vec![RawLog {
                transaction_index: 0,
                log_index: 0,
                tx_hash: hash(40 + value),
                address: [20 + value; 20],
                topics: vec![hash(70 + value)],
                data: vec![value],
                decoded_event: None,
                decoder_version: None,
            }],
        }
    }

    fn header(value: u8, parent: u8) -> BlockHeader {
        let height = if value == 0 {
            0
        } else if value < 10 {
            u64::from(value)
        } else {
            u64::from(value % 10)
        };
        BlockHeader::new(hash(value), hash(parent), height)
    }

    fn hash(value: u8) -> BlockHash {
        [value; 32]
    }
}
