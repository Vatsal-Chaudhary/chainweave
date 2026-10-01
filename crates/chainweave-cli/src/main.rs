use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use chainweave_core::{
    AppConfig, AsyncRangeCommitSink, BackfillError, BackfillRange, ChainIdentity, CommitProgress,
    FetchedRange, LogRangeSizer, OrderedCommitCoordinator, RetryDecision, RetryPolicy, RpcBudget,
    RpcFailure, RpcMethod, ValidationProfile,
};
use chainweave_rpc::{
    AnchoredRawLog, ContractLogFilter, RpcClient, RpcError, capture_target_head_with_retry,
    classify_rpc_error, fetch_header_by_number_with_retry,
};
use chainweave_sink::{IndexedBlock, PostgresBackfillCommitter, PostgresChainWriter};
use clap::{Parser, Subcommand};
use tokio::task::JoinSet;
use tracing::info;
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Parser)]
#[command(name = "chainweave", version, about = "Reorg-safe EVM chain indexer")]
struct Cli {
    #[arg(long, global = true, env = "CHAINWEAVE_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, global = true)]
    rpc_url: Option<Url>,
    #[arg(long, global = true, requires = "expected_genesis_hash")]
    expected_chain_id: Option<u64>,
    #[arg(long, global = true, requires = "expected_chain_id")]
    expected_genesis_hash: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Read and verify the current head from the configured primary RPC.
    Head,
    /// Execute a bounded historical backfill range.
    Backfill {
        #[arg(long)]
        from_block: u64,
        #[arg(long)]
        to_block: u64,
        #[arg(long)]
        contract_address: Option<String>,
        #[arg(long)]
        verify_reference: bool,
        #[arg(long)]
        reference_rpc_url: Option<Url>,
        #[arg(long, default_value_t = 1_000)]
        initial_log_blocks: u64,
        #[arg(long, default_value_t = 100)]
        min_log_blocks: u64,
        #[arg(long, default_value_t = 5_000)]
        max_log_blocks: u64,
        #[arg(long, default_value_t = 30_000)]
        rpc_timeout_ms: u64,
        #[arg(long)]
        rpc_max_requests: Option<u64>,
        #[arg(long)]
        rpc_max_cost_units: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing()?;
    let cli = Cli::parse();
    let mut config = AppConfig::load(cli.config.as_deref()).context("configuration load failed")?;
    apply_cli_overrides(&mut config, &cli)?;

    match cli.command {
        Command::Head => run_head(&config).await,
        Command::Backfill {
            from_block,
            to_block,
            contract_address,
            verify_reference,
            reference_rpc_url,
            initial_log_blocks,
            min_log_blocks,
            max_log_blocks,
            rpc_timeout_ms,
            rpc_max_requests,
            rpc_max_cost_units,
        } => {
            let options = BackfillOptions {
                from_block,
                to_block,
                contract_address,
                verify_reference,
                reference_rpc_url,
                initial_log_blocks,
                min_log_blocks,
                max_log_blocks,
                rpc_timeout: Duration::from_millis(rpc_timeout_ms),
                rpc_max_requests,
                rpc_max_cost_units,
            };
            run_backfill(&config, options).await
        }
    }
}

#[derive(Debug)]
struct BackfillOptions {
    from_block: u64,
    to_block: u64,
    contract_address: Option<String>,
    verify_reference: bool,
    reference_rpc_url: Option<Url>,
    initial_log_blocks: u64,
    min_log_blocks: u64,
    max_log_blocks: u64,
    rpc_timeout: Duration,
    rpc_max_requests: Option<u64>,
    rpc_max_cost_units: Option<u64>,
}

#[derive(Debug, Clone)]
struct AdaptiveFetchConfig {
    workers: usize,
    retry_policy: RetryPolicy,
    rpc_timeout: Duration,
    filter: Option<ContractLogFilter>,
    budget: SharedRpcBudget,
}

#[derive(Debug, Clone)]
struct SharedRpcBudget {
    inner: Arc<Mutex<RpcBudget>>,
}

#[derive(Debug, thiserror::Error)]
enum FetchRangeError {
    #[error("RPC request failed for range {range:?}: {source}")]
    Rpc {
        range: BackfillRange,
        source: RpcError,
    },
    #[error("provider limited eth_getLogs range {range:?}: {source}")]
    ProviderLimit {
        range: BackfillRange,
        source: RpcError,
    },
    #[error("RPC timeout while fetching {phase} for range {range:?}")]
    Timeout {
        range: BackfillRange,
        phase: &'static str,
    },
    #[error("backfill execution error: {0}")]
    Backfill(#[from] BackfillError),
    #[error("RPC budget mutex poisoned")]
    BudgetPoisoned,
    #[error("RPC log block hash does not match fetched header at height {height}")]
    LogBlockHashMismatch { height: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct RpcUsage {
    requests: u64,
    cost_units: u64,
}

fn apply_cli_overrides(config: &mut AppConfig, cli: &Cli) -> Result<()> {
    if let Some(url) = &cli.rpc_url {
        config.rpc.primary_url.clone_from(url);
    }
    match (&cli.expected_chain_id, &cli.expected_genesis_hash) {
        (Some(chain_id), Some(genesis_hash)) => {
            config.expected_chain = Some(ChainIdentity {
                chain_id: *chain_id,
                genesis_hash: genesis_hash.clone(),
            });
        }
        (None, None) => {}
        _ => bail!("expected chain ID and genesis hash must be provided together"),
    }
    Ok(())
}

async fn run_head(config: &AppConfig) -> Result<()> {
    config
        .validate(ValidationProfile::Head)
        .context("configuration validation failed")?;
    let client = RpcClient::connect(&config.rpc.primary_url).await?;
    let head = client.head().await?;
    if let Some(expected) = &config.expected_chain {
        RpcClient::verify_identity(&head, expected)?;
    }

    info!(
        chain_id = head.chain_id,
        block_number = head.number,
        "read current chain head"
    );
    println!("{}", serde_json::to_string_pretty(&head)?);
    Ok(())
}

async fn run_backfill(config: &AppConfig, options: BackfillOptions) -> Result<()> {
    config
        .validate(ValidationProfile::Workers)
        .context("configuration validation failed")?;
    let database_url = config
        .database_url
        .as_deref()
        .context("database_url is required before running backfill")?;

    let full_range = BackfillRange::new(options.from_block, options.to_block)?;
    let retry_policy = RetryPolicy::new(
        8,
        Duration::from_millis(500),
        Duration::from_secs(30),
        Duration::from_millis(500),
    )?;
    let sizer = LogRangeSizer::new(
        options.initial_log_blocks,
        options.min_log_blocks,
        options.max_log_blocks,
    )?;
    let contract_filter = options
        .contract_address
        .as_deref()
        .map(ContractLogFilter::address)
        .transpose()?;
    let budget = SharedRpcBudget::new(RpcBudget::new(
        options
            .rpc_max_requests
            .unwrap_or_else(|| full_range.len().saturating_mul(3).saturating_add(5_000)),
        options
            .rpc_max_cost_units
            .unwrap_or_else(|| full_range.len().saturating_mul(3).saturating_add(100_000)),
    )?);

    let client = RpcClient::connect(&config.rpc.primary_url).await?;
    let target_head = capture_target_head_with_retry(&client, retry_policy, options.rpc_timeout)
        .await
        .context("failed to capture target head")?;
    if let Some(expected) = &config.expected_chain {
        RpcClient::verify_identity(&target_head, expected)?;
    }
    if options.to_block > target_head.number {
        bail!(
            "backfill range end {} is beyond captured target head {}",
            options.to_block,
            target_head.number
        );
    }

    let writer = PostgresChainWriter::connect(database_url, target_head.chain_id).await?;
    writer.run_migrations().await?;
    writer
        .ensure_chain_identity(target_head.genesis_block_hash())
        .await?;

    let parent_anchor = if options.from_block == 0 {
        None
    } else {
        budget.record(RpcMethod::GetBlockByNumber, 0)?;
        Some(
            fetch_header_by_number_with_retry(
                &client,
                options.from_block - 1,
                retry_policy,
                options.rpc_timeout,
            )
            .await
            .context("failed to fetch parent anchor")?,
        )
    };
    let mut coordinator =
        OrderedCommitCoordinator::new(options.from_block, options.to_block, parent_anchor)?;
    let mut committer = PostgresBackfillCommitter::new(writer, parent_anchor);

    info!(
        chain_id = target_head.chain_id,
        captured_height = target_head.number,
        from_block = options.from_block,
        to_block = options.to_block,
        workers = config.queues.fetch,
        initial_log_blocks = options.initial_log_blocks,
        min_log_blocks = options.min_log_blocks,
        max_log_blocks = options.max_log_blocks,
        "starting adaptive bounded backfill"
    );

    let started_at = Instant::now();
    let fetch_config = AdaptiveFetchConfig {
        workers: config.queues.fetch,
        retry_policy,
        rpc_timeout: options.rpc_timeout,
        filter: contract_filter,
        budget: budget.clone(),
    };
    let report = fetch_and_commit_ranges(
        client,
        full_range,
        sizer,
        fetch_config,
        &mut coordinator,
        &mut committer,
    )
    .await?;

    if options.to_block == target_head.number
        && committer.last_committed_hash() != Some(target_head.block_hash())
    {
        bail!(
            "captured target head changed before commit at height {}",
            options.to_block
        );
    }

    let canonical_summary = committer
        .writer()
        .canonical_range_summary(full_range.from_block, full_range.to_block)
        .await?;
    if canonical_summary.canonical_blocks != full_range.len()
        || canonical_summary.min_height != Some(full_range.from_block)
        || canonical_summary.max_height != Some(full_range.to_block)
    {
        bail!(
            "canonical continuity check failed for {}..={}: count {}, min {:?}, max {:?}",
            full_range.from_block,
            full_range.to_block,
            canonical_summary.canonical_blocks,
            canonical_summary.min_height,
            canonical_summary.max_height
        );
    }
    let checkpoint = committer
        .writer()
        .checkpoint()
        .await?
        .context("backfill did not write a checkpoint")?;
    if checkpoint.last_height != full_range.to_block {
        bail!(
            "checkpoint continuity check failed: checkpoint height {}, expected {}",
            checkpoint.last_height,
            full_range.to_block
        );
    }

    let reference_records = if options.verify_reference {
        let reference_url = options
            .reference_rpc_url
            .as_ref()
            .unwrap_or(&config.rpc.primary_url);
        let reference_client = RpcClient::connect(reference_url).await?;
        let mut reference_sizer = LogRangeSizer::new(
            options.initial_log_blocks,
            options.min_log_blocks,
            options.max_log_blocks,
        )?;
        let mut records = Vec::new();
        let mut next = full_range.from_block;
        while let Some(range) = reference_sizer.next_range(next, full_range.to_block) {
            let range_records = fetch_reference_range_with_retry(
                &reference_client,
                range,
                contract_filter,
                retry_policy,
                options.rpc_timeout,
                &budget,
            )
            .await?;
            reference_sizer.record_success();
            records.extend(range_records);
            next = range.to_block.saturating_add(1);
        }
        records.sort();
        Some(records)
    } else {
        None
    };

    let mut database_records = committer
        .writer()
        .canonical_log_records(
            full_range.from_block,
            full_range.to_block,
            contract_filter.map(ContractLogFilter::address_bytes),
        )
        .await?;
    let committed_log_records = database_records.len();

    if let Some(reference_records) = reference_records {
        database_records.sort();
        if database_records != reference_records {
            bail!(
                "reference comparison failed: database records {}, reference records {}",
                database_records.len(),
                reference_records.len()
            );
        }
        println!(
            "reference comparison: matched {} normalized records",
            database_records.len()
        );
    }

    let elapsed = started_at.elapsed();
    let usage = budget.usage()?;
    let blocks_per_second = report.committed_blocks as f64 / elapsed.as_secs_f64().max(0.001);
    let rpc_calls_per_1k_blocks =
        usage.requests as f64 * 1_000.0 / report.committed_blocks.max(1) as f64;
    println!(
        "backfilled {}..={} through height {}",
        full_range.from_block,
        full_range.to_block,
        report
            .last_committed_height
            .map_or_else(|| "none".to_owned(), |height| height.to_string())
    );
    println!(
        "committed ranges: {}, blocks: {}, log records: {}, elapsed: {:.2}s, throughput: {:.2} blocks/s",
        report.committed_ranges,
        report.committed_blocks,
        committed_log_records,
        elapsed.as_secs_f64(),
        blocks_per_second
    );
    println!(
        "canonical continuity: {} blocks from {} through {}",
        canonical_summary.canonical_blocks,
        canonical_summary
            .min_height
            .unwrap_or(full_range.from_block),
        canonical_summary.max_height.unwrap_or(full_range.to_block)
    );
    println!(
        "RPC budget used: {} requests / {} cost units ({:.2} calls per 1k blocks)",
        usage.requests, usage.cost_units, rpc_calls_per_1k_blocks
    );
    Ok(())
}

async fn fetch_and_commit_ranges(
    client: RpcClient,
    full_range: BackfillRange,
    mut sizer: LogRangeSizer,
    config: AdaptiveFetchConfig,
    coordinator: &mut OrderedCommitCoordinator<IndexedBlock>,
    committer: &mut impl AsyncRangeCommitSink<IndexedBlock, Error = String>,
) -> Result<CommitProgress> {
    let mut join_set = JoinSet::new();
    let mut queued = VecDeque::new();
    let mut next_unallocated = full_range.from_block;
    let mut in_flight = 0_usize;
    let mut progress = CommitProgress::default();

    fill_fetch_window(
        &mut join_set,
        &client,
        &config,
        &mut queued,
        &mut next_unallocated,
        full_range.to_block,
        &sizer,
        &mut in_flight,
    );

    while let Some(result) = join_set.join_next().await {
        in_flight = in_flight.saturating_sub(1);
        match result.context("backfill fetch task panicked")? {
            Ok(fetched) => {
                sizer.record_success();
                let before_push_next_height = coordinator.next_height();
                match coordinator.push_async(fetched, committer).await {
                    Ok(range_progress) => {
                        merge_progress(&mut progress, range_progress);
                        log_commit_progress(&progress, range_progress);
                    }
                    Err(error @ BackfillError::FetchedSuffixInvalidated { refetch_from, .. }) => {
                        record_partial_commit_before_invalidation(
                            &mut progress,
                            before_push_next_height,
                            coordinator.next_height(),
                        );
                        log_commit_progress(
                            &progress,
                            CommitProgress {
                                committed_ranges: 1,
                                committed_blocks: coordinator.next_height()
                                    - before_push_next_height,
                                committed_logs: 0,
                                last_committed_height: Some(coordinator.next_height() - 1),
                            },
                        );
                        join_set.abort_all();
                        while join_set.join_next().await.is_some() {}
                        in_flight = 0;
                        queued.clear();
                        enqueue_chunks_front(
                            &mut queued,
                            refetch_from,
                            full_range.to_block,
                            sizer.current_blocks(),
                        );
                        next_unallocated = full_range.to_block.saturating_add(1);
                        info!(refetch_from, error = %error, "discarded invalidated backfill suffix");
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Err(FetchRangeError::ProviderLimit { range, source }) => {
                sizer.record_provider_limit();
                enqueue_chunks_front(
                    &mut queued,
                    range.from_block,
                    range.to_block,
                    sizer.current_blocks(),
                );
                info!(
                    from_block = range.from_block,
                    to_block = range.to_block,
                    next_log_blocks = sizer.current_blocks(),
                    error = %source,
                    "provider limited eth_getLogs range; retrying smaller chunks"
                );
            }
            Err(error) => return Err(error.into()),
        }

        fill_fetch_window(
            &mut join_set,
            &client,
            &config,
            &mut queued,
            &mut next_unallocated,
            full_range.to_block,
            &sizer,
            &mut in_flight,
        );
    }

    if !coordinator.is_complete() {
        bail!(
            "backfill ended before ordered coordinator completed; next expected height {}",
            coordinator.next_height()
        );
    }

    Ok(progress)
}

fn fill_fetch_window(
    join_set: &mut JoinSet<Result<FetchedRange<IndexedBlock>, FetchRangeError>>,
    client: &RpcClient,
    config: &AdaptiveFetchConfig,
    queued: &mut VecDeque<BackfillRange>,
    next_unallocated: &mut u64,
    target_block: u64,
    sizer: &LogRangeSizer,
    in_flight: &mut usize,
) {
    while *in_flight < config.workers {
        let range = if let Some(range) = queued.pop_front() {
            Some(range)
        } else {
            sizer
                .next_range(*next_unallocated, target_block)
                .inspect(|range| {
                    *next_unallocated = range.to_block.saturating_add(1);
                })
        };
        let Some(range) = range else {
            break;
        };
        spawn_fetch(join_set, client.clone(), range, config.clone());
        *in_flight += 1;
    }
}

fn spawn_fetch(
    join_set: &mut JoinSet<Result<FetchedRange<IndexedBlock>, FetchRangeError>>,
    client: RpcClient,
    range: BackfillRange,
    config: AdaptiveFetchConfig,
) {
    join_set.spawn(async move { fetch_range_with_retry(client, range, config).await });
}

async fn fetch_range_with_retry(
    client: RpcClient,
    range: BackfillRange,
    config: AdaptiveFetchConfig,
) -> Result<FetchedRange<IndexedBlock>, FetchRangeError> {
    let logs = fetch_logs_with_retry(&client, range, &config).await?;
    let mut logs_by_height = logs_by_height(logs);
    let mut headers = Vec::with_capacity(range.len() as usize);
    let mut blocks = fetch_blocks_by_number_with_retry(&client, range, &config).await?;
    for block in &mut blocks {
        let header = block.header;
        let height = header.height;
        let logs = logs_by_height.remove(&height).unwrap_or_default();
        for log in &logs {
            if log.block_hash != header.hash {
                return Err(FetchRangeError::LogBlockHashMismatch { height });
            }
        }
        block.logs = logs.into_iter().map(|log| log.raw).collect();
        headers.push(header);
    }
    FetchedRange::new(range, headers, blocks).map_err(FetchRangeError::Backfill)
}

async fn fetch_logs_with_retry(
    client: &RpcClient,
    range: BackfillRange,
    config: &AdaptiveFetchConfig,
) -> Result<Vec<AnchoredRawLog>, FetchRangeError> {
    let mut attempt = 1;
    loop {
        config.budget.record(RpcMethod::GetLogs, 0)?;
        match tokio::time::timeout(
            config.rpc_timeout,
            client.fetch_anchored_logs(range, config.filter),
        )
        .await
        {
            Ok(Ok(logs)) => return Ok(logs),
            Ok(Err(error)) => {
                let failure = classify_rpc_error(&error);
                if matches!(failure, RpcFailure::ProviderLimit) {
                    return Err(FetchRangeError::ProviderLimit {
                        range,
                        source: error,
                    });
                }
                match config.retry_policy.decision(failure, attempt) {
                    RetryDecision::RetryAfter(delay) => {
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                    }
                    RetryDecision::GiveUp => {
                        return Err(FetchRangeError::Rpc {
                            range,
                            source: error,
                        });
                    }
                }
            }
            Err(_) => match config.retry_policy.decision(RpcFailure::Timeout, attempt) {
                RetryDecision::RetryAfter(delay) => {
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                RetryDecision::GiveUp => {
                    return Err(FetchRangeError::Timeout {
                        range,
                        phase: "eth_getLogs",
                    });
                }
            },
        }
    }
}

async fn fetch_blocks_by_number_with_retry(
    client: &RpcClient,
    range: BackfillRange,
    config: &AdaptiveFetchConfig,
) -> Result<Vec<IndexedBlock>, FetchRangeError> {
    let mut attempt = 1;
    loop {
        for _ in range.from_block..=range.to_block {
            config.budget.record(RpcMethod::GetBlockByNumber, 0)?;
        }
        match tokio::time::timeout(config.rpc_timeout, client.fetch_blocks_by_number(range)).await {
            Ok(Ok(blocks)) => return Ok(blocks),
            Ok(Err(error)) => match config
                .retry_policy
                .decision(classify_rpc_error(&error), attempt)
            {
                RetryDecision::RetryAfter(delay) => {
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                RetryDecision::GiveUp => {
                    return Err(FetchRangeError::Rpc {
                        range,
                        source: error,
                    });
                }
            },
            Err(_) => match config.retry_policy.decision(RpcFailure::Timeout, attempt) {
                RetryDecision::RetryAfter(delay) => {
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                RetryDecision::GiveUp => {
                    return Err(FetchRangeError::Timeout {
                        range,
                        phase: "eth_getBlockByNumber",
                    });
                }
            },
        }
    }
}

async fn fetch_reference_range_with_retry(
    client: &RpcClient,
    range: BackfillRange,
    filter: Option<ContractLogFilter>,
    retry_policy: RetryPolicy,
    rpc_timeout: Duration,
    budget: &SharedRpcBudget,
) -> Result<Vec<chainweave_sink::NormalizedLogRecord>> {
    let config = AdaptiveFetchConfig {
        workers: 1,
        retry_policy,
        rpc_timeout,
        filter,
        budget: budget.clone(),
    };
    let logs = fetch_logs_with_retry(client, range, &config).await?;
    Ok(logs
        .into_iter()
        .map(|log| chainweave_sink::NormalizedLogRecord {
            block_hash: log.block_hash,
            tx_hash: log.raw.tx_hash,
            log_index: log.raw.log_index,
            address: log.raw.address,
            topics: log.raw.topics,
            data: log.raw.data,
        })
        .collect())
}

fn logs_by_height(logs: Vec<AnchoredRawLog>) -> BTreeMap<u64, Vec<AnchoredRawLog>> {
    let mut grouped = BTreeMap::new();
    for log in logs {
        grouped
            .entry(log.block_number)
            .or_insert_with(Vec::new)
            .push(log);
    }
    grouped
}

fn merge_progress(progress: &mut CommitProgress, range_progress: CommitProgress) {
    progress.committed_ranges += range_progress.committed_ranges;
    progress.committed_blocks += range_progress.committed_blocks;
    progress.committed_logs += range_progress.committed_logs;
    progress.last_committed_height = range_progress
        .last_committed_height
        .or(progress.last_committed_height);
}

fn log_commit_progress(progress: &CommitProgress, range_progress: CommitProgress) {
    if range_progress.committed_blocks == 0 {
        return;
    }
    info!(
        committed_blocks = progress.committed_blocks,
        committed_ranges = progress.committed_ranges,
        committed_items = progress.committed_logs,
        last_committed_height = progress.last_committed_height,
        "backfill commit progress"
    );
}

fn record_partial_commit_before_invalidation(
    progress: &mut CommitProgress,
    before_next_height: u64,
    after_next_height: u64,
) {
    if after_next_height <= before_next_height {
        return;
    }
    progress.committed_ranges += 1;
    progress.committed_blocks += after_next_height - before_next_height;
    progress.last_committed_height = Some(after_next_height - 1);
}

fn enqueue_chunks_front(
    queued: &mut VecDeque<BackfillRange>,
    from_block: u64,
    to_block: u64,
    chunk_size: u64,
) {
    if from_block > to_block {
        return;
    }
    let mut chunks = Vec::new();
    let mut from = from_block;
    while from <= to_block {
        let to = from
            .saturating_add(chunk_size.saturating_sub(1))
            .min(to_block);
        chunks.push(BackfillRange {
            from_block: from,
            to_block: to,
        });
        if to == u64::MAX {
            break;
        }
        from = to + 1;
    }
    for chunk in chunks.into_iter().rev() {
        queued.push_front(chunk);
    }
}

impl SharedRpcBudget {
    fn new(budget: RpcBudget) -> Self {
        Self {
            inner: Arc::new(Mutex::new(budget)),
        }
    }

    fn record(&self, method: RpcMethod, cost_units: u64) -> Result<(), FetchRangeError> {
        self.inner
            .lock()
            .map_err(|_| FetchRangeError::BudgetPoisoned)?
            .record(method, cost_units)?;
        Ok(())
    }

    fn usage(&self) -> Result<RpcUsage> {
        let budget = self
            .inner
            .lock()
            .map_err(|_| anyhow!("RPC budget mutex poisoned"))?;
        Ok(RpcUsage {
            requests: budget.requests(),
            cost_units: budget.cost_units(),
        })
    }
}

fn init_tracing() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize tracing: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use axum::{Json, Router, extract::State, routing::post};
    use chainweave_core::BlockHeader;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    #[derive(Debug, Default)]
    struct RecordingCommitter {
        committed: Vec<FetchedRange<IndexedBlock>>,
    }

    impl AsyncRangeCommitSink<IndexedBlock> for RecordingCommitter {
        type Error = String;

        async fn commit_range(
            &mut self,
            fetched: FetchedRange<IndexedBlock>,
        ) -> Result<(), Self::Error> {
            self.committed.push(fetched);
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct FixtureState {
        log_calls: Arc<AtomicUsize>,
        new_fork: Arc<AtomicBool>,
    }

    #[test]
    fn cli_identity_and_rpc_override_loaded_configuration() {
        let cli = Cli::try_parse_from([
            "chainweave",
            "--rpc-url",
            "wss://rpc.example.com/ws",
            "--expected-chain-id",
            "11155111",
            "--expected-genesis-hash",
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "head",
        ])
        .unwrap();
        let mut config = AppConfig::default();

        apply_cli_overrides(&mut config, &cli).unwrap();

        assert_eq!(config.rpc.primary_url.as_str(), "wss://rpc.example.com/ws");
        assert_eq!(config.expected_chain.unwrap().chain_id, 11_155_111);
    }

    #[test]
    fn cli_parses_backfill_block_range() {
        let cli = Cli::try_parse_from([
            "chainweave",
            "--rpc-url",
            "http://127.0.0.1:8545",
            "backfill",
            "--from-block",
            "100",
            "--to-block",
            "200",
        ])
        .unwrap();

        let Command::Backfill {
            from_block,
            to_block,
            ..
        } = cli.command
        else {
            panic!("expected backfill command");
        };
        assert_eq!(from_block, 100);
        assert_eq!(to_block, 200);
    }

    #[tokio::test]
    async fn adaptive_execution_retries_429_and_shrinks_provider_limited_ranges() {
        let state = FixtureState {
            log_calls: Arc::new(AtomicUsize::new(0)),
            new_fork: Arc::new(AtomicBool::new(false)),
        };
        let url = spawn_fixture_rpc(state.clone(), FixtureMode::RateLimit).await;
        let client = RpcClient::connect(&url).await.unwrap();
        let budget = SharedRpcBudget::new(RpcBudget::new(32, 128).unwrap());
        let config = AdaptiveFetchConfig {
            workers: 2,
            retry_policy: RetryPolicy::new(
                3,
                Duration::from_millis(1),
                Duration::from_millis(5),
                Duration::ZERO,
            )
            .unwrap(),
            rpc_timeout: Duration::from_secs(1),
            filter: None,
            budget,
        };
        let full_range = BackfillRange::new(1, 4).unwrap();
        let sizer = LogRangeSizer::new(4, 2, 4).unwrap();
        let mut coordinator =
            OrderedCommitCoordinator::new(1, 4, Some(test_header(0, 0, 0))).unwrap();
        let mut committer = RecordingCommitter::default();

        let progress = fetch_and_commit_ranges(
            client,
            full_range,
            sizer,
            config,
            &mut coordinator,
            &mut committer,
        )
        .await
        .unwrap();

        assert_eq!(progress.committed_blocks, 4);
        assert_eq!(
            committed_ranges(&committer),
            vec![
                BackfillRange::new(1, 2).unwrap(),
                BackfillRange::new(3, 4).unwrap()
            ]
        );
        assert!(state.log_calls.load(Ordering::SeqCst) >= 4);
    }

    #[tokio::test]
    async fn concurrent_fetch_discards_and_refetches_invalidated_suffix() {
        let state = FixtureState {
            log_calls: Arc::new(AtomicUsize::new(0)),
            new_fork: Arc::new(AtomicBool::new(false)),
        };
        let url = spawn_fixture_rpc(state, FixtureMode::ConcurrentReorg).await;
        let client = RpcClient::connect(&url).await.unwrap();
        let budget = SharedRpcBudget::new(RpcBudget::new(32, 128).unwrap());
        let config = AdaptiveFetchConfig {
            workers: 2,
            retry_policy: RetryPolicy::new(
                2,
                Duration::from_millis(1),
                Duration::from_millis(5),
                Duration::ZERO,
            )
            .unwrap(),
            rpc_timeout: Duration::from_secs(1),
            filter: None,
            budget,
        };
        let full_range = BackfillRange::new(1, 4).unwrap();
        let sizer = LogRangeSizer::new(2, 1, 2).unwrap();
        let mut coordinator =
            OrderedCommitCoordinator::new(1, 4, Some(test_header(0, 0, 0))).unwrap();
        let mut committer = RecordingCommitter::default();

        let progress = fetch_and_commit_ranges(
            client,
            full_range,
            sizer,
            config,
            &mut coordinator,
            &mut committer,
        )
        .await
        .unwrap();

        assert_eq!(progress.committed_blocks, 4);
        assert_eq!(
            committed_ranges(&committer),
            vec![
                BackfillRange::new(1, 2).unwrap(),
                BackfillRange::new(3, 4).unwrap()
            ]
        );
        let committed_hashes = committer
            .committed
            .iter()
            .flat_map(|range| range.headers.iter().map(|header| header.hash[0]))
            .collect::<Vec<_>>();
        assert_eq!(committed_hashes, vec![1, 0xa2, 0xa3, 0xa4]);
    }

    #[derive(Debug, Clone, Copy)]
    enum FixtureMode {
        RateLimit,
        ConcurrentReorg,
    }

    async fn spawn_fixture_rpc(state: FixtureState, mode: FixtureMode) -> Url {
        let app = Router::new()
            .route("/", post(fixture_rpc))
            .with_state((state, mode));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Url::parse(&format!("http://{address}")).unwrap()
    }

    async fn fixture_rpc(
        State((state, mode)): State<(FixtureState, FixtureMode)>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
        if let Some(batch) = request.as_array() {
            let mut responses = Vec::with_capacity(batch.len());
            for request in batch {
                responses.push(fixture_response(state.clone(), mode, request).await);
            }
            return Json(Value::Array(responses));
        }
        Json(fixture_response(state, mode, &request).await)
    }

    async fn fixture_response(state: FixtureState, mode: FixtureMode, request: &Value) -> Value {
        let method = request["method"].as_str().unwrap();
        match method {
            "eth_getLogs" => logs_response(state, mode, request).await,
            "eth_getBlockByNumber" => {
                let height = quantity_to_u64(request["params"][0].as_str().unwrap());
                success(request, block_response(state, mode, height))
            }
            other => panic!("unexpected fixture RPC method: {other}"),
        }
    }

    async fn logs_response(state: FixtureState, mode: FixtureMode, request: &Value) -> Value {
        let filter = &request["params"][0];
        let from = quantity_to_u64(filter["fromBlock"].as_str().unwrap());
        let to = quantity_to_u64(filter["toBlock"].as_str().unwrap());
        let call = state.log_calls.fetch_add(1, Ordering::SeqCst) + 1;
        match mode {
            FixtureMode::RateLimit if call == 1 => json!({
                "jsonrpc": "2.0",
                "id": request["id"],
                "error": {"code": -32005, "message": "429 too many requests"},
            }),
            FixtureMode::RateLimit if to.saturating_sub(from).saturating_add(1) > 2 => {
                json!({
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "error": {"code": -32005, "message": "query returned too many results"},
                })
            }
            FixtureMode::ConcurrentReorg if from == 1 => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                success(request, json!([]))
            }
            _ => success(request, json!([])),
        }
    }

    fn block_response(state: FixtureState, mode: FixtureMode, height: u64) -> Value {
        let replacement = match mode {
            FixtureMode::ConcurrentReorg => {
                if height == 2 {
                    state.new_fork.store(true, Ordering::SeqCst);
                    true
                } else {
                    state.new_fork.load(Ordering::SeqCst)
                }
            }
            FixtureMode::RateLimit => false,
        };
        let hash = match (replacement, height) {
            (true, 2) => 0xa2,
            (true, 3) => 0xa3,
            (true, 4) => 0xa4,
            (_, value) => u8::try_from(value).unwrap(),
        };
        let parent = match (replacement, height) {
            (_, 0) => 0,
            (true, 3) => 0xa2,
            (true, 4) => 0xa3,
            (_, value) => u8::try_from(value - 1).unwrap(),
        };
        json!({
            "number": format!("0x{height:x}"),
            "hash": repeat_hash(hash),
            "parentHash": repeat_hash(parent),
            "timestamp": format!("0x{:x}", 1_700_000_000 + height),
        })
    }

    fn success(request: &Value, result: Value) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": result,
        })
    }

    fn committed_ranges(committer: &RecordingCommitter) -> Vec<BackfillRange> {
        committer
            .committed
            .iter()
            .map(|range| range.range)
            .collect()
    }

    fn test_header(value: u8, parent: u8, height: u64) -> BlockHeader {
        BlockHeader::new([value; 32], [parent; 32], height)
    }

    fn quantity_to_u64(value: &str) -> u64 {
        u64::from_str_radix(value.strip_prefix("0x").unwrap(), 16).unwrap()
    }

    fn repeat_hash(value: u8) -> String {
        format!("0x{}", format!("{value:02x}").repeat(32))
    }
}
