use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chainweave_core::{
    AppConfig, BackfillRange, ChainIdentity, FetchedRange, LogRangeSizer, OrderedCommitCoordinator,
    RangePlanner, RetryDecision, RetryPolicy, RpcBudget, RpcFailure, RpcMethod, ValidationProfile,
};
use chainweave_rpc::RpcClient;
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
        } => run_backfill(&config, from_block, to_block).await,
    }
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

async fn run_backfill(config: &AppConfig, from_block: u64, to_block: u64) -> Result<()> {
    config
        .validate(ValidationProfile::Workers)
        .context("configuration validation failed")?;
    let database_url = config
        .database_url
        .as_deref()
        .context("database_url is required before running backfill")?;

    let full_range = BackfillRange::new(from_block, to_block)?;
    let retry_policy = RetryPolicy::new(
        3,
        Duration::from_millis(250),
        Duration::from_secs(5),
        Duration::from_millis(250),
    )?;
    let sizer = LogRangeSizer::new(1_000, 100, 1_000)?;
    let planner = RangePlanner::new(sizer.current_blocks(), config.queues.fetch)?;
    let plan = planner.plan(full_range);

    let client = RpcClient::connect(&config.rpc.primary_url).await?;
    let target_head = client.capture_target_head().await?;
    if let Some(expected) = &config.expected_chain {
        RpcClient::verify_identity(&target_head, expected)?;
    }
    if to_block > target_head.number {
        bail!(
            "backfill range end {to_block} is beyond captured target head {}",
            target_head.number
        );
    }

    let mut budget = RpcBudget::new(
        plan.full_range
            .len()
            .saturating_add(plan.request_ranges.len() as u64)
            .saturating_add(8),
        plan.full_range
            .len()
            .saturating_add((plan.request_ranges.len() as u64).saturating_mul(5))
            .saturating_add(32),
    )?;
    for range in &plan.request_ranges {
        budget.record(RpcMethod::GetLogs, 0)?;
        for _ in range.from_block..=range.to_block {
            budget.record(RpcMethod::GetBlockByNumber, 0)?;
        }
    }
    if from_block > 0 {
        budget.record(RpcMethod::GetBlockByNumber, 0)?;
    }

    let writer = PostgresChainWriter::connect(database_url, target_head.chain_id).await?;
    writer.run_migrations().await?;
    writer
        .ensure_chain_identity(target_head.genesis_block_hash())
        .await?;

    let parent_anchor = if from_block == 0 {
        None
    } else {
        Some(client.fetch_header_by_number(from_block - 1).await?)
    };
    let mut coordinator = OrderedCommitCoordinator::new(from_block, to_block, parent_anchor)?;
    let mut committer = PostgresBackfillCommitter::new(writer, parent_anchor);

    info!(
        chain_id = target_head.chain_id,
        captured_height = target_head.number,
        from_block,
        to_block,
        ranges = plan.request_ranges.len(),
        workers = plan.bounded_workers,
        "starting bounded backfill"
    );

    let report = fetch_and_commit_ranges(
        client,
        plan.request_ranges.clone(),
        plan.bounded_workers,
        retry_policy,
        &mut coordinator,
        &mut committer,
    )
    .await?;

    if to_block == target_head.number
        && committer.last_committed_hash() != Some(target_head.block_hash())
    {
        bail!("captured target head changed before commit at height {to_block}");
    }

    println!(
        "backfilled {}..={} through height {}",
        full_range.from_block,
        full_range.to_block,
        report
            .last_committed_height
            .map_or_else(|| "none".to_owned(), |height| height.to_string())
    );
    println!(
        "committed ranges: {}, blocks: {}, RPC budget used: {} requests / {} cost units",
        report.committed_ranges,
        report.committed_blocks,
        budget.requests(),
        budget.cost_units()
    );
    Ok(())
}

async fn fetch_and_commit_ranges(
    client: RpcClient,
    ranges: Vec<BackfillRange>,
    workers: usize,
    retry_policy: RetryPolicy,
    coordinator: &mut OrderedCommitCoordinator<IndexedBlock>,
    committer: &mut PostgresBackfillCommitter,
) -> Result<chainweave_core::CommitProgress> {
    let mut join_set = JoinSet::new();
    let mut ranges = ranges.into_iter();
    let mut in_flight = 0_usize;
    let mut progress = chainweave_core::CommitProgress::default();

    while in_flight < workers {
        let Some(range) = ranges.next() else {
            break;
        };
        spawn_fetch(&mut join_set, client.clone(), range, retry_policy);
        in_flight += 1;
    }

    while let Some(result) = join_set.join_next().await {
        in_flight = in_flight.saturating_sub(1);
        let fetched = result.context("backfill fetch task panicked")??;
        let range_progress = coordinator.push_async(fetched, committer).await?;
        progress.committed_ranges += range_progress.committed_ranges;
        progress.committed_blocks += range_progress.committed_blocks;
        progress.committed_logs += range_progress.committed_logs;
        progress.last_committed_height = range_progress
            .last_committed_height
            .or(progress.last_committed_height);

        if let Some(range) = ranges.next() {
            spawn_fetch(&mut join_set, client.clone(), range, retry_policy);
            in_flight += 1;
        }
    }

    if !coordinator.is_complete() {
        bail!(
            "backfill ended before ordered coordinator completed; next expected height {}",
            coordinator.next_height()
        );
    }

    Ok(progress)
}

fn spawn_fetch(
    join_set: &mut JoinSet<Result<FetchedRange<IndexedBlock>, chainweave_rpc::RpcError>>,
    client: RpcClient,
    range: BackfillRange,
    retry_policy: RetryPolicy,
) {
    join_set.spawn(async move { fetch_range_with_retry(client, range, retry_policy).await });
}

async fn fetch_range_with_retry(
    client: RpcClient,
    range: BackfillRange,
    retry_policy: RetryPolicy,
) -> Result<FetchedRange<IndexedBlock>, chainweave_rpc::RpcError> {
    let mut attempt = 1;
    loop {
        match client.fetch_backfill_range(range).await {
            Ok(fetched) => return Ok(fetched),
            Err(error) => match retry_policy.decision(classify_rpc_error(&error), attempt) {
                RetryDecision::RetryAfter(delay) => {
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                RetryDecision::GiveUp => return Err(error),
            },
        }
    }
}

fn classify_rpc_error(error: &chainweave_rpc::RpcError) -> RpcFailure {
    match error {
        chainweave_rpc::RpcError::Request(message) => {
            let classified = RpcFailure::from_rpc_message(message);
            if matches!(classified, RpcFailure::Permanent) {
                RpcFailure::Transient
            } else {
                classified
            }
        }
        _ => RpcFailure::Permanent,
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
        } = cli.command
        else {
            panic!("expected backfill command");
        };
        assert_eq!(from_block, 100);
        assert_eq!(to_block, 200);
    }
}
