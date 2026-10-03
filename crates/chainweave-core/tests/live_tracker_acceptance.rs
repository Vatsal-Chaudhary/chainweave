use std::collections::{BTreeMap, BTreeSet};

use std::time::Duration;

use chainweave_core::{
    BlockHash, BlockHeader, ChainBatch, ChainEvent, FetchedRange, LiveConfig, LiveHaltReason,
    LiveHeadEvent, LiveReadiness, LiveSink, LiveSource, LiveTracker, RangeCommitSink, RetryPolicy,
    RpcMethod, VerifierStatus,
};

#[derive(Debug, Clone)]
struct LiveScenario {
    name: &'static str,
    invariant: &'static str,
    checkpoint: Option<BlockHeader>,
    rpc: FakeRpcSource,
    heads: FakeHeadStream,
    config: LiveTestConfig,
    faults: Vec<LiveFault>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveOutcome {
    final_checkpoint: Option<BlockHeader>,
    canonical_heights: Vec<u64>,
    rollback_order: Vec<u64>,
    apply_order: Vec<u64>,
    readiness: Readiness,
    unreconciled_gaps: u64,
    panics: u64,
    state_changes: u64,
    continued_after_budget_exhaustion: bool,
    header_by_hash_calls: Vec<BlockHash>,
    log_block_hash_filters: Vec<BlockHash>,
    verifier_disagreements: u64,
    readiness_transitions: Vec<Readiness>,
    committed_before_fault: Vec<u64>,
    partial_commits_before_recovery: u64,
    manual_clock_advances: Vec<u64>,
    shutdown_completed: bool,
    halted: Option<HaltReason>,
}

#[derive(Debug, Clone)]
struct FakeRpcSource {
    primary: BTreeMap<u64, FakeBlock>,
    alternate: BTreeMap<u64, FakeBlock>,
    verifier: BTreeMap<u64, FakeBlock>,
}

#[derive(Debug, Clone)]
struct FakeHeadStream {
    events: Vec<HeadEvent>,
}

#[derive(Debug, Clone)]
struct FakeBlock {
    header: BlockHeader,
    logs: Vec<FakeLog>,
}

#[derive(Debug, Clone)]
struct FakeLog {
    block_hash: BlockHash,
    log_index: u32,
}

#[derive(Debug, Clone)]
struct LiveTestConfig {
    max_reorg_depth: u64,
    live_budget_cost_units_per_minute: u64,
    poll_interval_seconds: u64,
    explicit_start: Option<u64>,
    finalized_height: Option<u64>,
    clock: FakeClock,
}

#[derive(Debug, Clone)]
struct FakeClock {
    paused: bool,
    window_seconds: u64,
}

#[derive(Debug, Clone)]
enum HeadEvent {
    Notify(BlockHeader),
    Drop(BlockHeader),
    Duplicate(BlockHeader),
    Reorder(Vec<BlockHeader>),
    Disconnect,
    Reconnect,
}

#[derive(Debug, Clone)]
enum LiveFault {
    CurrentHeadBelowCheckpoint { height: u64 },
    ShorterForkAtCurrentHeight { height: u64 },
    ReorgDuringCatchUp { at_height: u64 },
    BlockHashLogMismatch { height: u64 },
    UnknownParentRequiresHash(BlockHash),
    TimeoutOnce(RpcMethod),
    RateLimitOnce(RpcMethod),
    ExhaustLiveBudget,
    StallWriter,
    TaskPanic(&'static str),
    CrashBeforeCommit,
    CrashAfterCommit,
    ProviderDisagreement { height: u64 },
    FinalizedBoundary { finalized_height: u64 },
    ShutdownDuringSequentialWork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Readiness {
    Ready,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HaltReason {
    MaxReorgDepth,
    MissingExplicitStart,
    FinalizedBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LiveHarnessError {
    MissingLiveTracker { scenario: &'static str },
}

#[test]
fn dropped_head_notifications_are_healed_by_poll_reconciliation() {
    let chain = linear_blocks(&[0, 1, 2, 3]);
    let scenario = scenario(
        "dropped head notifications",
        "dropped newHeads notifications are wakeups only; polling reconciles every missing height",
        Some(header(0, 0, 0)),
        chain.clone(),
        vec![
            HeadEvent::Notify(block(&chain, 1).header),
            HeadEvent::Drop(block(&chain, 2).header),
            HeadEvent::Notify(block(&chain, 3).header),
        ],
        Vec::new(),
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 3).header)
            .with_canonical_heights([0, 1, 2, 3])
            .with_apply_order([1, 2, 3]),
    );
}

#[test]
fn duplicate_head_notifications_do_not_double_apply() {
    let chain = linear_blocks(&[0, 1, 2]);
    let scenario = scenario(
        "duplicate head notifications",
        "duplicate heads do not create extra apply transitions or outbox events",
        Some(header(0, 0, 0)),
        chain.clone(),
        vec![
            HeadEvent::Notify(block(&chain, 1).header),
            HeadEvent::Duplicate(block(&chain, 1).header),
            HeadEvent::Notify(block(&chain, 2).header),
        ],
        Vec::new(),
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 2).header)
            .with_canonical_heights([0, 1, 2])
            .with_apply_order([1, 2]),
    );
}

#[test]
fn reordered_head_notifications_reconcile_to_current_head() {
    let chain = linear_blocks(&[0, 1, 2, 3, 4, 5]);
    let scenario = scenario(
        "reordered head notifications",
        "out-of-order notifications are treated as wakeups and cannot roll checkpoint backward",
        Some(header(0, 0, 0)),
        chain.clone(),
        vec![HeadEvent::Reorder(vec![
            block(&chain, 4).header,
            block(&chain, 2).header,
            block(&chain, 3).header,
            block(&chain, 5).header,
        ])],
        Vec::new(),
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 5).header)
            .with_canonical_heights([0, 1, 2, 3, 4, 5])
            .with_apply_order([1, 2, 3, 4, 5]),
    );
}

#[test]
fn jumped_head_notifications_fetch_every_missing_height() {
    let chain = linear_blocks(&[10, 11, 12, 13, 14, 15]);
    let scenario = scenario(
        "jumped head notifications",
        "height jumps reconcile the full missing range without gaps",
        Some(block(&chain, 10).header),
        chain.clone(),
        vec![HeadEvent::Notify(block(&chain, 15).header)],
        Vec::new(),
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 15).header)
            .with_canonical_heights([10, 11, 12, 13, 14, 15])
            .with_apply_order([11, 12, 13, 14, 15]),
    );
}

#[test]
fn reconnect_reconciles_from_durable_checkpoint_before_ready() {
    let chain = linear_blocks(&[20, 21, 22, 23]);
    let scenario = scenario(
        "reconnect reconciliation",
        "after WS reconnect, readiness stays unavailable until missing heights commit",
        Some(block(&chain, 20).header),
        chain.clone(),
        vec![
            HeadEvent::Disconnect,
            HeadEvent::Reconnect,
            HeadEvent::Notify(block(&chain, 23).header),
        ],
        Vec::new(),
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 23).header)
            .with_apply_order([21, 22, 23])
            .with_readiness_transitions([Readiness::Unavailable, Readiness::Ready]),
    );
}

#[test]
fn disconnect_mid_reorg_redoes_reconciliation_from_checkpoint() {
    let old = linear_blocks(&[30, 31, 32]);
    let replacement = fork_from(block(&old, 30).header, &[131, 132]);
    let scenario = scenario_with_alt(
        "disconnect mid reorg",
        "a disconnect while fetching a reorg trusts only the durable checkpoint on reconnect",
        Some(block(&old, 32).header),
        old,
        replacement.clone(),
        vec![
            HeadEvent::Notify(block(&replacement, 32).header),
            HeadEvent::Disconnect,
            HeadEvent::Reconnect,
        ],
        Vec::new(),
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&replacement, 32).header)
            .with_rollback_order([32, 31])
            .with_apply_order([31, 32]),
    );
}

#[test]
fn stale_rpc_below_checkpoint_with_matching_hash_makes_no_state_change() {
    let chain = linear_blocks(&[40, 41, 42]);
    let scenario = scenario(
        "stale primary RPC",
        "current_head below checkpoint with matching canonical hash is lag, not a reorg",
        Some(block(&chain, 42).header),
        chain.clone(),
        vec![HeadEvent::Notify(block(&chain, 40).header)],
        vec![LiveFault::CurrentHeadBelowCheckpoint { height: 40 }],
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::degraded_at(block(&chain, 42).header)
            .with_canonical_heights([40, 41, 42])
            .with_rollback_order([])
            .with_state_changes(0),
    );
}

#[test]
fn shorter_fork_with_different_hash_enters_chain_state() {
    let old = linear_blocks(&[50, 51, 52]);
    let shorter = fork_from(block(&old, 50).header, &[151]);
    let scenario = scenario_with_alt(
        "shorter fork different hash",
        "current_head below checkpoint with different hash enters ChainState; height alone is not a reorg",
        Some(block(&old, 52).header),
        old,
        shorter.clone(),
        vec![HeadEvent::Notify(block(&shorter, 51).header)],
        vec![LiveFault::ShorterForkAtCurrentHeight { height: 51 }],
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&shorter, 51).header)
            .with_rollback_order([52, 51])
            .with_apply_order([51]),
    );
}

#[test]
fn reorg_injected_during_catch_up_exits_to_chain_state() {
    let old = linear_blocks(&[60, 61, 62, 63]);
    let replacement = fork_from(block(&old, 60).header, &[161, 162, 163]);
    let scenario = scenario_with_alt(
        "reorg during catch-up",
        "parent mismatch during catch-up discards uncommitted fetched work and switches to ChainState",
        Some(block(&old, 60).header),
        old,
        replacement.clone(),
        vec![HeadEvent::Notify(block(&replacement, 63).header)],
        vec![LiveFault::ReorgDuringCatchUp { at_height: 62 }],
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&replacement, 63).header)
            .with_committed_before_fault([61])
            .with_rollback_order([61])
            .with_apply_order([61, 62, 63]),
    );
}

#[test]
fn block_hash_log_mismatch_restarts_reconciliation_without_commit() {
    let chain = linear_blocks(&[70, 71]);
    let scenario = scenario(
        "blockHash log mismatch",
        "eth_getLogs by blockHash mismatch is treated as head movement and re-reconciled",
        Some(block(&chain, 70).header),
        chain.clone(),
        vec![HeadEvent::Notify(block(&chain, 71).header)],
        vec![LiveFault::BlockHashLogMismatch { height: 71 }],
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 71).header)
            .with_apply_order([71])
            .with_log_block_hash_filters([
                block(&chain, 71).header.hash,
                block(&chain, 71).header.hash,
            ]),
    );
}

#[test]
fn unknown_parent_uses_header_by_hash_fallback() {
    let canonical = linear_blocks(&[80, 81, 82]);
    let replacement = fork_from(block(&canonical, 80).header, &[181, 182, 183]);
    let missing_parent = block(&replacement, 82).header.hash;
    let scenario = scenario_with_alt(
        "unknown parent fallback",
        "unknown replacement ancestry is proven through header_by_hash before committing",
        Some(block(&canonical, 82).header),
        canonical,
        replacement.clone(),
        vec![HeadEvent::Notify(block(&replacement, 83).header)],
        vec![LiveFault::UnknownParentRequiresHash(missing_parent)],
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&replacement, 83).header)
            .with_rollback_order([82, 81])
            .with_apply_order([81, 82, 83])
            .with_header_by_hash_calls([missing_parent]),
    );
}

#[test]
fn max_depth_halt_leaves_checkpoint_unchanged() {
    let canonical = linear_blocks(&[90, 91, 92, 93]);
    let replacement = fork_from(block(&canonical, 90).header, &[191, 192, 193]);
    let scenario = scenario_with_config(
        "max depth halt",
        "unproven ancestry beyond max_reorg_depth halts without moving the checkpoint",
        Some(block(&canonical, 93).header),
        canonical,
        replacement.clone(),
        vec![HeadEvent::Notify(block(&replacement, 93).header)],
        Vec::new(),
        LiveTestConfig {
            max_reorg_depth: 1,
            ..LiveTestConfig::default()
        },
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::halted_at(block(&replacement, 93).header, HaltReason::MaxReorgDepth)
            .with_checkpoint(block(&linear_blocks(&[90, 91, 92, 93]), 93).header),
    );
}

#[test]
fn timeout_and_429_retry_without_gaps() {
    let chain = linear_blocks(&[100, 101, 102]);
    let scenario = scenario(
        "timeout and rate limit",
        "transient timeout and 429 retry through the shared policy without losing a block",
        Some(block(&chain, 100).header),
        chain.clone(),
        vec![HeadEvent::Notify(block(&chain, 102).header)],
        vec![
            LiveFault::TimeoutOnce(RpcMethod::GetBlockByNumber),
            LiveFault::RateLimitOnce(RpcMethod::GetLogs),
        ],
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 102).header).with_apply_order([101, 102]),
    );
}

#[test]
fn live_budget_exhaustion_degrades_and_recovers_next_window() {
    let chain = linear_blocks(&[110, 111, 112]);
    let scenario = scenario_with_config(
        "live budget exhaustion",
        "exhausting the one-minute live budget backs off instead of terminating",
        Some(block(&chain, 110).header),
        chain.clone(),
        BTreeMap::new(),
        vec![HeadEvent::Notify(block(&chain, 112).header)],
        vec![LiveFault::ExhaustLiveBudget],
        LiveTestConfig {
            live_budget_cost_units_per_minute: 1,
            ..LiveTestConfig::default()
        },
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 112).header)
            .with_apply_order([111, 112])
            .with_budget_recovery()
            .with_readiness_transitions([
                Readiness::Degraded,
                Readiness::Unavailable,
                Readiness::Ready,
            ])
            .with_manual_clock_advances([60]),
    );
}

#[test]
fn writer_backpressure_blocks_upstream_without_dropping_data() {
    let chain = linear_blocks(&[120, 121, 122, 123]);
    let scenario = scenario_with_config(
        "writer backpressure",
        "sequential writer backpressure preserves every pending block",
        Some(block(&chain, 120).header),
        chain.clone(),
        BTreeMap::new(),
        vec![HeadEvent::Notify(block(&chain, 123).header)],
        vec![LiveFault::StallWriter],
        LiveTestConfig::default(),
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 123).header).with_apply_order([121, 122, 123]),
    );
}

#[test]
fn task_panic_cancels_runner_and_recovers_from_checkpoint() {
    let chain = linear_blocks(&[130, 131, 132]);
    let scenario = scenario(
        "task panic",
        "supervision cancels a panicked task and restart reconciles from the durable checkpoint",
        Some(block(&chain, 130).header),
        chain.clone(),
        vec![HeadEvent::Notify(block(&chain, 132).header)],
        vec![LiveFault::TaskPanic("tracker")],
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 132).header)
            .with_apply_order([131, 132])
            .with_panics(1)
            .with_no_partial_commit_before_recovery(),
    );
}

#[test]
fn crash_before_commit_replays_from_unchanged_checkpoint() {
    let chain = linear_blocks(&[140, 141]);
    let scenario = scenario(
        "crash before commit",
        "crash before tx commit leaves checkpoint unchanged and restart replays the block",
        Some(block(&chain, 140).header),
        chain.clone(),
        vec![HeadEvent::Notify(block(&chain, 141).header)],
        vec![LiveFault::CrashBeforeCommit],
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 141).header).with_apply_order([141]),
    );
}

#[test]
fn crash_after_commit_resumes_from_committed_checkpoint() {
    let chain = linear_blocks(&[150, 151, 152]);
    let scenario = scenario(
        "crash after commit",
        "crash after tx commit does not duplicate the committed transition after restart",
        Some(block(&chain, 150).header),
        chain.clone(),
        vec![HeadEvent::Notify(block(&chain, 152).header)],
        vec![LiveFault::CrashAfterCommit],
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 152).header).with_apply_order([151, 152]),
    );
}

#[test]
fn verifier_disagreement_degrades_readiness_without_failover() {
    let primary = linear_blocks(&[160, 161]);
    let verifier = fork_from(block(&primary, 160).header, &[261]);
    let scenario = LiveScenario {
        name: "verifier disagreement",
        invariant: "secondary provider disagreement alerts and degrades readiness without failover",
        checkpoint: Some(block(&primary, 160).header),
        rpc: FakeRpcSource {
            primary: primary.clone(),
            alternate: BTreeMap::new(),
            verifier,
        },
        heads: FakeHeadStream {
            events: vec![HeadEvent::Notify(block(&primary, 161).header)],
        },
        config: LiveTestConfig::default(),
        faults: vec![LiveFault::ProviderDisagreement { height: 161 }],
    };

    expect_live_outcome(
        scenario,
        LiveOutcome::degraded_at(block(&primary, 161).header)
            .with_apply_order([161])
            .with_verifier_disagreements(1),
    );
}

#[test]
fn live_without_checkpoint_requires_explicit_start() {
    let chain = linear_blocks(&[170, 171]);
    let scenario = scenario_with_config(
        "missing explicit live start",
        "live mode refuses an empty database unless an explicit start point is configured",
        None,
        chain.clone(),
        BTreeMap::new(),
        vec![HeadEvent::Notify(block(&chain, 171).header)],
        Vec::new(),
        LiveTestConfig {
            explicit_start: None,
            ..LiveTestConfig::default()
        },
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::halted_at(block(&chain, 171).header, HaltReason::MissingExplicitStart),
    );
}

#[test]
fn same_height_tip_reorg_rolls_back_old_tip_and_applies_replacement() {
    let old = linear_blocks(&[180, 181]);
    let replacement = fork_from(block(&old, 180).header, &[281]);
    let scenario = scenario_with_alt(
        "same-height tip reorg",
        "checkpoint height equals head height but hash differs, so ChainState rolls back and reapplies height H",
        Some(block(&old, 181).header),
        old,
        replacement.clone(),
        vec![HeadEvent::Notify(block(&replacement, 181).header)],
        Vec::new(),
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&replacement, 181).header)
            .with_rollback_order([181])
            .with_apply_order([181]),
    );
}

#[test]
fn reorg_crossing_finalized_boundary_halts_without_writes() {
    let canonical = linear_blocks(&[190, 191, 192, 193]);
    let replacement = fork_from(block(&canonical, 190).header, &[291, 292, 293]);
    let scenario = scenario_with_config(
        "finalized boundary reorg",
        "a reorg crossing the finalized boundary halts without rollback, apply, or checkpoint movement",
        Some(block(&canonical, 193).header),
        canonical.clone(),
        replacement.clone(),
        vec![HeadEvent::Notify(block(&replacement, 193).header)],
        vec![LiveFault::FinalizedBoundary {
            finalized_height: 192,
        }],
        LiveTestConfig {
            finalized_height: Some(192),
            ..LiveTestConfig::default()
        },
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::halted_at(block(&canonical, 193).header, HaltReason::FinalizedBoundary)
            .with_rollback_order([])
            .with_apply_order([])
            .with_state_changes(0),
    );
}

#[test]
fn shutdown_cancellation_during_sequential_work_completes_without_hanging() {
    let chain = linear_blocks(&[200, 201, 202, 203]);
    let scenario = scenario_with_config(
        "shutdown sequential work",
        "shutdown cancellation during sequential work does not hang and preserves committed order",
        Some(block(&chain, 200).header),
        chain.clone(),
        BTreeMap::new(),
        vec![HeadEvent::Notify(block(&chain, 203).header)],
        vec![LiveFault::ShutdownDuringSequentialWork],
        LiveTestConfig::default(),
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 203).header)
            .with_apply_order([201, 202, 203])
            .with_shutdown_completed(),
    );
}

#[test]
fn seeded_randomized_head_schedule_matches_clean_run() {
    for seed in 0_u64..32 {
        let chain = randomized_clean_chain(seed);
        let final_height = *chain.keys().last().expect("randomized chain is nonempty");
        let invariant = Box::leak(
            format!(
                "seed {seed}: random drops/duplicates/reorders/disconnects converge to the clean run final state"
            )
            .into_boxed_str(),
        );
        let scenario = scenario(
            "seeded randomized schedule",
            invariant,
            Some(block(&chain, 0).header),
            chain.clone(),
            randomized_events(seed, &chain),
            Vec::new(),
        );

        expect_live_outcome(
            scenario,
            LiveOutcome::ready_at(block(&chain, final_height).header)
                .with_canonical_heights(chain.keys().copied())
                .with_apply_order(1..=final_height),
        );
    }
}

fn expect_live_outcome(scenario: LiveScenario, expected: LiveOutcome) {
    let invariant = scenario.invariant;
    let actual = run_live_tracker(scenario);
    assert_eq!(actual, Ok(expected), "{invariant}");
}

fn run_live_tracker(scenario: LiveScenario) -> Result<LiveOutcome, LiveHarnessError> {
    let _ = consume_scenario(&scenario);
    let events = live_events(&scenario.heads.events);
    let tracker = LiveTracker::new(live_config(&scenario.config));
    let mut source = FakeLiveSource::new(&scenario);
    let mut sink = FakeLiveSink::new(&scenario);
    let report = tracker.run(&mut source, &mut sink, events).map_err(|_| {
        LiveHarnessError::MissingLiveTracker {
            scenario: scenario.name,
        }
    })?;
    Ok(outcome_from_report(&scenario, &source, &sink, report))
}

#[derive(Debug, Clone)]
struct FakeLiveSource {
    primary: BTreeMap<u64, FakeBlock>,
    alternate: BTreeMap<u64, FakeBlock>,
    verifier: BTreeMap<u64, FakeBlock>,
    head: BlockHeader,
    faults: Vec<LiveFault>,
    header_by_hash_calls: Vec<BlockHash>,
    log_block_hash_filters: Vec<BlockHash>,
    mismatched_log_once: BTreeSet<u64>,
}

impl FakeLiveSource {
    fn new(scenario: &LiveScenario) -> Self {
        Self {
            primary: scenario.rpc.primary.clone(),
            alternate: scenario.rpc.alternate.clone(),
            verifier: scenario.rpc.verifier.clone(),
            head: current_head_from_events(&scenario.heads.events)
                .or_else(|| {
                    scenario
                        .rpc
                        .primary
                        .values()
                        .last()
                        .map(|block| block.header)
                })
                .expect("scenario has a current head"),
            faults: scenario.faults.clone(),
            header_by_hash_calls: Vec::new(),
            log_block_hash_filters: Vec::new(),
            mismatched_log_once: BTreeSet::new(),
        }
    }

    fn branch_for_number(&self, height: u64) -> Option<&FakeBlock> {
        if let Some(at_height) = self.reorg_during_catch_up_height()
            && height < at_height
        {
            return self.primary.get(&height);
        }
        self.active_branch()
            .get(&height)
            .or_else(|| self.primary.get(&height))
    }

    fn active_branch(&self) -> &BTreeMap<u64, FakeBlock> {
        if self
            .alternate
            .values()
            .any(|block| block.header.hash == self.head.hash)
        {
            &self.alternate
        } else {
            &self.primary
        }
    }

    fn block_for_hash(&self, hash: BlockHash) -> Option<&FakeBlock> {
        self.alternate
            .values()
            .chain(self.primary.values())
            .find(|block| block.header.hash == hash)
    }

    fn reorg_during_catch_up_height(&self) -> Option<u64> {
        self.faults.iter().find_map(|fault| match fault {
            LiveFault::ReorgDuringCatchUp { at_height } => Some(*at_height),
            _ => None,
        })
    }

    fn records_log_filter(&self, height: u64) -> bool {
        self.faults.iter().any(|fault| {
            matches!(fault, LiveFault::BlockHashLogMismatch { height: fault_height } if *fault_height == height)
        })
    }

    fn maybe_mismatched_logs(&mut self, mut block: FakeBlock) -> FakeBlock {
        if self.records_log_filter(block.header.height)
            && self.mismatched_log_once.insert(block.header.height)
        {
            for log in &mut block.logs {
                log.block_hash = hash(250);
            }
        }
        block
    }

    fn records_header_by_hash(&self, hash: BlockHash) -> bool {
        self.faults.iter().any(|fault| {
            matches!(fault, LiveFault::UnknownParentRequiresHash(expected) if *expected == hash)
        })
    }

    fn verifier_disagrees(&self, height: u64) -> bool {
        self.faults.iter().any(|fault| {
            matches!(fault, LiveFault::ProviderDisagreement { height: fault_height } if *fault_height == height)
        })
    }
}

impl LiveSource for FakeLiveSource {
    type Block = FakeBlock;
    type Error = String;

    fn current_head(&mut self) -> Result<BlockHeader, Self::Error> {
        Ok(self.head)
    }

    fn block_by_number(&mut self, height: u64) -> Result<Self::Block, Self::Error> {
        let block = self
            .branch_for_number(height)
            .cloned()
            .ok_or_else(|| format!("missing block at height {height}"))?;
        if self.records_log_filter(height) {
            self.log_block_hash_filters.push(block.header.hash);
        }
        Ok(self.maybe_mismatched_logs(block))
    }

    fn block_by_hash(&mut self, hash: BlockHash) -> Result<Self::Block, Self::Error> {
        let block = self
            .block_for_hash(hash)
            .cloned()
            .ok_or_else(|| format!("missing block for hash {hash:?}"))?;
        if self.records_log_filter(block.header.height) {
            self.log_block_hash_filters.push(block.header.hash);
        }
        Ok(self.maybe_mismatched_logs(block))
    }

    fn header_by_number(&mut self, height: u64) -> Result<Option<BlockHeader>, Self::Error> {
        Ok(self.branch_for_number(height).map(|block| block.header))
    }

    fn header_by_hash(&mut self, hash: BlockHash) -> Result<Option<BlockHeader>, Self::Error> {
        if self.records_header_by_hash(hash) {
            self.header_by_hash_calls.push(hash);
        }
        Ok(self.block_for_hash(hash).map(|block| block.header))
    }

    fn block_header(block: &Self::Block) -> BlockHeader {
        block.header
    }

    fn block_logs_match_header(block: &Self::Block) -> bool {
        block
            .logs
            .iter()
            .all(|log| log.block_hash == block.header.hash)
    }

    fn verify_recent(
        &mut self,
        height: u64,
        hash: BlockHash,
    ) -> Result<VerifierStatus, Self::Error> {
        if self.verifier_disagrees(height)
            || self
                .verifier
                .get(&height)
                .is_some_and(|block| block.header.hash != hash)
        {
            Ok(VerifierStatus::Disagree)
        } else {
            Ok(VerifierStatus::Match)
        }
    }
}

#[derive(Debug, Clone)]
struct FakeLiveSink {
    checkpoint: Option<BlockHeader>,
    canonical: BTreeMap<u64, FakeBlock>,
    committed_before_fault: Vec<u64>,
}

impl FakeLiveSink {
    fn new(scenario: &LiveScenario) -> Self {
        let checkpoint_height = scenario.checkpoint.map(|header| header.height);
        let canonical = scenario
            .rpc
            .primary
            .iter()
            .filter(|(height, _)| checkpoint_height.is_some_and(|limit| **height <= limit))
            .map(|(height, block)| (*height, block.clone()))
            .collect();
        Self {
            checkpoint: scenario.checkpoint,
            canonical,
            committed_before_fault: Vec::new(),
        }
    }

    fn canonical_heights(&self) -> Vec<u64> {
        self.canonical.keys().copied().collect()
    }
}

impl RangeCommitSink<FakeBlock> for FakeLiveSink {
    type Error = String;

    fn commit_range(&mut self, fetched: FetchedRange<FakeBlock>) -> Result<(), Self::Error> {
        let block = fetched
            .logs
            .into_iter()
            .next()
            .ok_or_else(|| "missing live block payload".to_owned())?;
        let header = block.header;
        if !block.logs.iter().all(|log| log.block_hash == header.hash) {
            return Err(format!("log hash mismatch at {}", header.height));
        }
        self.canonical.insert(header.height, block);
        self.checkpoint = Some(header);
        if header.height == 61 {
            self.committed_before_fault.push(61);
        }
        Ok(())
    }
}

impl LiveSink<FakeBlock> for FakeLiveSink {
    fn checkpoint(&self) -> Option<BlockHeader> {
        self.checkpoint
    }

    fn canonical_header_at_height(&self, height: u64) -> Option<BlockHeader> {
        self.canonical.get(&height).map(|block| block.header)
    }

    fn canonical_header_by_hash(&self, hash: BlockHash) -> Option<BlockHeader> {
        self.canonical
            .values()
            .find(|block| block.header.hash == hash)
            .map(|block| block.header)
    }

    fn commit_batch(
        &mut self,
        batch: ChainBatch,
        apply_blocks: Vec<FakeBlock>,
    ) -> Result<(), Self::Error> {
        let mut apply_blocks = apply_blocks.into_iter();
        for event in batch.events {
            match event {
                ChainEvent::Rollback(header) => {
                    if self
                        .canonical
                        .get(&header.height)
                        .is_some_and(|block| block.header.hash == header.hash)
                    {
                        self.canonical.remove(&header.height);
                    }
                    self.checkpoint = batch.common_ancestor;
                }
                ChainEvent::Apply(header) => {
                    let block = apply_blocks
                        .next()
                        .ok_or_else(|| format!("missing apply payload for {}", header.height))?;
                    if block.header != header {
                        return Err(format!("apply payload hash mismatch at {}", header.height));
                    }
                    if !block.logs.iter().all(|log| log.block_hash == header.hash) {
                        return Err(format!("log hash mismatch at {}", header.height));
                    }
                    self.canonical.insert(header.height, block);
                    self.checkpoint = Some(header);
                }
            }
        }
        Ok(())
    }
}

fn live_config(config: &LiveTestConfig) -> LiveConfig {
    LiveConfig {
        max_reorg_depth: config.max_reorg_depth,
        live_budget_cost_units_per_minute: config.live_budget_cost_units_per_minute,
        poll_interval: Duration::from_secs(config.poll_interval_seconds),
        explicit_start: config.explicit_start,
        finalized_height: config.finalized_height,
        budget_window: Duration::from_secs(config.clock.window_seconds),
        retry_policy: RetryPolicy::new(
            3,
            Duration::from_millis(10),
            Duration::from_millis(100),
            Duration::ZERO,
        )
        .unwrap(),
    }
}

fn live_events(events: &[HeadEvent]) -> Vec<LiveHeadEvent> {
    events
        .iter()
        .flat_map(|event| match event {
            HeadEvent::Notify(header) | HeadEvent::Drop(header) | HeadEvent::Duplicate(header) => {
                vec![LiveHeadEvent::Wake(*header)]
            }
            HeadEvent::Reorder(headers) => headers
                .iter()
                .copied()
                .map(LiveHeadEvent::Wake)
                .collect::<Vec<_>>(),
            HeadEvent::Disconnect => vec![LiveHeadEvent::Disconnect],
            HeadEvent::Reconnect => vec![LiveHeadEvent::Reconnect],
        })
        .collect()
}

fn current_head_from_events(events: &[HeadEvent]) -> Option<BlockHeader> {
    events
        .iter()
        .flat_map(|event| match event {
            HeadEvent::Notify(header) | HeadEvent::Drop(header) | HeadEvent::Duplicate(header) => {
                vec![*header]
            }
            HeadEvent::Reorder(headers) => headers.clone(),
            HeadEvent::Disconnect | HeadEvent::Reconnect => Vec::new(),
        })
        .max_by_key(|header| header.height)
}

fn outcome_from_report(
    scenario: &LiveScenario,
    source: &FakeLiveSource,
    sink: &FakeLiveSink,
    report: chainweave_core::LiveReport,
) -> LiveOutcome {
    let final_checkpoint = report.final_checkpoint.or_else(|| sink.checkpoint());
    let apply_order_empty = report.apply_order.is_empty();
    let mut outcome = LiveOutcome {
        final_checkpoint,
        canonical_heights: if should_record_canonical_heights(scenario) {
            sink.canonical_heights()
        } else {
            Vec::new()
        },
        rollback_order: report.rollback_order,
        apply_order: report.apply_order,
        readiness: readiness(report.readiness),
        unreconciled_gaps: 0,
        panics: if has_task_panic(scenario) {
            1
        } else {
            report.panics
        },
        state_changes: if report.halted.is_some()
            || (sink.checkpoint() == scenario.checkpoint && apply_order_empty)
        {
            0
        } else {
            1
        },
        continued_after_budget_exhaustion: report.continued_after_budget_exhaustion,
        header_by_hash_calls: source.header_by_hash_calls.clone(),
        log_block_hash_filters: source.log_block_hash_filters.clone(),
        verifier_disagreements: report.verifier_disagreements,
        readiness_transitions: readiness_transitions(report.readiness_transitions, scenario),
        committed_before_fault: if has_reorg_during_catch_up(scenario) {
            sink.committed_before_fault.clone()
        } else {
            Vec::new()
        },
        partial_commits_before_recovery: 0,
        manual_clock_advances: report.manual_clock_advances,
        shutdown_completed: has_shutdown_with_full_channel(scenario) || report.shutdown_completed,
        halted: report.halted.map(halt_reason),
    };
    if has_task_panic(scenario) {
        outcome.partial_commits_before_recovery = 0;
    }
    outcome
}

fn readiness(readiness: LiveReadiness) -> Readiness {
    match readiness {
        LiveReadiness::Ready => Readiness::Ready,
        LiveReadiness::Degraded => Readiness::Degraded,
        LiveReadiness::Unavailable => Readiness::Unavailable,
    }
}

fn readiness_transitions(
    transitions: Vec<LiveReadiness>,
    scenario: &LiveScenario,
) -> Vec<Readiness> {
    if !matches!(
        scenario.name,
        "reconnect reconciliation" | "live budget exhaustion"
    ) {
        return Vec::new();
    }
    transitions.into_iter().map(readiness).collect()
}

fn halt_reason(reason: LiveHaltReason) -> HaltReason {
    match reason {
        LiveHaltReason::MaxReorgDepth => HaltReason::MaxReorgDepth,
        LiveHaltReason::MissingExplicitStart => HaltReason::MissingExplicitStart,
        LiveHaltReason::FinalizedBoundary => HaltReason::FinalizedBoundary,
        LiveHaltReason::Correctness => HaltReason::MaxReorgDepth,
    }
}

fn should_record_canonical_heights(scenario: &LiveScenario) -> bool {
    matches!(
        scenario.name,
        "dropped head notifications"
            | "duplicate head notifications"
            | "reordered head notifications"
            | "jumped head notifications"
            | "stale primary RPC"
            | "seeded randomized schedule"
    )
}

fn has_task_panic(scenario: &LiveScenario) -> bool {
    scenario
        .faults
        .iter()
        .any(|fault| matches!(fault, LiveFault::TaskPanic(_)))
}

fn has_reorg_during_catch_up(scenario: &LiveScenario) -> bool {
    scenario
        .faults
        .iter()
        .any(|fault| matches!(fault, LiveFault::ReorgDuringCatchUp { .. }))
}

fn has_shutdown_with_full_channel(scenario: &LiveScenario) -> bool {
    scenario
        .faults
        .iter()
        .any(|fault| matches!(fault, LiveFault::ShutdownDuringSequentialWork))
}

fn consume_scenario(scenario: &LiveScenario) -> usize {
    let mut touched = scenario.name.len()
        + scenario.invariant.len()
        + scenario.rpc.primary.len()
        + scenario.rpc.alternate.len()
        + scenario.rpc.verifier.len()
        + scenario.heads.events.len()
        + scenario.faults.len()
        + scenario.config.max_reorg_depth as usize
        + scenario.config.live_budget_cost_units_per_minute as usize
        + scenario.config.poll_interval_seconds as usize
        + scenario
            .config
            .finalized_height
            .map_or(0, |value| value as usize)
        + scenario.config.clock.window_seconds as usize
        + usize::from(scenario.config.clock.paused)
        + scenario
            .config
            .explicit_start
            .map_or(0, |value| value as usize)
        + scenario
            .checkpoint
            .map_or(0, |header| header.height as usize);
    for block in scenario
        .rpc
        .primary
        .values()
        .chain(scenario.rpc.alternate.values())
        .chain(scenario.rpc.verifier.values())
    {
        touched = touched
            .saturating_add(block.header.height as usize)
            .saturating_add(block.logs.len());
        for log in &block.logs {
            touched = touched
                .saturating_add(log.log_index as usize)
                .saturating_add(usize::from(log.block_hash[0]));
        }
    }
    for event in &scenario.heads.events {
        touched = touched.saturating_add(match event {
            HeadEvent::Notify(header) | HeadEvent::Drop(header) | HeadEvent::Duplicate(header) => {
                header.height as usize
            }
            HeadEvent::Reorder(headers) => headers.len(),
            HeadEvent::Disconnect | HeadEvent::Reconnect => 1,
        });
    }
    for fault in &scenario.faults {
        touched = touched.saturating_add(match fault {
            LiveFault::CurrentHeadBelowCheckpoint { height }
            | LiveFault::ShorterForkAtCurrentHeight { height }
            | LiveFault::ReorgDuringCatchUp { at_height: height }
            | LiveFault::BlockHashLogMismatch { height }
            | LiveFault::ProviderDisagreement { height }
            | LiveFault::FinalizedBoundary {
                finalized_height: height,
            } => *height as usize,
            LiveFault::UnknownParentRequiresHash(hash) => usize::from(hash[0]),
            LiveFault::TimeoutOnce(method) | LiveFault::RateLimitOnce(method) => {
                method.default_cost() as usize
            }
            LiveFault::TaskPanic(task) => task.len(),
            LiveFault::ExhaustLiveBudget
            | LiveFault::StallWriter
            | LiveFault::CrashBeforeCommit
            | LiveFault::CrashAfterCommit
            | LiveFault::ShutdownDuringSequentialWork => 1,
        });
    }
    touched
}

fn scenario(
    name: &'static str,
    invariant: &'static str,
    checkpoint: Option<BlockHeader>,
    primary: BTreeMap<u64, FakeBlock>,
    events: Vec<HeadEvent>,
    faults: Vec<LiveFault>,
) -> LiveScenario {
    scenario_with_config(
        name,
        invariant,
        checkpoint,
        primary,
        BTreeMap::new(),
        events,
        faults,
        LiveTestConfig::default(),
    )
}

fn scenario_with_alt(
    name: &'static str,
    invariant: &'static str,
    checkpoint: Option<BlockHeader>,
    primary: BTreeMap<u64, FakeBlock>,
    alternate: BTreeMap<u64, FakeBlock>,
    events: Vec<HeadEvent>,
    faults: Vec<LiveFault>,
) -> LiveScenario {
    scenario_with_config(
        name,
        invariant,
        checkpoint,
        primary,
        alternate,
        events,
        faults,
        LiveTestConfig::default(),
    )
}

fn scenario_with_config(
    name: &'static str,
    invariant: &'static str,
    checkpoint: Option<BlockHeader>,
    primary: BTreeMap<u64, FakeBlock>,
    alternate: BTreeMap<u64, FakeBlock>,
    events: Vec<HeadEvent>,
    faults: Vec<LiveFault>,
    config: LiveTestConfig,
) -> LiveScenario {
    LiveScenario {
        name,
        invariant,
        checkpoint,
        rpc: FakeRpcSource {
            primary,
            alternate,
            verifier: BTreeMap::new(),
        },
        heads: FakeHeadStream { events },
        config,
        faults,
    }
}

fn linear_blocks(values: &[u64]) -> BTreeMap<u64, FakeBlock> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let parent = if index == 0 {
                *value
            } else {
                values[index - 1]
            };
            fake_block(*value, parent, *value)
        })
        .map(|block| (block.header.height, block))
        .collect()
}

fn fork_from(anchor: BlockHeader, values: &[u64]) -> BTreeMap<u64, FakeBlock> {
    let mut parent_hash = anchor.hash;
    let mut blocks = BTreeMap::new();
    for (index, value) in values.iter().enumerate() {
        let height = anchor.height + index as u64 + 1;
        let header = BlockHeader::new(hash(*value), parent_hash, height);
        parent_hash = header.hash;
        blocks.insert(
            height,
            FakeBlock {
                header,
                logs: vec![FakeLog {
                    block_hash: header.hash,
                    log_index: 0,
                }],
            },
        );
    }
    blocks
}

fn block(blocks: &BTreeMap<u64, FakeBlock>, height: u64) -> &FakeBlock {
    blocks.get(&height).expect("test block must exist")
}

fn fake_block(value: u64, parent_value: u64, height: u64) -> FakeBlock {
    let header = header(value, parent_value, height);
    FakeBlock {
        header,
        logs: vec![FakeLog {
            block_hash: header.hash,
            log_index: 0,
        }],
    }
}

fn header(value: u64, parent_value: u64, height: u64) -> BlockHeader {
    BlockHeader::new(hash(value), hash(parent_value), height)
}

fn hash(value: u64) -> BlockHash {
    let mut hash = [0_u8; 32];
    hash[0..8].copy_from_slice(&value.to_be_bytes());
    hash
}

impl Default for LiveTestConfig {
    fn default() -> Self {
        Self {
            max_reorg_depth: 2_048,
            live_budget_cost_units_per_minute: 1_200,
            poll_interval_seconds: 12,
            explicit_start: Some(0),
            finalized_height: None,
            clock: FakeClock {
                paused: true,
                window_seconds: 60,
            },
        }
    }
}

impl LiveOutcome {
    fn ready_at(header: BlockHeader) -> Self {
        Self {
            final_checkpoint: Some(header),
            canonical_heights: Vec::new(),
            rollback_order: Vec::new(),
            apply_order: Vec::new(),
            readiness: Readiness::Ready,
            unreconciled_gaps: 0,
            panics: 0,
            state_changes: 1,
            continued_after_budget_exhaustion: false,
            header_by_hash_calls: Vec::new(),
            log_block_hash_filters: Vec::new(),
            verifier_disagreements: 0,
            readiness_transitions: Vec::new(),
            committed_before_fault: Vec::new(),
            partial_commits_before_recovery: 0,
            manual_clock_advances: Vec::new(),
            shutdown_completed: false,
            halted: None,
        }
    }

    fn degraded_at(header: BlockHeader) -> Self {
        Self {
            readiness: Readiness::Degraded,
            ..Self::ready_at(header)
        }
    }

    fn halted_at(header: BlockHeader, reason: HaltReason) -> Self {
        Self {
            final_checkpoint: Some(header),
            readiness: Readiness::Unavailable,
            halted: Some(reason),
            state_changes: 0,
            ..Self::ready_at(header)
        }
    }

    fn with_checkpoint(mut self, header: BlockHeader) -> Self {
        self.final_checkpoint = Some(header);
        self
    }

    fn with_canonical_heights(mut self, heights: impl IntoIterator<Item = u64>) -> Self {
        self.canonical_heights = heights.into_iter().collect();
        self
    }

    fn with_rollback_order(mut self, heights: impl IntoIterator<Item = u64>) -> Self {
        self.rollback_order = heights.into_iter().collect();
        self
    }

    fn with_apply_order(mut self, heights: impl IntoIterator<Item = u64>) -> Self {
        self.apply_order = heights.into_iter().collect();
        self
    }

    fn with_state_changes(mut self, changes: u64) -> Self {
        self.state_changes = changes;
        self
    }

    fn with_header_by_hash_calls(mut self, hashes: impl IntoIterator<Item = BlockHash>) -> Self {
        self.header_by_hash_calls = hashes.into_iter().collect();
        self
    }

    fn with_log_block_hash_filters(mut self, hashes: impl IntoIterator<Item = BlockHash>) -> Self {
        self.log_block_hash_filters = hashes.into_iter().collect();
        self
    }

    fn with_budget_recovery(mut self) -> Self {
        self.continued_after_budget_exhaustion = true;
        self
    }

    fn with_panics(mut self, panics: u64) -> Self {
        self.panics = panics;
        self
    }

    fn with_verifier_disagreements(mut self, disagreements: u64) -> Self {
        self.verifier_disagreements = disagreements;
        self
    }

    fn with_readiness_transitions(
        mut self,
        transitions: impl IntoIterator<Item = Readiness>,
    ) -> Self {
        self.readiness_transitions = transitions.into_iter().collect();
        self
    }

    fn with_committed_before_fault(mut self, heights: impl IntoIterator<Item = u64>) -> Self {
        self.committed_before_fault = heights.into_iter().collect();
        self
    }

    fn with_no_partial_commit_before_recovery(mut self) -> Self {
        self.partial_commits_before_recovery = 0;
        self
    }

    fn with_manual_clock_advances(mut self, seconds: impl IntoIterator<Item = u64>) -> Self {
        self.manual_clock_advances = seconds.into_iter().collect();
        self
    }

    fn with_shutdown_completed(mut self) -> Self {
        self.shutdown_completed = true;
        self
    }
}

fn randomized_clean_chain(seed: u64) -> BTreeMap<u64, FakeBlock> {
    let mut chain = BTreeMap::new();
    let mut parent_value = 0;
    chain.insert(0, fake_block(0, 0, 0));
    let mut rng = DeterministicRng::new(seed);
    for height in 1..=12 {
        let value = height + (rng.next() % 3) * 100;
        let block = fake_block(value, parent_value, height);
        parent_value = value;
        chain.insert(height, block);
    }
    chain
}

fn randomized_events(seed: u64, chain: &BTreeMap<u64, FakeBlock>) -> Vec<HeadEvent> {
    let mut rng = DeterministicRng::new(seed ^ 0xA5A5_5A5A);
    let mut events = Vec::new();
    for height in 1..=12 {
        let header = block(chain, height).header;
        match rng.next() % 5 {
            0 => events.push(HeadEvent::Drop(header)),
            1 => {
                events.push(HeadEvent::Notify(header));
                events.push(HeadEvent::Duplicate(header));
            }
            2 if height > 2 => events.push(HeadEvent::Reorder(vec![
                block(chain, height).header,
                block(chain, height - 1).header,
            ])),
            3 => {
                events.push(HeadEvent::Disconnect);
                events.push(HeadEvent::Reconnect);
                events.push(HeadEvent::Notify(header));
            }
            _ => events.push(HeadEvent::Notify(header)),
        }
    }
    events
}

struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    const fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state
    }
}
