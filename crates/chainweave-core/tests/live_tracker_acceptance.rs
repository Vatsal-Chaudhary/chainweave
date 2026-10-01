use std::collections::BTreeMap;

use chainweave_core::{BlockHash, BlockHeader, RpcMethod};

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
    queue_capacity: usize,
    explicit_start: Option<u64>,
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
    StagePanic(&'static str),
    CrashBeforeCommit,
    CrashAfterCommit,
    ProviderDisagreement { height: u64 },
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
        LiveOutcome::ready_at(block(&chain, 3).header).with_canonical_heights([0, 1, 2, 3]),
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
        LiveOutcome::ready_at(block(&chain, 5).header).with_canonical_heights([0, 1, 2, 3, 4, 5]),
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
        LiveOutcome::ready_at(block(&chain, 23).header).with_apply_order([21, 22, 23]),
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
    let replacement = fork_from(block(&old, 61).header, &[162, 163]);
    let scenario = scenario_with_alt(
        "reorg during catch-up",
        "parent mismatch during catch-up discards uncommitted fetched work and switches to ChainState",
        Some(block(&old, 61).header),
        old,
        replacement.clone(),
        vec![HeadEvent::Notify(block(&replacement, 63).header)],
        vec![LiveFault::ReorgDuringCatchUp { at_height: 62 }],
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&replacement, 63).header).with_apply_order([62, 63]),
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
            .with_log_block_hash_filters([block(&chain, 71).header.hash]),
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
            .with_budget_recovery(),
    );
}

#[test]
fn writer_backpressure_blocks_upstream_without_dropping_data() {
    let chain = linear_blocks(&[120, 121, 122, 123]);
    let scenario = scenario_with_config(
        "writer backpressure",
        "bounded writer saturation applies backpressure and preserves every queued block",
        Some(block(&chain, 120).header),
        chain.clone(),
        BTreeMap::new(),
        vec![HeadEvent::Notify(block(&chain, 123).header)],
        vec![LiveFault::StallWriter],
        LiveTestConfig {
            queue_capacity: 1,
            ..LiveTestConfig::default()
        },
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 123).header).with_apply_order([121, 122, 123]),
    );
}

#[test]
fn stage_panic_cancels_pipeline_and_recovers_from_checkpoint() {
    let chain = linear_blocks(&[130, 131, 132]);
    let scenario = scenario(
        "stage panic",
        "supervision cancels a panicked stage and restart reconciles from the durable checkpoint",
        Some(block(&chain, 130).header),
        chain.clone(),
        vec![HeadEvent::Notify(block(&chain, 132).header)],
        vec![LiveFault::StagePanic("coordinate")],
    );

    expect_live_outcome(
        scenario,
        LiveOutcome::ready_at(block(&chain, 132).header)
            .with_apply_order([131, 132])
            .with_panics(1),
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

fn expect_live_outcome(scenario: LiveScenario, expected: LiveOutcome) {
    let invariant = scenario.invariant;
    let actual = run_live_tracker(scenario);
    assert_eq!(actual, Ok(expected), "{invariant}");
}

fn run_live_tracker(scenario: LiveScenario) -> Result<LiveOutcome, LiveHarnessError> {
    let _ = consume_scenario(&scenario);
    Err(LiveHarnessError::MissingLiveTracker {
        scenario: scenario.name,
    })
}

fn consume_scenario(scenario: &LiveScenario) -> usize {
    let mut touched = scenario.name.len()
        + scenario.invariant.len()
        + scenario.rpc.primary.len()
        + scenario.rpc.alternate.len()
        + scenario.rpc.verifier.len()
        + scenario.heads.events.len()
        + scenario.faults.len()
        + scenario.config.queue_capacity
        + scenario.config.max_reorg_depth as usize
        + scenario.config.live_budget_cost_units_per_minute as usize
        + scenario.config.poll_interval_seconds as usize
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
            | LiveFault::ProviderDisagreement { height } => *height as usize,
            LiveFault::UnknownParentRequiresHash(hash) => usize::from(hash[0]),
            LiveFault::TimeoutOnce(method) | LiveFault::RateLimitOnce(method) => {
                method.default_cost() as usize
            }
            LiveFault::StagePanic(stage) => stage.len(),
            LiveFault::ExhaustLiveBudget
            | LiveFault::StallWriter
            | LiveFault::CrashBeforeCommit
            | LiveFault::CrashAfterCommit => 1,
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
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let parent_hash_value = if index == 0 {
                anchor.hash[0].into()
            } else {
                values[index - 1]
            };
            fake_block(*value, parent_hash_value, anchor.height + index as u64 + 1)
        })
        .map(|block| (block.header.height, block))
        .collect()
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
            queue_capacity: 4,
            explicit_start: Some(0),
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
}
