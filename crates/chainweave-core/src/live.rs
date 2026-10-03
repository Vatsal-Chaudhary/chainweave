use std::{marker::PhantomData, time::Duration};

use thiserror::Error;

use crate::{
    AncestryResolver, BackfillError, BackfillRange, BlockHash, BlockHeader, ChainBatch, ChainError,
    ChainEvent, ChainState, FetchedRange, OrderedCommitCoordinator, RangeCommitSink, ResolverError,
    RetryPolicy, RpcBudget, RpcMethod,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveConfig {
    pub max_reorg_depth: u64,
    pub live_budget_cost_units_per_minute: u64,
    pub poll_interval: Duration,
    pub explicit_start: Option<u64>,
    pub finalized_height: Option<u64>,
    pub budget_window: Duration,
    pub retry_policy: RetryPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveReadiness {
    Ready,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveHaltReason {
    MaxReorgDepth,
    MissingExplicitStart,
    FinalizedBoundary,
    Correctness,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveReport {
    pub final_checkpoint: Option<BlockHeader>,
    pub readiness: LiveReadiness,
    pub readiness_transitions: Vec<LiveReadiness>,
    pub rollback_order: Vec<u64>,
    pub apply_order: Vec<u64>,
    pub committed_before_fault: Vec<u64>,
    pub panics: u64,
    pub partial_commits_before_recovery: u64,
    pub continued_after_budget_exhaustion: bool,
    pub manual_clock_advances: Vec<u64>,
    pub verifier_disagreements: u64,
    pub shutdown_completed: bool,
    pub halted: Option<LiveHaltReason>,
}

impl LiveReport {
    #[must_use]
    pub fn new(final_checkpoint: Option<BlockHeader>) -> Self {
        Self {
            final_checkpoint,
            readiness: LiveReadiness::Ready,
            readiness_transitions: Vec::new(),
            rollback_order: Vec::new(),
            apply_order: Vec::new(),
            committed_before_fault: Vec::new(),
            panics: 0,
            partial_commits_before_recovery: 0,
            continued_after_budget_exhaustion: false,
            manual_clock_advances: Vec::new(),
            verifier_disagreements: 0,
            shutdown_completed: false,
            halted: None,
        }
    }

    #[must_use]
    pub fn halted(final_checkpoint: Option<BlockHeader>, reason: LiveHaltReason) -> Self {
        Self {
            readiness: LiveReadiness::Unavailable,
            halted: Some(reason),
            ..Self::new(final_checkpoint)
        }
    }

    fn transition(&mut self, readiness: LiveReadiness) {
        if self.readiness != readiness {
            self.readiness = readiness;
            self.readiness_transitions.push(readiness);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveHeadEvent {
    Wake(BlockHeader),
    Disconnect,
    Reconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifierStatus {
    Match,
    Disagree,
    Unavailable,
}

pub trait LiveSource {
    type Block: Clone;
    type Error: std::fmt::Display;

    fn current_head(&mut self) -> Result<BlockHeader, Self::Error>;
    fn block_by_number(&mut self, height: u64) -> Result<Self::Block, Self::Error>;
    fn block_by_hash(&mut self, hash: BlockHash) -> Result<Self::Block, Self::Error>;
    fn header_by_number(&mut self, height: u64) -> Result<Option<BlockHeader>, Self::Error>;
    fn header_by_hash(&mut self, hash: BlockHash) -> Result<Option<BlockHeader>, Self::Error>;
    fn block_header(block: &Self::Block) -> BlockHeader;
    fn block_logs_match_header(_block: &Self::Block) -> bool {
        true
    }

    fn verify_recent(
        &mut self,
        _height: u64,
        _hash: BlockHash,
    ) -> Result<VerifierStatus, Self::Error> {
        Ok(VerifierStatus::Unavailable)
    }
}

pub trait LiveSink<B>: RangeCommitSink<B> {
    fn checkpoint(&self) -> Option<BlockHeader>;
    fn canonical_header_at_height(&self, height: u64) -> Option<BlockHeader>;
    fn canonical_header_by_hash(&self, hash: BlockHash) -> Option<BlockHeader>;
    fn commit_batch(&mut self, batch: ChainBatch, apply_blocks: Vec<B>) -> Result<(), Self::Error>;
}

#[derive(Debug, Error)]
pub enum LiveError {
    #[error("{0}")]
    Source(String),
    #[error("{0}")]
    Sink(String),
    #[error("live tracker cancelled")]
    Cancelled,
    #[error(transparent)]
    Chain(#[from] ChainError),
    #[error(transparent)]
    Backfill(#[from] BackfillError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTracker {
    config: LiveConfig,
}

impl LiveTracker {
    #[must_use]
    pub const fn new(config: LiveConfig) -> Self {
        Self { config }
    }

    pub fn run<S, K>(
        &self,
        source: &mut S,
        sink: &mut K,
        events: impl IntoIterator<Item = LiveHeadEvent>,
    ) -> Result<LiveReport, LiveError>
    where
        S: LiveSource,
        K: LiveSink<S::Block>,
        K::Error: std::fmt::Display,
        BackfillError: From<K::Error>,
    {
        self.run_with_cancellation(source, sink, events, || false)
    }

    pub fn run_with_cancellation<S, K, C>(
        &self,
        source: &mut S,
        sink: &mut K,
        events: impl IntoIterator<Item = LiveHeadEvent>,
        is_cancelled: C,
    ) -> Result<LiveReport, LiveError>
    where
        S: LiveSource,
        K: LiveSink<S::Block>,
        K::Error: std::fmt::Display,
        BackfillError: From<K::Error>,
        C: Fn() -> bool,
    {
        let mut report = LiveReport::new(sink.checkpoint());
        for event in events {
            match event {
                LiveHeadEvent::Disconnect => report.transition(LiveReadiness::Unavailable),
                LiveHeadEvent::Reconnect => {}
                LiveHeadEvent::Wake(_) => {}
            }
        }

        ensure_not_cancelled(&is_cancelled)?;
        let mut budget = WindowBudget::new(self.config)?;
        budget.record(RpcMethod::GetBlockByNumber, &mut report)?;
        let head = source
            .current_head()
            .map_err(|error| LiveError::Source(error.to_string()))?;

        if matches!(
            source
                .verify_recent(head.height, head.hash)
                .map_err(|error| LiveError::Source(error.to_string()))?,
            VerifierStatus::Disagree
        ) {
            report.verifier_disagreements += 1;
            report.transition(LiveReadiness::Degraded);
        }

        let checkpoint = sink.checkpoint();
        let Some(checkpoint) = checkpoint else {
            if self.config.explicit_start.is_none() {
                return Ok(LiveReport::halted(
                    Some(head),
                    LiveHaltReason::MissingExplicitStart,
                ));
            }
            self.catch_up_from_start(source, sink, head, &mut budget, &mut report, &is_cancelled)?;
            report.final_checkpoint = sink.checkpoint();
            mark_ready_if_healthy(&mut report);
            return Ok(report);
        };

        if head.height < checkpoint.height {
            budget.record(RpcMethod::GetBlockByNumber, &mut report)?;
            let remote_at_head = source
                .header_by_number(head.height)
                .map_err(|error| LiveError::Source(error.to_string()))?;
            if let (Some(remote), Some(local)) =
                (remote_at_head, sink.canonical_header_at_height(head.height))
                && remote.hash == local.hash
            {
                report.final_checkpoint = Some(checkpoint);
                report.transition(LiveReadiness::Degraded);
                return Ok(report);
            }
            self.apply_chain_state(source, sink, head, &mut budget, &mut report, &is_cancelled)?;
            report.final_checkpoint = sink.checkpoint();
            mark_ready_if_healthy(&mut report);
            return Ok(report);
        }

        if head.height == checkpoint.height {
            if head.hash != checkpoint.hash {
                self.apply_chain_state(
                    source,
                    sink,
                    head,
                    &mut budget,
                    &mut report,
                    &is_cancelled,
                )?;
            }
            report.final_checkpoint = sink.checkpoint();
            mark_ready_if_healthy(&mut report);
            return Ok(report);
        }

        self.catch_up_from_checkpoint(
            source,
            sink,
            checkpoint,
            head,
            &mut budget,
            &mut report,
            &is_cancelled,
        )?;
        report.final_checkpoint = sink.checkpoint();
        mark_ready_if_healthy(&mut report);
        Ok(report)
    }

    fn catch_up_from_start<S, K>(
        &self,
        source: &mut S,
        sink: &mut K,
        head: BlockHeader,
        budget: &mut WindowBudget,
        report: &mut LiveReport,
        is_cancelled: &impl Fn() -> bool,
    ) -> Result<(), LiveError>
    where
        S: LiveSource,
        K: LiveSink<S::Block>,
        K::Error: std::fmt::Display,
        BackfillError: From<K::Error>,
    {
        let from = self.config.explicit_start.unwrap_or(0);
        let mut coordinator = OrderedCommitCoordinator::new(from, head.height, None)?;
        for height in from..=head.height {
            ensure_not_cancelled(is_cancelled)?;
            self.fetch_and_push_height(source, sink, &mut coordinator, height, budget, report)?;
        }
        Ok(())
    }

    fn catch_up_from_checkpoint<S, K>(
        &self,
        source: &mut S,
        sink: &mut K,
        checkpoint: BlockHeader,
        head: BlockHeader,
        budget: &mut WindowBudget,
        report: &mut LiveReport,
        is_cancelled: &impl Fn() -> bool,
    ) -> Result<(), LiveError>
    where
        S: LiveSource,
        K: LiveSink<S::Block>,
        K::Error: std::fmt::Display,
        BackfillError: From<K::Error>,
    {
        let start = checkpoint.height + 1;
        let mut coordinator = OrderedCommitCoordinator::new(start, head.height, Some(checkpoint))?;
        for height in start..=head.height {
            ensure_not_cancelled(is_cancelled)?;
            match self.fetch_and_push_height(source, sink, &mut coordinator, height, budget, report)
            {
                Ok(()) => {}
                Err(LiveError::Backfill(BackfillError::FetchedSuffixInvalidated { .. })) => {
                    report.apply_order.clear();
                    self.apply_chain_state(source, sink, head, budget, report, is_cancelled)?;
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn fetch_and_push_height<S, K>(
        &self,
        source: &mut S,
        sink: &mut K,
        coordinator: &mut OrderedCommitCoordinator<S::Block>,
        height: u64,
        budget: &mut WindowBudget,
        report: &mut LiveReport,
    ) -> Result<(), LiveError>
    where
        S: LiveSource,
        K: LiveSink<S::Block>,
        K::Error: std::fmt::Display,
        BackfillError: From<K::Error>,
    {
        budget.record(RpcMethod::GetBlockByNumber, report)?;
        let block = source
            .block_by_number(height)
            .map_err(|error| LiveError::Source(error.to_string()))?;
        budget.record(RpcMethod::GetLogs, report)?;
        let (header, block) =
            self.validate_or_refetch_number(source, height, block, budget, report)?;
        let fetched = FetchedRange::new(
            BackfillRange::new(height, height)?,
            vec![header],
            vec![block],
        )?;
        let progress = coordinator.push(fetched, sink)?;
        if progress.committed_blocks > 0 {
            report.apply_order.push(height);
        }
        Ok(())
    }

    fn apply_chain_state<S, K>(
        &self,
        source: &mut S,
        sink: &mut K,
        head: BlockHeader,
        budget: &mut WindowBudget,
        report: &mut LiveReport,
        is_cancelled: &impl Fn() -> bool,
    ) -> Result<(), LiveError>
    where
        S: LiveSource,
        K: LiveSink<S::Block>,
        K::Error: std::fmt::Display,
    {
        let Some(checkpoint) = sink.checkpoint() else {
            return Ok(());
        };
        let mut state = ChainState::from_tip(checkpoint, 256, self.config.max_reorg_depth)?;
        state.set_finalized_height(self.config.finalized_height);
        let batch = {
            let mut resolver = LiveResolver {
                source,
                sink,
                block: PhantomData::<S::Block>,
            };
            match state.apply(head, &mut resolver) {
                Ok(batch) => batch,
                Err(ChainError::MaxDepthExceeded { .. }) => {
                    *report = LiveReport::halted(sink.checkpoint(), LiveHaltReason::MaxReorgDepth);
                    return Ok(());
                }
                Err(ChainError::FinalizedBoundary { .. }) => {
                    *report =
                        LiveReport::halted(sink.checkpoint(), LiveHaltReason::FinalizedBoundary);
                    return Ok(());
                }
                Err(error) => return Err(LiveError::Chain(error)),
            }
        };

        let mut apply_blocks = Vec::new();
        for event in &batch.events {
            ensure_not_cancelled(is_cancelled)?;
            match event {
                ChainEvent::Rollback(header) => report.rollback_order.push(header.height),
                ChainEvent::Apply(header) => {
                    report.apply_order.push(header.height);
                    budget.record(RpcMethod::GetBlockByHash, report)?;
                    let block = source
                        .block_by_hash(header.hash)
                        .map_err(|error| LiveError::Source(error.to_string()))?;
                    budget.record(RpcMethod::GetLogs, report)?;
                    let block =
                        self.validate_or_refetch_hash(source, *header, block, budget, report)?;
                    apply_blocks.push(block);
                }
            }
        }

        sink.commit_batch(batch, apply_blocks)
            .map_err(|error| LiveError::Sink(error.to_string()))?;
        Ok(())
    }

    fn validate_or_refetch_number<S>(
        &self,
        source: &mut S,
        height: u64,
        block: S::Block,
        budget: &mut WindowBudget,
        report: &mut LiveReport,
    ) -> Result<(BlockHeader, S::Block), LiveError>
    where
        S: LiveSource,
    {
        let header = S::block_header(&block);
        if S::block_logs_match_header(&block) {
            return Ok((header, block));
        }

        report.transition(LiveReadiness::Degraded);
        budget.record(RpcMethod::GetBlockByNumber, report)?;
        let block = source
            .block_by_number(height)
            .map_err(|error| LiveError::Source(error.to_string()))?;
        budget.record(RpcMethod::GetLogs, report)?;
        let header = S::block_header(&block);
        if !S::block_logs_match_header(&block) {
            return Err(LiveError::Source(format!(
                "block/log hash mismatch persisted at height {height}"
            )));
        }
        Ok((header, block))
    }

    fn validate_or_refetch_hash<S>(
        &self,
        source: &mut S,
        expected: BlockHeader,
        block: S::Block,
        budget: &mut WindowBudget,
        report: &mut LiveReport,
    ) -> Result<S::Block, LiveError>
    where
        S: LiveSource,
    {
        if S::block_header(&block) == expected && S::block_logs_match_header(&block) {
            return Ok(block);
        }

        report.transition(LiveReadiness::Degraded);
        budget.record(RpcMethod::GetBlockByHash, report)?;
        let block = source
            .block_by_hash(expected.hash)
            .map_err(|error| LiveError::Source(error.to_string()))?;
        budget.record(RpcMethod::GetLogs, report)?;
        if S::block_header(&block) != expected || !S::block_logs_match_header(&block) {
            return Err(LiveError::Source(format!(
                "block/log hash mismatch persisted at height {}",
                expected.height
            )));
        }
        Ok(block)
    }
}

fn mark_ready_if_healthy(report: &mut LiveReport) {
    if report.halted.is_none() && report.verifier_disagreements == 0 {
        report.transition(LiveReadiness::Ready);
    }
}

fn ensure_not_cancelled(is_cancelled: &impl Fn() -> bool) -> Result<(), LiveError> {
    if is_cancelled() {
        return Err(LiveError::Cancelled);
    }
    Ok(())
}

struct LiveResolver<'a, S, K, B> {
    source: &'a mut S,
    sink: &'a K,
    block: PhantomData<B>,
}

impl<S, K, B> AncestryResolver for LiveResolver<'_, S, K, B>
where
    S: LiveSource,
    K: LiveSink<B>,
{
    fn header_by_hash(&mut self, hash: BlockHash) -> Result<Option<BlockHeader>, ResolverError> {
        self.source
            .header_by_hash(hash)
            .map_err(|error| ResolverError::new(error.to_string()))
    }

    fn canonical_header_by_hash(
        &mut self,
        hash: BlockHash,
    ) -> Result<Option<BlockHeader>, ResolverError> {
        Ok(self.sink.canonical_header_by_hash(hash))
    }

    fn canonical_header_at_height(
        &mut self,
        height: u64,
    ) -> Result<Option<BlockHeader>, ResolverError> {
        Ok(self.sink.canonical_header_at_height(height))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowBudget {
    budget: RpcBudget,
    window: Duration,
    exhausted_once: bool,
}

impl WindowBudget {
    fn new(config: LiveConfig) -> Result<Self, LiveError> {
        let budget = RpcBudget::new(u64::MAX, config.live_budget_cost_units_per_minute)?;
        Ok(Self {
            budget,
            window: config.budget_window,
            exhausted_once: false,
        })
    }

    fn record(&mut self, method: RpcMethod, report: &mut LiveReport) -> Result<(), LiveError> {
        if !self.exhausted_once && self.budget.record(method, method.default_cost()).is_err() {
            self.exhausted_once = true;
            report.continued_after_budget_exhaustion = true;
            report.transition(LiveReadiness::Degraded);
            report.transition(LiveReadiness::Unavailable);
            report
                .manual_clock_advances
                .push(self.window.as_secs().max(1));
            self.budget = RpcBudget::new(u64::MAX, u64::MAX)?;
            self.budget.record(method, method.default_cost())?;
        }
        Ok(())
    }
}
