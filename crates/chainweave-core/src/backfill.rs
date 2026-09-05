use std::{collections::BTreeMap, future::Future, time::Duration};

use thiserror::Error;

use crate::{BlockHash, BlockHeader};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackfillRange {
    pub from_block: u64,
    pub to_block: u64,
}

impl BackfillRange {
    pub const fn new(from_block: u64, to_block: u64) -> Result<Self, BackfillError> {
        if from_block > to_block {
            return Err(BackfillError::InvalidRange {
                from_block,
                to_block,
            });
        }
        Ok(Self {
            from_block,
            to_block,
        })
    }

    #[must_use]
    pub const fn len(self) -> u64 {
        self.to_block
            .saturating_sub(self.from_block)
            .saturating_add(1)
    }

    #[must_use]
    pub const fn contains(self, height: u64) -> bool {
        self.from_block <= height && height <= self.to_block
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillPlan {
    pub full_range: BackfillRange,
    pub request_ranges: Vec<BackfillRange>,
    pub bounded_workers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangePlanner {
    pub max_blocks_per_request: u64,
    pub bounded_workers: usize,
}

impl RangePlanner {
    pub const fn new(
        max_blocks_per_request: u64,
        bounded_workers: usize,
    ) -> Result<Self, BackfillError> {
        if max_blocks_per_request == 0 {
            return Err(BackfillError::EmptyRangeSpan);
        }
        if bounded_workers == 0 {
            return Err(BackfillError::EmptyWorkerCount);
        }
        Ok(Self {
            max_blocks_per_request,
            bounded_workers,
        })
    }

    pub fn plan(self, full_range: BackfillRange) -> BackfillPlan {
        let mut request_ranges = Vec::new();
        let mut from_block = full_range.from_block;
        while from_block <= full_range.to_block {
            let to_block = from_block
                .saturating_add(self.max_blocks_per_request - 1)
                .min(full_range.to_block);
            request_ranges.push(BackfillRange {
                from_block,
                to_block,
            });
            if to_block == u64::MAX {
                break;
            }
            from_block = to_block + 1;
        }

        BackfillPlan {
            full_range,
            request_ranges,
            bounded_workers: self.bounded_workers,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogRangeSizer {
    current_blocks: u64,
    min_blocks: u64,
    max_blocks: u64,
}

impl LogRangeSizer {
    pub const fn new(
        initial_blocks: u64,
        min_blocks: u64,
        max_blocks: u64,
    ) -> Result<Self, BackfillError> {
        if min_blocks == 0 || initial_blocks == 0 || max_blocks == 0 {
            return Err(BackfillError::EmptyRangeSpan);
        }
        if min_blocks > max_blocks {
            return Err(BackfillError::InvalidSizerBounds {
                min_blocks,
                max_blocks,
            });
        }
        if initial_blocks < min_blocks || initial_blocks > max_blocks {
            return Err(BackfillError::InitialSizerOutOfBounds {
                initial_blocks,
                min_blocks,
                max_blocks,
            });
        }
        Ok(Self {
            current_blocks: initial_blocks,
            min_blocks,
            max_blocks,
        })
    }

    #[must_use]
    pub const fn current_blocks(self) -> u64 {
        self.current_blocks
    }

    pub fn record_success(&mut self) {
        self.current_blocks = self
            .current_blocks
            .saturating_mul(2)
            .min(self.max_blocks)
            .max(self.min_blocks);
    }

    pub fn record_provider_limit(&mut self) {
        self.current_blocks = (self.current_blocks / 2).max(self.min_blocks);
    }

    #[must_use]
    pub fn next_range(self, from_block: u64, target_block: u64) -> Option<BackfillRange> {
        if from_block > target_block {
            return None;
        }
        Some(BackfillRange {
            from_block,
            to_block: from_block
                .saturating_add(self.current_blocks - 1)
                .min(target_block),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcFailure {
    Timeout,
    RateLimited { retry_after: Option<Duration> },
    ProviderLimit,
    Transient,
    Permanent,
}

impl RpcFailure {
    #[must_use]
    pub fn from_http_status(status: u16) -> Self {
        match status {
            408 | 425 | 500..=599 => Self::Transient,
            429 => Self::RateLimited { retry_after: None },
            _ => Self::Permanent,
        }
    }

    #[must_use]
    pub fn from_rpc_message(message: &str) -> Self {
        let lower = message.to_ascii_lowercase();
        if lower.contains("too many requests") || lower.contains("rate limit") {
            Self::RateLimited { retry_after: None }
        } else if lower.contains("more than")
            || lower.contains("too many results")
            || lower.contains("response size")
            || lower.contains("block range")
        {
            Self::ProviderLimit
        } else {
            Self::Permanent
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub jitter: Duration,
}

impl RetryPolicy {
    pub fn new(
        max_attempts: u32,
        base_delay: Duration,
        max_delay: Duration,
        jitter: Duration,
    ) -> Result<Self, BackfillError> {
        if max_attempts == 0 {
            return Err(BackfillError::EmptyRetryAttempts);
        }
        if base_delay.is_zero() || max_delay.is_zero() {
            return Err(BackfillError::EmptyRetryDelay);
        }
        if base_delay > max_delay {
            return Err(BackfillError::InvalidRetryBounds);
        }
        Ok(Self {
            max_attempts,
            base_delay,
            max_delay,
            jitter,
        })
    }

    #[must_use]
    pub fn decision(self, failure: RpcFailure, attempt: u32) -> RetryDecision {
        if matches!(failure, RpcFailure::Permanent) || attempt >= self.max_attempts {
            return RetryDecision::GiveUp;
        }
        if let RpcFailure::RateLimited {
            retry_after: Some(delay),
        } = failure
        {
            return RetryDecision::RetryAfter(delay.min(self.max_delay));
        }
        RetryDecision::RetryAfter(self.backoff(attempt))
    }

    fn backoff(self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1).min(31);
        let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
        let base = self
            .base_delay
            .saturating_mul(multiplier)
            .min(self.max_delay);
        base.saturating_add(self.jitter_for(attempt))
            .min(self.max_delay)
    }

    fn jitter_for(self, attempt: u32) -> Duration {
        let jitter_ms = u64::try_from(self.jitter.as_millis()).unwrap_or(u64::MAX);
        if jitter_ms == 0 {
            return Duration::ZERO;
        }
        Duration::from_millis((u64::from(attempt) * 37) % (jitter_ms + 1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    RetryAfter(Duration),
    GiveUp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcBudget {
    max_requests: u64,
    max_cost_units: u64,
    requests: u64,
    cost_units: u64,
}

impl RpcBudget {
    pub const fn new(max_requests: u64, max_cost_units: u64) -> Result<Self, BackfillError> {
        if max_requests == 0 {
            return Err(BackfillError::EmptyRequestBudget);
        }
        if max_cost_units == 0 {
            return Err(BackfillError::EmptyCostBudget);
        }
        Ok(Self {
            max_requests,
            max_cost_units,
            requests: 0,
            cost_units: 0,
        })
    }

    pub fn record(&mut self, method: RpcMethod, cost_units: u64) -> Result<(), BackfillError> {
        let next_requests = self.requests.saturating_add(1);
        let next_cost = self
            .cost_units
            .saturating_add(cost_units.max(method.default_cost()));
        if next_requests > self.max_requests {
            return Err(BackfillError::RequestBudgetExceeded {
                max_requests: self.max_requests,
            });
        }
        if next_cost > self.max_cost_units {
            return Err(BackfillError::CostBudgetExceeded {
                max_cost_units: self.max_cost_units,
            });
        }
        self.requests = next_requests;
        self.cost_units = next_cost;
        Ok(())
    }

    #[must_use]
    pub const fn requests(&self) -> u64 {
        self.requests
    }

    #[must_use]
    pub const fn cost_units(&self) -> u64 {
        self.cost_units
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcMethod {
    GetBlockByNumber,
    GetBlockByHash,
    GetLogs,
}

impl RpcMethod {
    #[must_use]
    pub const fn default_cost(self) -> u64 {
        match self {
            Self::GetBlockByNumber | Self::GetBlockByHash => 1,
            Self::GetLogs => 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRange<L> {
    pub range: BackfillRange,
    pub headers: Vec<BlockHeader>,
    pub logs: Vec<L>,
}

impl<L> FetchedRange<L> {
    pub fn new(
        range: BackfillRange,
        headers: Vec<BlockHeader>,
        logs: Vec<L>,
    ) -> Result<Self, BackfillError> {
        validate_headers(&range, &headers)?;
        Ok(Self {
            range,
            headers,
            logs,
        })
    }

    #[must_use]
    pub fn first_header(&self) -> Option<BlockHeader> {
        self.headers.first().copied()
    }

    #[must_use]
    pub fn last_header(&self) -> Option<BlockHeader> {
        self.headers.last().copied()
    }
}

pub trait BackfillSource {
    type Log;
    type Error;

    fn fetch_range(&mut self, range: BackfillRange)
    -> Result<FetchedRange<Self::Log>, Self::Error>;
}

pub trait RangeCommitSink<L> {
    type Error;

    fn commit_range(&mut self, fetched: FetchedRange<L>) -> Result<(), Self::Error>;
}

pub trait AsyncRangeCommitSink<L> {
    type Error;

    fn commit_range(
        &mut self,
        fetched: FetchedRange<L>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackfillHandoff {
    pub captured_head: BlockHeader,
    pub backfilled_through: BlockHeader,
}

impl BackfillHandoff {
    pub const fn new(captured_head: BlockHeader, backfilled_through: BlockHeader) -> Self {
        Self {
            captured_head,
            backfilled_through,
        }
    }

    #[must_use]
    pub fn needs_reconcile(self) -> bool {
        self.captured_head.hash != self.backfilled_through.hash
            || self.captured_head.height != self.backfilled_through.height
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedCommitCoordinator<L> {
    next_height: u64,
    target_height: u64,
    parent_anchor: Option<BlockHeader>,
    pending: BTreeMap<u64, FetchedRange<L>>,
}

impl<L> OrderedCommitCoordinator<L> {
    pub const fn new(
        from_block: u64,
        target_height: u64,
        parent_anchor: Option<BlockHeader>,
    ) -> Result<Self, BackfillError> {
        if from_block > target_height {
            return Err(BackfillError::InvalidRange {
                from_block,
                to_block: target_height,
            });
        }
        if from_block == 0 && parent_anchor.is_some() {
            return Err(BackfillError::UnexpectedParentAnchor);
        }
        if from_block > 0 {
            let Some(anchor) = parent_anchor else {
                return Err(BackfillError::MissingParentAnchor { from_block });
            };
            if anchor.height + 1 != from_block {
                return Err(BackfillError::InvalidParentAnchor {
                    anchor_height: anchor.height,
                    from_block,
                });
            }
        }
        Ok(Self {
            next_height: from_block,
            target_height,
            parent_anchor,
            pending: BTreeMap::new(),
        })
    }

    pub fn push<S>(
        &mut self,
        fetched: FetchedRange<L>,
        sink: &mut S,
    ) -> Result<CommitProgress, BackfillError>
    where
        S: RangeCommitSink<L>,
        BackfillError: From<S::Error>,
    {
        self.insert_pending(fetched)?;
        self.drain_ready(sink)
    }

    pub async fn push_async<S>(
        &mut self,
        fetched: FetchedRange<L>,
        sink: &mut S,
    ) -> Result<CommitProgress, BackfillError>
    where
        S: AsyncRangeCommitSink<L>,
        BackfillError: From<S::Error>,
    {
        self.insert_pending(fetched)?;
        self.drain_ready_async(sink).await
    }

    #[must_use]
    pub const fn next_height(&self) -> u64 {
        self.next_height
    }

    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.next_height > self.target_height
    }

    fn drain_ready<S>(&mut self, sink: &mut S) -> Result<CommitProgress, BackfillError>
    where
        S: RangeCommitSink<L>,
        BackfillError: From<S::Error>,
    {
        let mut progress = CommitProgress::default();
        while let Some(fetched) = self.pending.remove(&self.next_height) {
            let last_header = self.validate_ready_range(&fetched)?;
            let block_count = fetched.range.len();
            let log_count = fetched.logs.len();
            sink.commit_range(fetched)?;
            self.parent_anchor = Some(last_header);
            self.next_height = last_header.height.saturating_add(1);
            progress.committed_ranges += 1;
            progress.committed_blocks += block_count;
            progress.committed_logs += log_count;
            progress.last_committed_height = Some(last_header.height);
        }
        Ok(progress)
    }

    async fn drain_ready_async<S>(&mut self, sink: &mut S) -> Result<CommitProgress, BackfillError>
    where
        S: AsyncRangeCommitSink<L>,
        BackfillError: From<S::Error>,
    {
        let mut progress = CommitProgress::default();
        while let Some(fetched) = self.pending.remove(&self.next_height) {
            let last_header = self.validate_ready_range(&fetched)?;
            let block_count = fetched.range.len();
            let log_count = fetched.logs.len();
            sink.commit_range(fetched).await?;
            self.parent_anchor = Some(last_header);
            self.next_height = last_header.height.saturating_add(1);
            progress.committed_ranges += 1;
            progress.committed_blocks += block_count;
            progress.committed_logs += log_count;
            progress.last_committed_height = Some(last_header.height);
        }
        Ok(progress)
    }

    fn insert_pending(&mut self, fetched: FetchedRange<L>) -> Result<(), BackfillError> {
        let from_block = fetched.range.from_block;
        if fetched.range.to_block > self.target_height {
            return Err(BackfillError::RangeBeyondTarget {
                to_block: fetched.range.to_block,
                target_height: self.target_height,
            });
        }
        if from_block < self.next_height {
            return Err(BackfillError::StaleRange {
                from_block,
                next_height: self.next_height,
            });
        }
        if self.pending.insert(from_block, fetched).is_some() {
            return Err(BackfillError::DuplicateRange { from_block });
        }
        Ok(())
    }

    fn validate_ready_range(
        &mut self,
        fetched: &FetchedRange<L>,
    ) -> Result<BlockHeader, BackfillError> {
        if let Some(first_header) = fetched.first_header()
            && let Some(anchor) = self.parent_anchor
            && (first_header.parent_hash != anchor.hash || first_header.height != anchor.height + 1)
        {
            let refetch_from = first_header.height;
            self.discard_suffix(refetch_from);
            return Err(BackfillError::FetchedSuffixInvalidated {
                refetch_from,
                expected_parent: anchor.hash,
                actual_parent: first_header.parent_hash,
            });
        }
        fetched
            .last_header()
            .ok_or(BackfillError::EmptyFetchedRange {
                from_block: self.next_height,
            })
    }

    fn discard_suffix(&mut self, refetch_from: u64) {
        self.pending = self.pending.split_off(&refetch_from);
        self.pending.clear();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CommitProgress {
    pub committed_ranges: usize,
    pub committed_blocks: u64,
    pub committed_logs: usize,
    pub last_committed_height: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BackfillError {
    #[error("invalid backfill range: from block {from_block} is greater than to block {to_block}")]
    InvalidRange { from_block: u64, to_block: u64 },
    #[error("backfill range span must be nonzero")]
    EmptyRangeSpan,
    #[error("backfill worker count must be nonzero")]
    EmptyWorkerCount,
    #[error("sizer min blocks {min_blocks} exceed max blocks {max_blocks}")]
    InvalidSizerBounds { min_blocks: u64, max_blocks: u64 },
    #[error("initial sizer span {initial_blocks} is outside [{min_blocks}, {max_blocks}]")]
    InitialSizerOutOfBounds {
        initial_blocks: u64,
        min_blocks: u64,
        max_blocks: u64,
    },
    #[error("retry attempts must be nonzero")]
    EmptyRetryAttempts,
    #[error("retry delays must be nonzero")]
    EmptyRetryDelay,
    #[error("base retry delay must not exceed max retry delay")]
    InvalidRetryBounds,
    #[error("RPC request budget must be nonzero")]
    EmptyRequestBudget,
    #[error("RPC cost budget must be nonzero")]
    EmptyCostBudget,
    #[error("RPC request budget exceeded: max {max_requests}")]
    RequestBudgetExceeded { max_requests: u64 },
    #[error("RPC cost budget exceeded: max {max_cost_units}")]
    CostBudgetExceeded { max_cost_units: u64 },
    #[error("range {to_block} exceeds target height {target_height}")]
    RangeBeyondTarget { to_block: u64, target_height: u64 },
    #[error("range starting at {from_block} is stale; next commit height is {next_height}")]
    StaleRange { from_block: u64, next_height: u64 },
    #[error("duplicate fetched range starting at {from_block}")]
    DuplicateRange { from_block: u64 },
    #[error("parent anchor is required for backfill starting at {from_block}")]
    MissingParentAnchor { from_block: u64 },
    #[error("parent anchor is invalid: anchor height {anchor_height}, from block {from_block}")]
    InvalidParentAnchor { anchor_height: u64, from_block: u64 },
    #[error("genesis backfill must not have a parent anchor")]
    UnexpectedParentAnchor,
    #[error("fetched range {from_block}-{to_block} returned {actual} headers, expected {expected}")]
    HeaderCountMismatch {
        from_block: u64,
        to_block: u64,
        expected: u64,
        actual: usize,
    },
    #[error("fetched header at offset {offset} has height {actual}, expected {expected}")]
    HeaderHeightMismatch {
        offset: usize,
        expected: u64,
        actual: u64,
    },
    #[error("fetched range has a broken parent link at height {height}")]
    BrokenParentLink { height: u64 },
    #[error("fetched range starting at {from_block} was empty")]
    EmptyFetchedRange { from_block: u64 },
    #[error(
        "fetched suffix invalidated from height {refetch_from}; expected parent {expected_parent:?}, got {actual_parent:?}"
    )]
    FetchedSuffixInvalidated {
        refetch_from: u64,
        expected_parent: BlockHash,
        actual_parent: BlockHash,
    },
    #[error("commit failed: {0}")]
    Commit(String),
}

impl From<String> for BackfillError {
    fn from(value: String) -> Self {
        Self::Commit(value)
    }
}

fn validate_headers(range: &BackfillRange, headers: &[BlockHeader]) -> Result<(), BackfillError> {
    let expected = range.len();
    if headers.len() as u64 != expected {
        return Err(BackfillError::HeaderCountMismatch {
            from_block: range.from_block,
            to_block: range.to_block,
            expected,
            actual: headers.len(),
        });
    }

    for (offset, header) in headers.iter().enumerate() {
        let expected_height = range.from_block + offset as u64;
        if header.height != expected_height {
            return Err(BackfillError::HeaderHeightMismatch {
                offset,
                expected: expected_height,
                actual: header.height,
            });
        }
        if let Some(parent) = offset.checked_sub(1).and_then(|parent| headers.get(parent))
            && (header.parent_hash != parent.hash || header.height != parent.height + 1)
        {
            return Err(BackfillError::BrokenParentLink {
                height: header.height,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct MemorySink {
        committed: Vec<FetchedRange<u64>>,
    }

    impl RangeCommitSink<u64> for MemorySink {
        type Error = String;

        fn commit_range(&mut self, fetched: FetchedRange<u64>) -> Result<(), Self::Error> {
            self.committed.push(fetched);
            Ok(())
        }
    }

    fn hash(value: u8) -> BlockHash {
        [value; 32]
    }

    fn header(value: u8, parent: u8, height: u64) -> BlockHeader {
        BlockHeader::new(hash(value), hash(parent), height)
    }

    fn fetched(range: BackfillRange, ids: &[u8]) -> FetchedRange<u64> {
        let headers = ids
            .iter()
            .enumerate()
            .map(|(offset, value)| {
                let height = range.from_block + offset as u64;
                let parent = if offset == 0 {
                    value.saturating_sub(1)
                } else {
                    ids[offset - 1]
                };
                header(*value, parent, height)
            })
            .collect();
        FetchedRange::new(
            range,
            headers,
            ids.iter().map(|value| u64::from(*value)).collect(),
        )
        .unwrap()
    }

    #[test]
    fn planner_splits_inclusive_ranges_for_bounded_workers() {
        let full_range = BackfillRange::new(10, 25).unwrap();
        let plan = RangePlanner::new(6, 3).unwrap().plan(full_range);

        assert_eq!(plan.bounded_workers, 3);
        assert_eq!(
            plan.request_ranges,
            vec![
                BackfillRange::new(10, 15).unwrap(),
                BackfillRange::new(16, 21).unwrap(),
                BackfillRange::new(22, 25).unwrap(),
            ]
        );
        assert!(BackfillRange::new(9, 8).is_err());
        assert!(RangePlanner::new(0, 3).is_err());
        assert!(RangePlanner::new(10, 0).is_err());
    }

    #[test]
    fn adaptive_sizer_grows_on_success_and_shrinks_on_provider_limits() {
        let mut sizer = LogRangeSizer::new(100, 25, 400).unwrap();

        assert_eq!(
            sizer.next_range(1_000, 1_500).unwrap(),
            BackfillRange::new(1_000, 1_099).unwrap()
        );
        sizer.record_success();
        assert_eq!(sizer.current_blocks(), 200);
        sizer.record_success();
        sizer.record_success();
        assert_eq!(sizer.current_blocks(), 400);

        sizer.record_provider_limit();
        assert_eq!(sizer.current_blocks(), 200);
        sizer.record_provider_limit();
        sizer.record_provider_limit();
        sizer.record_provider_limit();
        assert_eq!(sizer.current_blocks(), 25);
    }

    #[test]
    fn retry_policy_handles_timeouts_rate_limits_provider_limits_and_budget() {
        let policy = RetryPolicy::new(
            3,
            Duration::from_millis(100),
            Duration::from_secs(1),
            Duration::from_millis(50),
        )
        .unwrap();

        assert!(matches!(
            policy.decision(RpcFailure::Timeout, 1),
            RetryDecision::RetryAfter(_)
        ));
        assert_eq!(
            policy.decision(
                RpcFailure::RateLimited {
                    retry_after: Some(Duration::from_secs(7)),
                },
                1,
            ),
            RetryDecision::RetryAfter(Duration::from_secs(1))
        );
        assert!(matches!(
            RpcFailure::from_http_status(429),
            RpcFailure::RateLimited { .. }
        ));
        assert_eq!(
            RpcFailure::from_rpc_message("query returned more than 10000 results"),
            RpcFailure::ProviderLimit
        );
        assert_eq!(
            policy.decision(RpcFailure::ProviderLimit, 3),
            RetryDecision::GiveUp
        );
        assert_eq!(
            policy.decision(RpcFailure::Permanent, 1),
            RetryDecision::GiveUp
        );

        let mut budget = RpcBudget::new(2, 6).unwrap();
        budget.record(RpcMethod::GetBlockByNumber, 0).unwrap();
        budget.record(RpcMethod::GetLogs, 0).unwrap();
        assert_eq!(budget.requests(), 2);
        assert_eq!(budget.cost_units(), 6);
        assert!(budget.record(RpcMethod::GetBlockByHash, 0).is_err());
    }

    #[test]
    fn fetched_ranges_validate_header_count_height_and_parent_continuity() {
        let range = BackfillRange::new(7, 8).unwrap();
        let good = vec![header(7, 6, 7), header(8, 7, 8)];
        assert!(FetchedRange::<()>::new(range, good, Vec::new()).is_ok());

        let short = vec![header(7, 6, 7)];
        assert!(matches!(
            FetchedRange::<()>::new(range, short, Vec::new()).unwrap_err(),
            BackfillError::HeaderCountMismatch { .. }
        ));

        let wrong_height = vec![header(7, 6, 7), header(8, 7, 9)];
        assert!(matches!(
            FetchedRange::<()>::new(range, wrong_height, Vec::new()).unwrap_err(),
            BackfillError::HeaderHeightMismatch { .. }
        ));

        let broken_parent = vec![header(7, 6, 7), header(8, 99, 8)];
        assert!(matches!(
            FetchedRange::<()>::new(range, broken_parent, Vec::new()).unwrap_err(),
            BackfillError::BrokenParentLink { .. }
        ));
    }

    #[test]
    fn ordered_coordinator_commits_out_of_order_ranges_in_height_order() {
        let anchor = header(9, 8, 9);
        let mut coordinator = OrderedCommitCoordinator::new(10, 13, Some(anchor)).unwrap();
        let mut sink = MemorySink::default();

        let progress = coordinator
            .push(
                fetched(BackfillRange::new(12, 13).unwrap(), &[12, 13]),
                &mut sink,
            )
            .unwrap();
        assert_eq!(progress.committed_ranges, 0);
        assert_eq!(sink.committed.len(), 0);

        let progress = coordinator
            .push(
                fetched(BackfillRange::new(10, 11).unwrap(), &[10, 11]),
                &mut sink,
            )
            .unwrap();

        assert_eq!(progress.committed_ranges, 2);
        assert_eq!(progress.committed_logs, 4);
        assert_eq!(progress.last_committed_height, Some(13));
        assert!(coordinator.is_complete());
        assert_eq!(
            sink.committed
                .iter()
                .map(|range| range.range)
                .collect::<Vec<_>>(),
            vec![
                BackfillRange::new(10, 11).unwrap(),
                BackfillRange::new(12, 13).unwrap(),
            ]
        );
    }

    #[test]
    fn coordinator_discards_fetched_suffix_when_anchor_no_longer_matches() {
        let original_anchor = header(9, 8, 9);
        let replacement_anchor = header(99, 8, 9);
        let mut coordinator = OrderedCommitCoordinator::new(10, 13, Some(original_anchor)).unwrap();
        let mut sink = MemorySink::default();

        coordinator
            .push(
                fetched(BackfillRange::new(12, 13).unwrap(), &[12, 13]),
                &mut sink,
            )
            .unwrap();
        coordinator.parent_anchor = Some(replacement_anchor);

        let error = coordinator
            .push(
                fetched(BackfillRange::new(10, 11).unwrap(), &[10, 11]),
                &mut sink,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            BackfillError::FetchedSuffixInvalidated {
                refetch_from: 10,
                expected_parent,
                actual_parent,
            } if expected_parent == hash(99) && actual_parent == hash(9)
        ));
        assert_eq!(sink.committed.len(), 0);
        assert_eq!(coordinator.next_height(), 10);
    }

    #[test]
    fn handoff_requires_reconcile_unless_backfilled_head_matches_captured_head() {
        let captured = header(50, 49, 50);
        assert!(!BackfillHandoff::new(captured, captured).needs_reconcile());
        assert!(BackfillHandoff::new(captured, header(51, 50, 51)).needs_reconcile());
    }
}
