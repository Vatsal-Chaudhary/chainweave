use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use thiserror::Error;

pub type BlockHash = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockHeader {
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
    pub height: u64,
}

impl BlockHeader {
    #[must_use]
    pub const fn new(hash: BlockHash, parent_hash: BlockHash, height: u64) -> Self {
        Self {
            hash,
            parent_hash,
            height,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainEvent {
    Apply(BlockHeader),
    Rollback(BlockHeader),
}

impl ChainEvent {
    #[must_use]
    pub const fn header(self) -> BlockHeader {
        match self {
            Self::Apply(header) | Self::Rollback(header) => header,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainTransition {
    Bootstrap,
    Duplicate,
    Extend,
    Gap,
    Reorg,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainBatch {
    pub transition: ChainTransition,
    pub common_ancestor: Option<BlockHeader>,
    pub events: Vec<ChainEvent>,
}

impl ChainBatch {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

pub trait AncestryResolver {
    /// Returns a header by hash from the authoritative ancestry source.
    ///
    /// # Errors
    ///
    /// Returns [`ResolverError`] when the backing source cannot answer safely.
    fn header_by_hash(&mut self, hash: BlockHash) -> Result<Option<BlockHeader>, ResolverError>;

    /// Returns a persisted canonical header by hash when it is known.
    ///
    /// # Errors
    ///
    /// Returns [`ResolverError`] when the backing source cannot answer safely.
    fn canonical_header_by_hash(
        &mut self,
        hash: BlockHash,
    ) -> Result<Option<BlockHeader>, ResolverError>;

    /// Returns a persisted canonical header at a height when it is known.
    ///
    /// # Errors
    ///
    /// Returns [`ResolverError`] when the backing source cannot answer safely.
    fn canonical_header_at_height(
        &mut self,
        height: u64,
    ) -> Result<Option<BlockHeader>, ResolverError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct ResolverError {
    message: String,
}

impl ResolverError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ChainError {
    #[error("header cache size must be nonzero")]
    EmptyHeaderCache,
    #[error("max reorg depth must be nonzero")]
    EmptyMaxReorgDepth,
    #[error("new head is behind the current tip: new={new_height}, tip={tip_height}")]
    NewHeadBehindTip { new_height: u64, tip_height: u64 },
    #[error(
        "new head references current tip but has invalid height: new={new_height}, tip={tip_height}"
    )]
    InvalidExtensionHeight { new_height: u64, tip_height: u64 },
    #[error("missing parent header for hash {hash:?}")]
    MissingParent { hash: BlockHash },
    #[error(
        "invalid parent link: child height {child_height} parent height {parent_height}; child parent hash {expected_hash:?}, parent hash {actual_hash:?}"
    )]
    InvalidParentLink {
        child_height: u64,
        parent_height: u64,
        expected_hash: BlockHash,
        actual_hash: BlockHash,
    },
    #[error("could not prove common ancestor within max depth {max_depth}")]
    MaxDepthExceeded { max_depth: u64 },
    #[error(
        "reorg would cross finalized boundary: ancestor height {ancestor_height}, finalized height {finalized_height}"
    )]
    FinalizedBoundary {
        ancestor_height: u64,
        finalized_height: u64,
    },
    #[error("canonical chain is empty")]
    EmptyCanonicalChain,
    #[error("canonical chain has a broken link at height {height}")]
    BrokenCanonicalChain { height: u64 },
    #[error("ancestry resolver failed: {0}")]
    Resolver(#[from] ResolverError),
}

#[derive(Debug, Clone)]
pub struct ChainState {
    cache: HeaderCache,
    max_reorg_depth: u64,
    finalized_height: Option<u64>,
    tip: Option<BlockHeader>,
    canonical_by_height: BTreeMap<u64, BlockHeader>,
    canonical_hashes: HashSet<BlockHash>,
}

impl ChainState {
    /// Builds an empty transition state.
    ///
    /// # Errors
    ///
    /// Returns an error if either bound is zero.
    pub fn new(header_cache_size: usize, max_reorg_depth: u64) -> Result<Self, ChainError> {
        if header_cache_size == 0 {
            return Err(ChainError::EmptyHeaderCache);
        }
        if max_reorg_depth == 0 {
            return Err(ChainError::EmptyMaxReorgDepth);
        }

        Ok(Self {
            cache: HeaderCache::new(header_cache_size),
            max_reorg_depth,
            finalized_height: None,
            tip: None,
            canonical_by_height: BTreeMap::new(),
            canonical_hashes: HashSet::new(),
        })
    }

    /// Seeds state with only the current canonical tip.
    ///
    /// This mirrors a restarted process whose older canonical history must be proven through the
    /// resolver instead of the recent in-memory cache.
    ///
    /// # Errors
    ///
    /// Returns an error if either bound is zero.
    pub fn from_tip(
        tip: BlockHeader,
        header_cache_size: usize,
        max_reorg_depth: u64,
    ) -> Result<Self, ChainError> {
        let mut state = Self::new(header_cache_size, max_reorg_depth)?;
        state.insert_canonical(tip);
        state.tip = Some(tip);
        Ok(state)
    }

    /// Seeds state with a known continuous canonical chain.
    ///
    /// # Errors
    ///
    /// Returns an error when the provided chain is empty, the bounds are invalid, or adjacent
    /// headers do not link by hash and height.
    pub fn from_canonical_chain(
        headers: &[BlockHeader],
        header_cache_size: usize,
        max_reorg_depth: u64,
    ) -> Result<Self, ChainError> {
        if headers.is_empty() {
            return Err(ChainError::EmptyCanonicalChain);
        }
        let mut state = Self::new(header_cache_size, max_reorg_depth)?;
        for window in headers.windows(2) {
            let [parent, child] = window else {
                unreachable!("windows(2) always returns two headers");
            };
            if child.height != parent.height + 1 || child.parent_hash != parent.hash {
                return Err(ChainError::BrokenCanonicalChain {
                    height: child.height,
                });
            }
        }

        for header in headers {
            state.insert_canonical(*header);
        }
        state.tip = headers.last().copied();
        Ok(state)
    }

    #[must_use]
    pub const fn tip(&self) -> Option<BlockHeader> {
        self.tip
    }

    #[must_use]
    pub fn canonical_chain(&self) -> Vec<BlockHeader> {
        self.canonical_by_height.values().copied().collect()
    }

    #[must_use]
    pub const fn finalized_height(&self) -> Option<u64> {
        self.finalized_height
    }

    pub fn set_finalized_height(&mut self, finalized_height: Option<u64>) {
        self.finalized_height = finalized_height;
    }

    #[must_use]
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    #[must_use]
    pub fn cached_header(&self, hash: BlockHash) -> Option<BlockHeader> {
        self.cache.get(hash)
    }

    /// Applies a new observed head and returns the ordered canonical transition batch.
    ///
    /// # Errors
    ///
    /// Returns an error when ancestry cannot be proven, the branch is malformed, the configured
    /// maximum reorg depth is exceeded, or the transition would cross the finalized boundary.
    pub fn apply<R: AncestryResolver>(
        &mut self,
        new_head: BlockHeader,
        resolver: &mut R,
    ) -> Result<ChainBatch, ChainError> {
        if self.is_known_canonical(new_head)
            || self.tip.is_some_and(|tip| tip.hash == new_head.hash)
        {
            self.cache.insert(new_head);
            return Ok(ChainBatch {
                transition: ChainTransition::Duplicate,
                common_ancestor: Some(new_head),
                events: Vec::new(),
            });
        }

        let Some(tip) = self.tip else {
            if new_head.height != 0 {
                return Err(ChainError::MissingParent {
                    hash: new_head.parent_hash,
                });
            }
            let batch = ChainBatch {
                transition: ChainTransition::Bootstrap,
                common_ancestor: None,
                events: vec![ChainEvent::Apply(new_head)],
            };
            self.commit(&batch);
            return Ok(batch);
        };

        if new_head.height < tip.height {
            return Err(ChainError::NewHeadBehindTip {
                new_height: new_head.height,
                tip_height: tip.height,
            });
        }

        if new_head.parent_hash == tip.hash {
            if new_head.height != tip.height + 1 {
                return Err(ChainError::InvalidExtensionHeight {
                    new_height: new_head.height,
                    tip_height: tip.height,
                });
            }
            let batch = ChainBatch {
                transition: ChainTransition::Extend,
                common_ancestor: Some(tip),
                events: vec![ChainEvent::Apply(new_head)],
            };
            self.commit(&batch);
            return Ok(batch);
        }

        let (ancestor, mut replacement_descending) =
            self.resolve_common_ancestor(new_head, resolver)?;
        self.enforce_finalized_boundary(ancestor)?;

        let rollbacks = self.rollback_path(tip, ancestor, resolver)?;
        replacement_descending.reverse();
        let applies = replacement_descending;

        let mut events = Vec::with_capacity(rollbacks.len() + applies.len());
        events.extend(rollbacks.into_iter().map(ChainEvent::Rollback));
        events.extend(applies.into_iter().map(ChainEvent::Apply));

        let transition = if events.is_empty() {
            ChainTransition::Duplicate
        } else if events
            .iter()
            .any(|event| matches!(event, ChainEvent::Rollback(_)))
        {
            ChainTransition::Reorg
        } else {
            ChainTransition::Gap
        };

        let batch = ChainBatch {
            transition,
            common_ancestor: Some(ancestor),
            events,
        };
        self.commit(&batch);
        Ok(batch)
    }

    fn resolve_common_ancestor<R: AncestryResolver>(
        &self,
        new_head: BlockHeader,
        resolver: &mut R,
    ) -> Result<(BlockHeader, Vec<BlockHeader>), ChainError> {
        let mut current = new_head;
        let mut replacement_descending = Vec::new();

        loop {
            if let Some(canonical) = self.canonical_match(current, resolver)? {
                return Ok((canonical, replacement_descending));
            }

            replacement_descending.push(current);
            if replacement_descending.len() as u64 > self.max_reorg_depth {
                return Err(ChainError::MaxDepthExceeded {
                    max_depth: self.max_reorg_depth,
                });
            }

            current = self.parent_for(current, resolver)?;
        }
    }

    fn rollback_path<R: AncestryResolver>(
        &self,
        tip: BlockHeader,
        ancestor: BlockHeader,
        resolver: &mut R,
    ) -> Result<Vec<BlockHeader>, ChainError> {
        let mut current = tip;
        let mut rollbacks = Vec::new();

        while current.height > ancestor.height {
            rollbacks.push(current);
            if rollbacks.len() as u64 > self.max_reorg_depth {
                return Err(ChainError::MaxDepthExceeded {
                    max_depth: self.max_reorg_depth,
                });
            }
            current = self.parent_for(current, resolver)?;
        }

        if current.hash != ancestor.hash {
            return Err(ChainError::MissingParent {
                hash: ancestor.hash,
            });
        }

        Ok(rollbacks)
    }

    fn canonical_match<R: AncestryResolver>(
        &self,
        header: BlockHeader,
        resolver: &mut R,
    ) -> Result<Option<BlockHeader>, ChainError> {
        if self.is_known_canonical(header) {
            return Ok(Some(header));
        }

        if let Some(canonical) = resolver.canonical_header_by_hash(header.hash)? {
            validate_same_header(header, canonical)?;
            return Ok(Some(canonical));
        }

        if let Some(canonical) = resolver.canonical_header_at_height(header.height)?
            && canonical.hash == header.hash
        {
            validate_same_header(header, canonical)?;
            return Ok(Some(canonical));
        }

        Ok(None)
    }

    fn parent_for<R: AncestryResolver>(
        &self,
        child: BlockHeader,
        resolver: &mut R,
    ) -> Result<BlockHeader, ChainError> {
        if child.height == 0 {
            return Err(ChainError::MissingParent {
                hash: child.parent_hash,
            });
        }

        let local_parent = self
            .cache
            .get(child.parent_hash)
            .or_else(|| self.canonical_by_height.get(&(child.height - 1)).copied())
            .filter(|candidate| candidate.hash == child.parent_hash);
        let parent = if let Some(parent) = local_parent {
            Some(parent)
        } else {
            resolver.header_by_hash(child.parent_hash)?
        };

        let Some(parent) = parent else {
            return Err(ChainError::MissingParent {
                hash: child.parent_hash,
            });
        };

        validate_parent_link(child, parent)?;
        Ok(parent)
    }

    fn enforce_finalized_boundary(&self, ancestor: BlockHeader) -> Result<(), ChainError> {
        if let Some(finalized_height) = self.finalized_height
            && ancestor.height < finalized_height
        {
            return Err(ChainError::FinalizedBoundary {
                ancestor_height: ancestor.height,
                finalized_height,
            });
        }
        Ok(())
    }

    fn is_known_canonical(&self, header: BlockHeader) -> bool {
        self.canonical_hashes.contains(&header.hash)
            && self
                .canonical_by_height
                .get(&header.height)
                .is_some_and(|known| *known == header)
    }

    fn insert_canonical(&mut self, header: BlockHeader) {
        self.cache.insert(header);
        self.canonical_by_height.insert(header.height, header);
        self.canonical_hashes.insert(header.hash);
    }

    fn remove_canonical(&mut self, header: BlockHeader) {
        if self
            .canonical_by_height
            .get(&header.height)
            .is_some_and(|known| known.hash == header.hash)
        {
            self.canonical_by_height.remove(&header.height);
            self.canonical_hashes.remove(&header.hash);
        }
        self.cache.insert(header);
    }

    fn commit(&mut self, batch: &ChainBatch) {
        for event in &batch.events {
            match event {
                ChainEvent::Rollback(header) => self.remove_canonical(*header),
                ChainEvent::Apply(header) => self.insert_canonical(*header),
            }
        }

        if let Some(last_apply) = batch.events.iter().rev().find_map(|event| match event {
            ChainEvent::Apply(header) => Some(*header),
            ChainEvent::Rollback(_) => None,
        }) {
            self.tip = Some(last_apply);
        } else if matches!(batch.transition, ChainTransition::Duplicate) {
            self.tip = self.tip.or(batch.common_ancestor);
        }
    }
}

fn validate_same_header(left: BlockHeader, right: BlockHeader) -> Result<(), ChainError> {
    if left == right {
        Ok(())
    } else {
        Err(ChainError::InvalidParentLink {
            child_height: left.height,
            parent_height: right.height,
            expected_hash: left.hash,
            actual_hash: right.hash,
        })
    }
}

fn validate_parent_link(child: BlockHeader, parent: BlockHeader) -> Result<(), ChainError> {
    if child.parent_hash != parent.hash || child.height != parent.height + 1 {
        return Err(ChainError::InvalidParentLink {
            child_height: child.height,
            parent_height: parent.height,
            expected_hash: child.parent_hash,
            actual_hash: parent.hash,
        });
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct HeaderCache {
    capacity: usize,
    order: VecDeque<BlockHash>,
    headers: HashMap<BlockHash, BlockHeader>,
}

impl HeaderCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            headers: HashMap::with_capacity(capacity),
        }
    }

    fn insert(&mut self, header: BlockHeader) {
        if let std::collections::hash_map::Entry::Occupied(mut entry) =
            self.headers.entry(header.hash)
        {
            entry.insert(header);
            return;
        }

        if self.order.len() == self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.headers.remove(&evicted);
        }

        self.order.push_back(header.hash);
        self.headers.insert(header.hash, header);
    }

    fn get(&self, hash: BlockHash) -> Option<BlockHeader> {
        self.headers.get(&hash).copied()
    }

    fn len(&self) -> usize {
        self.headers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct MemoryResolver {
        headers: HashMap<BlockHash, BlockHeader>,
        canonical_by_hash: HashMap<BlockHash, BlockHeader>,
        canonical_by_height: BTreeMap<u64, BlockHeader>,
        header_fetches: Vec<BlockHash>,
        canonical_hash_lookups: Vec<BlockHash>,
        canonical_height_lookups: Vec<u64>,
    }

    impl MemoryResolver {
        fn with_headers(headers: impl IntoIterator<Item = BlockHeader>) -> Self {
            let mut resolver = Self::default();
            for header in headers {
                resolver.headers.insert(header.hash, header);
            }
            resolver
        }

        fn mark_canonical(&mut self, headers: impl IntoIterator<Item = BlockHeader>) {
            for header in headers {
                self.canonical_by_hash.insert(header.hash, header);
                self.canonical_by_height.insert(header.height, header);
                self.headers.insert(header.hash, header);
            }
        }
    }

    impl AncestryResolver for MemoryResolver {
        fn header_by_hash(
            &mut self,
            hash: BlockHash,
        ) -> Result<Option<BlockHeader>, ResolverError> {
            self.header_fetches.push(hash);
            Ok(self.headers.get(&hash).copied())
        }

        fn canonical_header_by_hash(
            &mut self,
            hash: BlockHash,
        ) -> Result<Option<BlockHeader>, ResolverError> {
            self.canonical_hash_lookups.push(hash);
            Ok(self.canonical_by_hash.get(&hash).copied())
        }

        fn canonical_header_at_height(
            &mut self,
            height: u64,
        ) -> Result<Option<BlockHeader>, ResolverError> {
            self.canonical_height_lookups.push(height);
            Ok(self.canonical_by_height.get(&height).copied())
        }
    }

    fn hash(value: u8) -> BlockHash {
        [value; 32]
    }

    fn header(value: u8, parent: u8, height: u64) -> BlockHeader {
        BlockHeader::new(hash(value), hash(parent), height)
    }

    fn headers(values: &[(u8, u8)]) -> Vec<BlockHeader> {
        values
            .iter()
            .enumerate()
            .map(|(height, (value, parent))| header(*value, *parent, height as u64))
            .collect()
    }

    fn event_headers(events: &[ChainEvent]) -> Vec<(bool, u8, u64)> {
        events
            .iter()
            .map(|event| {
                let applied = matches!(event, ChainEvent::Apply(_));
                let header = event.header();
                (applied, header.hash[0], header.height)
            })
            .collect()
    }

    #[test]
    fn simple_extension_emits_one_apply() {
        let chain = headers(&[(0, 0), (1, 0)]);
        let mut state = ChainState::from_canonical_chain(&chain, 4, 8).unwrap();
        let mut resolver = MemoryResolver::default();
        let new_head = header(2, 1, 2);

        let batch = state.apply(new_head, &mut resolver).unwrap();

        assert_eq!(batch.transition, ChainTransition::Extend);
        assert_eq!(batch.common_ancestor, Some(chain[1]));
        assert_eq!(batch.events, vec![ChainEvent::Apply(new_head)]);
        assert_eq!(state.tip(), Some(new_head));
        assert_eq!(state.canonical_chain(), vec![chain[0], chain[1], new_head]);
    }

    #[test]
    fn duplicate_head_is_idempotent_no_op() {
        let chain = headers(&[(0, 0), (1, 0), (2, 1)]);
        let mut state = ChainState::from_canonical_chain(&chain, 4, 8).unwrap();
        let mut resolver = MemoryResolver::default();

        let batch = state.apply(chain[2], &mut resolver).unwrap();

        assert_eq!(batch.transition, ChainTransition::Duplicate);
        assert!(batch.is_empty());
        assert_eq!(state.tip(), Some(chain[2]));
        assert_eq!(state.canonical_chain(), chain);
    }

    #[test]
    fn skipped_notification_fetches_missing_parents_and_applies_ancestor_first() {
        let chain = headers(&[(0, 0), (1, 0)]);
        let missing = header(2, 1, 2);
        let new_head = header(3, 2, 3);
        let mut state = ChainState::from_canonical_chain(&chain, 4, 8).unwrap();
        let mut resolver = MemoryResolver::with_headers([missing]);

        let batch = state.apply(new_head, &mut resolver).unwrap();

        assert_eq!(batch.transition, ChainTransition::Gap);
        assert_eq!(batch.common_ancestor, Some(chain[1]));
        assert_eq!(
            event_headers(&batch.events),
            vec![(true, 2, 2), (true, 3, 3)]
        );
        assert_eq!(resolver.header_fetches, vec![hash(2)]);
        assert_eq!(
            state.canonical_chain(),
            vec![chain[0], chain[1], missing, new_head]
        );
    }

    #[test]
    fn unknown_parent_falls_back_to_resolver_and_persisted_canonical_lookup() {
        let chain = headers(&[(0, 0), (1, 0), (2, 1), (3, 2)]);
        let replacement = header(4, 1, 2);
        let new_head = header(5, 4, 3);
        let mut state = ChainState::from_tip(chain[3], 1, 8).unwrap();
        let mut resolver = MemoryResolver::with_headers([chain[2], replacement]);
        resolver.mark_canonical(chain);

        let batch = state.apply(new_head, &mut resolver).unwrap();

        assert_eq!(batch.transition, ChainTransition::Reorg);
        assert_eq!(batch.common_ancestor, Some(header(1, 0, 1)));
        assert_eq!(
            event_headers(&batch.events),
            vec![(false, 3, 3), (false, 2, 2), (true, 4, 2), (true, 5, 3)]
        );
        assert!(resolver.header_fetches.contains(&hash(4)));
        assert!(resolver.header_fetches.contains(&hash(2)));
        assert!(resolver.canonical_hash_lookups.contains(&hash(1)));
    }

    #[test]
    fn single_block_reorg_rolls_back_then_applies() {
        let chain = headers(&[(0, 0), (1, 0), (2, 1)]);
        let replacement = header(3, 1, 2);
        let mut state = ChainState::from_canonical_chain(&chain, 4, 8).unwrap();
        let mut resolver = MemoryResolver::default();

        let batch = state.apply(replacement, &mut resolver).unwrap();

        assert_eq!(batch.transition, ChainTransition::Reorg);
        assert_eq!(batch.common_ancestor, Some(chain[1]));
        assert_eq!(
            batch.events,
            vec![
                ChainEvent::Rollback(chain[2]),
                ChainEvent::Apply(replacement)
            ]
        );
        assert_eq!(
            state.canonical_chain(),
            vec![chain[0], chain[1], replacement]
        );
    }

    #[test]
    fn deep_reorg_rolls_back_descendants_and_applies_ancestors_first() {
        let canonical = headers(&[(0, 0), (1, 0), (2, 1), (3, 2), (4, 3), (5, 4), (6, 5)]);
        let b2 = header(12, 1, 2);
        let b3 = header(13, 12, 3);
        let b4 = header(14, 13, 4);
        let b5 = header(15, 14, 5);
        let b6 = header(16, 15, 6);
        let mut state = ChainState::from_canonical_chain(&canonical, 4, 8).unwrap();
        let mut resolver = MemoryResolver::with_headers([b2, b3, b4, b5]);

        let batch = state.apply(b6, &mut resolver).unwrap();

        assert_eq!(batch.transition, ChainTransition::Reorg);
        assert_eq!(batch.common_ancestor, Some(canonical[1]));
        assert_eq!(
            event_headers(&batch.events),
            vec![
                (false, 6, 6),
                (false, 5, 5),
                (false, 4, 4),
                (false, 3, 3),
                (false, 2, 2),
                (true, 12, 2),
                (true, 13, 3),
                (true, 14, 4),
                (true, 15, 5),
                (true, 16, 6),
            ]
        );
        assert_eq!(
            state.canonical_chain(),
            vec![canonical[0], canonical[1], b2, b3, b4, b5, b6]
        );
    }

    #[test]
    fn reorg_of_a_reorg_uses_current_canonical_branch() {
        let canonical = headers(&[(0, 0), (1, 0), (2, 1), (3, 2)]);
        let b2 = header(12, 1, 2);
        let b3 = header(13, 12, 3);
        let c2 = header(22, 1, 2);
        let c3 = header(23, 22, 3);
        let mut state = ChainState::from_canonical_chain(&canonical, 8, 8).unwrap();
        let mut resolver = MemoryResolver::with_headers([b2, c2]);

        state.apply(b3, &mut resolver).unwrap();
        let batch = state.apply(c3, &mut resolver).unwrap();

        assert_eq!(batch.transition, ChainTransition::Reorg);
        assert_eq!(
            event_headers(&batch.events),
            vec![(false, 13, 3), (false, 12, 2), (true, 22, 2), (true, 23, 3)]
        );
        assert_eq!(
            state.canonical_chain(),
            vec![canonical[0], canonical[1], c2, c3]
        );
    }

    #[test]
    fn cache_eviction_does_not_define_correctness_boundary() {
        let canonical = headers(&[(0, 0), (1, 0), (2, 1), (3, 2), (4, 3)]);
        let b3 = header(13, 2, 3);
        let b4 = header(14, 13, 4);
        let mut state = ChainState::from_tip(canonical[4], 2, 8).unwrap();
        let mut resolver = MemoryResolver::with_headers([canonical[3], canonical[2], b3]);
        resolver.mark_canonical([canonical[2]]);

        assert_eq!(state.cache_len(), 1);
        assert_eq!(state.cached_header(canonical[2].hash), None);

        let batch = state.apply(b4, &mut resolver).unwrap();

        assert_eq!(batch.transition, ChainTransition::Reorg);
        assert_eq!(
            event_headers(&batch.events),
            vec![(false, 4, 4), (false, 3, 3), (true, 13, 3), (true, 14, 4)]
        );
        assert!(resolver.header_fetches.contains(&hash(13)));
        assert!(resolver.header_fetches.contains(&hash(3)));
    }

    #[test]
    fn valid_reorg_depths_up_to_max_preserve_exact_event_order() {
        for depth in 1..=6 {
            let canonical: Vec<_> = (0..=6)
                .map(|height| {
                    let value = u8::try_from(height).unwrap();
                    let parent = value.saturating_sub(1);
                    header(value, parent, height)
                })
                .collect();
            let ancestor_height = 6 - depth;
            let mut replacements = Vec::new();
            for height in (ancestor_height + 1)..=6 {
                let value = 50 + u8::try_from(height).unwrap();
                let parent = replacements.last().map_or(
                    canonical[usize::try_from(ancestor_height).unwrap()].hash[0],
                    |parent: &BlockHeader| parent.hash[0],
                );
                replacements.push(header(value, parent, height));
            }

            let mut state = ChainState::from_canonical_chain(&canonical, 3, 6).unwrap();
            let missing_parent_count = usize::try_from(depth - 1).unwrap();
            let mut resolver = MemoryResolver::with_headers(
                replacements.iter().copied().take(missing_parent_count),
            );
            let new_head = *replacements.last().unwrap();

            let batch = state.apply(new_head, &mut resolver).unwrap();

            let expected_rollbacks = ((ancestor_height + 1)..=6).rev().map(|height| {
                (
                    false,
                    canonical[usize::try_from(height).unwrap()].hash[0],
                    height,
                )
            });
            let expected_applies = replacements
                .iter()
                .map(|header| (true, header.hash[0], header.height));
            let expected: Vec<_> = expected_rollbacks.chain(expected_applies).collect();
            assert_eq!(event_headers(&batch.events), expected);
        }
    }

    #[test]
    fn unresolvable_missing_parent_fails_closed() {
        let chain = headers(&[(0, 0), (1, 0)]);
        let new_head = header(3, 2, 3);
        let mut state = ChainState::from_canonical_chain(&chain, 4, 8).unwrap();
        let mut resolver = MemoryResolver::default();

        let error = state.apply(new_head, &mut resolver).unwrap_err();

        assert_eq!(error, ChainError::MissingParent { hash: hash(2) });
        assert_eq!(state.canonical_chain(), chain);
    }

    #[test]
    fn malformed_direct_extension_height_fails_closed() {
        let chain = headers(&[(0, 0), (1, 0)]);
        let malformed = header(2, 1, 3);
        let mut state = ChainState::from_canonical_chain(&chain, 4, 8).unwrap();
        let mut resolver = MemoryResolver::default();

        let error = state.apply(malformed, &mut resolver).unwrap_err();

        assert!(matches!(error, ChainError::InvalidExtensionHeight { .. }));
        assert_eq!(state.canonical_chain(), chain);
    }

    #[test]
    fn malformed_fetched_parent_link_fails_closed() {
        let chain = headers(&[(0, 0), (1, 0)]);
        let bad_parent = header(2, 0, 7);
        let new_head = header(3, 2, 3);
        let mut state = ChainState::from_canonical_chain(&chain, 4, 8).unwrap();
        let mut resolver = MemoryResolver::with_headers([bad_parent]);

        let error = state.apply(new_head, &mut resolver).unwrap_err();

        assert!(matches!(error, ChainError::InvalidParentLink { .. }));
        assert_eq!(state.canonical_chain(), chain);
    }

    #[test]
    fn reorg_beyond_configured_max_depth_fails_closed() {
        let canonical = headers(&[(0, 0), (1, 0), (2, 1)]);
        let b1 = header(11, 0, 1);
        let b2 = header(12, 11, 2);
        let mut state = ChainState::from_canonical_chain(&canonical, 4, 1).unwrap();
        let mut resolver = MemoryResolver::with_headers([b1]);

        let error = state.apply(b2, &mut resolver).unwrap_err();

        assert_eq!(error, ChainError::MaxDepthExceeded { max_depth: 1 });
        assert_eq!(state.canonical_chain(), canonical);
    }

    #[test]
    fn finalized_boundary_violation_fails_closed() {
        let canonical = headers(&[(0, 0), (1, 0), (2, 1)]);
        let b1 = header(11, 0, 1);
        let b2 = header(12, 11, 2);
        let mut state = ChainState::from_canonical_chain(&canonical, 4, 8).unwrap();
        state.set_finalized_height(Some(1));
        let mut resolver = MemoryResolver::with_headers([b1]);

        let error = state.apply(b2, &mut resolver).unwrap_err();

        assert_eq!(
            error,
            ChainError::FinalizedBoundary {
                ancestor_height: 0,
                finalized_height: 1
            }
        );
        assert_eq!(state.canonical_chain(), canonical);
    }
}
