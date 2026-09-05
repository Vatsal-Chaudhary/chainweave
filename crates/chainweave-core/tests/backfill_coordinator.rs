use std::collections::BTreeMap;

use chainweave_core::{
    BackfillError, BackfillRange, BackfillSource, BlockHash, BlockHeader, FetchedRange,
    OrderedCommitCoordinator, RangeCommitSink,
};

#[derive(Debug, Default)]
struct DeterministicSource {
    ranges: BTreeMap<u64, FetchedRange<u64>>,
}

impl DeterministicSource {
    fn insert(&mut self, range: FetchedRange<u64>) {
        self.ranges.insert(range.range.from_block, range);
    }
}

impl BackfillSource for DeterministicSource {
    type Error = BackfillError;
    type Log = u64;

    fn fetch_range(
        &mut self,
        range: BackfillRange,
    ) -> Result<FetchedRange<Self::Log>, Self::Error> {
        self.ranges
            .remove(&range.from_block)
            .filter(|fetched| fetched.range == range)
            .ok_or(BackfillError::InvalidRange {
                from_block: range.from_block,
                to_block: range.to_block,
            })
    }
}

#[derive(Debug, Default)]
struct RecordingSink {
    committed: Vec<BackfillRange>,
}

impl RangeCommitSink<u64> for RecordingSink {
    type Error = String;

    fn commit_range(&mut self, fetched: FetchedRange<u64>) -> Result<(), Self::Error> {
        self.committed.push(fetched.range);
        Ok(())
    }
}

#[test]
fn deterministic_source_reorg_invalidation_discards_fetched_suffix_before_commit() {
    let old_suffix = fetched(BackfillRange::new(10, 10).unwrap(), &[header(10, 9, 10)]);
    let pending_suffix = fetched(
        BackfillRange::new(11, 12).unwrap(),
        &[header(11, 10, 11), header(12, 11, 12)],
    );
    let mut source = DeterministicSource::default();
    source.insert(old_suffix);
    source.insert(pending_suffix);

    let replacement_anchor = header(99, 8, 9);
    let mut coordinator = OrderedCommitCoordinator::new(10, 12, Some(replacement_anchor)).unwrap();
    let mut sink = RecordingSink::default();

    let fetched_later = source
        .fetch_range(BackfillRange::new(11, 12).unwrap())
        .unwrap();
    assert_eq!(
        coordinator
            .push(fetched_later, &mut sink)
            .unwrap()
            .committed_ranges,
        0
    );

    let fetched_old = source
        .fetch_range(BackfillRange::new(10, 10).unwrap())
        .unwrap();
    let error = coordinator.push(fetched_old, &mut sink).unwrap_err();

    assert!(matches!(
        error,
        BackfillError::FetchedSuffixInvalidated {
            refetch_from: 10,
            expected_parent,
            actual_parent,
        } if expected_parent == hash(99) && actual_parent == hash(9)
    ));
    assert!(sink.committed.is_empty());
    assert_eq!(coordinator.next_height(), 10);
}

fn fetched(range: BackfillRange, headers: &[BlockHeader]) -> FetchedRange<u64> {
    FetchedRange::new(
        range,
        headers.to_vec(),
        headers.iter().map(|header| header.height).collect(),
    )
    .unwrap()
}

fn header(value: u8, parent: u8, height: u64) -> BlockHeader {
    BlockHeader::new(hash(value), hash(parent), height)
}

fn hash(value: u8) -> BlockHash {
    [value; 32]
}
