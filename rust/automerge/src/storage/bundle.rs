use crate::change_graph::ChangeGraph;
use crate::op_set2::change::{ActorMapper, BuildChangeMetadata};
use crate::op_set2::OpSet;
use crate::storage::change::{Unverified, Verified};
use crate::storage::{parse, Header};
use crate::types::{ActorId, ChangeHash};
use crate::{AutomergeError, Change};

use std::borrow::Cow;

mod builder;
mod error;
mod meta;
mod storage;

pub use builder::BundleChangeIter;

pub(crate) use builder::{BundleBuilder, BundleChangeIterUnverified, OpIter, OpIterUnverified};
pub(crate) use error::ParseError;
pub(crate) use meta::BundleMetadata;
pub(crate) use storage::BundleStorage;

/// EXPERIMENTAL: A set of changes compressed into a bundle
///
/// Bundles are produced by [`Automerge::bundle`](crate::Automerge::bundle) and
/// contain a set of compressed changes which is not necessarily the whole
/// document. A bundle can be loaded using
/// [`Automerge::load_incremental`](crate::Automerge::load_incremental) but can
/// also be loaded using `TryFrom<&[u8]>` in order to examine the contents of
/// the bundle.
///
/// This feature is experimental, the file format for bundles may still change
/// so do not use this feature in systems where you expect data to stick around
#[derive(Debug)]
pub struct Bundle {
    pub(crate) storage: BundleStorage<'static, Verified>,
}

impl Bundle {
    pub(crate) fn for_hashes<I>(
        op_set: &OpSet,
        change_graph: &ChangeGraph,
        hashes: I,
    ) -> Result<Bundle, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let changes = change_graph
            .get_bundle_metadata(hashes)
            .collect::<Result<_, _>>()?;
        Ok(Self::from_meta(op_set, changes))
    }

    fn from_meta(op_set: &OpSet, changes: Vec<BundleMetadata<'_>>) -> Bundle {
        let min = changes
            .iter()
            .map(|c| c.start_op as usize)
            .min()
            .unwrap_or(0);
        let max = changes.iter().map(|c| c.max_op as usize).max().unwrap_or(0) + 1;

        let mapper = ActorMapper::new(&op_set.actors);

        let mut collector = BundleBuilder::from_change_meta(changes, mapper);

        for op in op_set.iter_ctr_range(min..max) {
            let op_id = op.id;
            let op_succ = op.succ();
            collector.process_op(op);

            for id in op_succ {
                collector.process_succ(op_id, id);
            }
        }

        collector.finish()
    }

    pub(crate) fn new_from_unverified(
        stored: BundleStorage<'static, Unverified>,
    ) -> Result<Self, ParseError> {
        Ok(Self {
            storage: stored.verify()?,
        })
    }

    pub fn actors(&self) -> &[ActorId] {
        self.storage.actors.as_slice()
    }

    pub fn authors(&self) -> &[Vec<u8>] {
        &[]
    }

    pub fn iter_changes(&self) -> BundleChangeIter<'_> {
        self.storage.iter_change_meta()
    }

    pub fn to_changes(&self) -> Result<Vec<Change>, AutomergeError> {
        self.storage
            .to_changes()
            .map_err(|e| AutomergeError::Unbundle(Box::new(e)))
    }

    /// On-disk form of the bundle, with per-column DEFLATE applied where
    /// each column is large enough to benefit. Falls back to the
    /// uncompressed buffer for bundles whose construction predates the
    /// per-column compression pass (or that were parsed from input with no
    /// compressed columns).
    pub fn bytes(&self) -> &[u8] {
        match &self.storage.compressed_bytes {
            Some(c) => c,
            None => &self.storage.bytes,
        }
    }

    pub fn deps(&self) -> &[ChangeHash] {
        self.storage.deps()
    }
}

#[derive(Clone, Debug)]
pub struct BundleChange<'a> {
    pub actor: usize,
    pub author: Option<usize>,
    pub seq: u64,
    pub start_op: u64,
    pub max_op: u64,
    pub timestamp: i64,
    pub message: Option<Cow<'a, str>>,
    pub deps: Vec<u64>,
    pub extra: Cow<'a, [u8]>,
}

impl<'a> From<BundleChange<'a>> for BuildChangeMetadata<'a> {
    fn from(bundle: BundleChange<'a>) -> Self {
        BuildChangeMetadata {
            actor: bundle.actor,
            seq: bundle.seq,
            start_op: bundle.start_op,
            max_op: bundle.max_op,
            timestamp: bundle.timestamp,
            message: bundle.message,
            deps: bundle.deps,
            extra: bundle.extra,
            builder: 0,
        }
    }
}

impl<'a> TryFrom<&'a [u8]> for Bundle {
    type Error = InvalidBundle;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        let input = parse::Input::new(bytes);
        let (i, header) = Header::parse::<crate::storage::chunk::error::Header>(input)
            .map_err(|e| InvalidBundle(format!("invalid header: {}", e)))?;
        let (_i, bundle) = BundleStorage::parse_following_header(i, header)
            .map_err(|e| InvalidBundle(format!("invalid contents: {}", e)))?;
        let verified = bundle
            .verify()
            .map_err(|e| InvalidBundle(format!("unable to verify ops: {}", e)))?;
        Ok(Self {
            storage: verified.into_owned(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid bundle: {0}")]
pub struct InvalidBundle(String);

#[cfg(test)]
mod test {
    use crate::transaction::Transactable;
    use crate::{ActorId, Automerge, Change, ROOT};

    /// Regression for wire-byte determinism, not just document convergence.
    /// Two actors concurrently delete the same prior write. The bundle builder
    /// reconstructs those delete operations from successor links and buffers
    /// them in a HashMap. Before `flush_deletes` sorted their IDs, randomized
    /// map iteration could encode the same changes in a different byte order.
    #[test]
    fn bundle_concurrent_deletions_are_deterministic() {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::from(&[0][..]));
        let mut tx = doc.transaction();
        tx.put(ROOT, "value", 1).unwrap();
        tx.commit();
        let mut left = doc.clone();
        left.set_actor(ActorId::from(&[1][..]));
        let mut right = doc.clone();
        right.set_actor(ActorId::from(&[2][..]));
        for branch in [&mut left, &mut right] {
            let mut tx = branch.transaction();
            tx.delete(ROOT, "value").unwrap();
            tx.commit();
        }
        left.merge(&mut right).unwrap();
        let hashes: Vec<_> = left.get_changes(&[]).iter().map(Change::hash).collect();
        let expected = left
            .bundle(hashes.iter().copied())
            .unwrap()
            .bytes()
            .to_vec();
        // Each call creates a new builder with a freshly seeded HashMap.
        // Repetition exercises different map iteration orders; reversing the
        // input also checks that callers' hash order cannot affect the bytes.
        for _ in 0..20 {
            assert_eq!(
                left.bundle(hashes.iter().rev().copied()).unwrap().bytes(),
                expected
            );
        }
        // Stable encoding must still retain both deletion changes, not merely
        // reconstruct their shared visible result (an absent "value" key).
        let mut loaded = Automerge::new();
        loaded.load_incremental(&expected).unwrap();
        assert_eq!(loaded.get_heads(), left.get_heads());
        assert_eq!(loaded.hydrate(None), left.hydrate(None));
    }

    #[test]
    fn make_bundle() {
        let mut doc = Automerge::new();

        let mut tx = doc.transaction();
        tx.put(&ROOT, "aaa", "aaa").unwrap();
        let (Some(h0), _) = tx.commit() else { panic!() };

        let mut d2 = doc.fork();

        let mut tx = doc.transaction();
        tx.put(&ROOT, "bbb", "bbb").unwrap();
        let (Some(h1), _) = tx.commit() else { panic!() };

        let mut tx = doc.transaction();
        tx.put(&ROOT, "ccc", "ccc").unwrap();
        let (Some(h2), _) = tx.commit() else { panic!() };

        let bundle = doc.bundle([h0, h1, h2]).unwrap();
        let changes = bundle.to_changes().unwrap();
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].max_op(), 1);
        assert_eq!(changes[0].hash(), h0);
        assert_eq!(changes[1].max_op(), 2);
        assert_eq!(changes[1].hash(), h1);
        assert_eq!(changes[2].max_op(), 3);
        assert_eq!(changes[2].hash(), h2);

        d2.load_incremental(bundle.bytes()).unwrap();

        assert_eq!(doc.save(), d2.save());

        let bundle = doc.bundle([h0, h2]).unwrap();
        let changes = bundle.to_changes().unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].max_op(), 1);
        assert_eq!(changes[0].hash(), h0);
        assert_eq!(changes[1].max_op(), 3);
        assert_eq!(changes[1].hash(), h2);
    }
}
