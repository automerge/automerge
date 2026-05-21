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

    fn from_meta(op_set: &OpSet, mut changes: Vec<BundleMetadata<'_>>) -> Bundle {
        // Sort changes by start_op before encoding the change columns.
        // This is a valid topological sort (a change's start_op is strictly
        // greater than the max_op of any of its deps, which is in turn ≥ those
        // deps' start_ops) so the bundle's "internal deps come first" invariant
        // is preserved. Without this, change columns are encoded in whatever
        // order callers happened to provide hashes — which for fragment
        // bundling (topological/iteration order) thrashes the delta-encoded
        // start_op / max_op / seq / timestamp columns and inflates output by
        // 10–20× on common workloads.
        changes.sort_by(|a, b| {
            a.start_op
                .cmp(&b.start_op)
                .then_with(|| a.actor.cmp(&b.actor))
        });
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
        &self.storage.actors
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

    pub fn bytes(&self) -> &[u8] {
        &self.storage.bytes
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
    use crate::{Automerge, ROOT};

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
