use std::{borrow::Cow, ops::Range};

use bundle_change_columns::BundleChangeColumns;
use bundle_op_columns::BundleOpColumns;
use reify_deletes::ReifiedDeletes;

use crate::{
    change_graph::ChangeGraph, clock::Clock, op_set2::OpSet, types::OpId, ActorId, ChangeHash,
};

use super::{CompressConfig, Header};

mod bundle_change_columns;
mod bundle_op_columns;
mod reify_deletes;

pub(crate) struct CommitRange<'a> {
    start: &'a [ChangeHash],
    end: &'a [ChangeHash],
}

#[derive(Debug)]
struct CommitRangeClocks {
    start: Clock,
    end: Clock,
}

impl CommitRangeClocks {
    fn covers(&self, op_id: &OpId) -> bool {
        !self.start.covers(op_id) && self.end.covers(op_id)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Bundle<'a> {
    bytes: Cow<'a, [u8]>,
    #[allow(dead_code)]
    compressed_bytes: Option<Cow<'a, [u8]>>,
    header: Header,
    actors: Vec<ActorId>,
    deps: Vec<ChangeHash>,
    heads: Vec<ChangeHash>,
    ops: BundleOpColumns,
    change_metadata: BundleChangeColumns,
    change_bytes: Range<usize>,
}

impl<'a> Bundle<'a> {
    pub(crate) fn new(
        op_set: &OpSet,
        change_graph: &ChangeGraph,
        commit_ranges: &[CommitRange],
        compress: CompressConfig,
    ) -> Bundle<'static> {
        let mut out = Vec::new();

        let range_clocks = commit_ranges
            .iter()
            .map(|range| CommitRangeClocks {
                start: change_graph.clock_for_heads(&range.start),
                end: change_graph.clock_for_heads(&range.end),
            })
            .collect::<Vec<_>>();

        let reified_deletes = ReifiedDeletes::new(op_set, &range_clocks);

        // encode the bundle ops
        let ops =
            bundle_op_columns::BundleOpColumns::new(&mut out, &reified_deletes, &range_clocks);

        // Write the filtered ops to BundleOpColumns
        // Filter the change graph to only include changes that are between start and end
        // Write the changes to BundleChangeColumns
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BundleChangeMetadata<'a> {
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) max_op: u64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<Cow<'a, str>>,
    pub(crate) deps: Vec<u64>,
    pub(crate) extra: Cow<'a, [u8]>,
}
