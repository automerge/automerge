//! Read-only views of a column's internal layout, for inspectors and
//! debugging tools.  Enabled by the `debug` feature; the shapes here may
//! change in any release.

use super::{Node, NodeId, SlabAggregate, SlabBTree};
use crate::column::{Column, WeightFn};
use crate::delta::{DeltaColumn, DeltaValue};
use crate::prefix::{PrefixColumn, PrefixValue};
use crate::{Codec, ColumnValueRef};

/// One slab of a column as it sits in memory.
#[derive(Debug, Clone)]
pub struct SlabDebug {
    /// Items held by the slabs before this one.
    pub item_start: usize,
    /// Items held by this slab.
    pub len: usize,
    /// Segments counted against the column's per-slab limit.
    pub segments: usize,
    /// The encoded bytes, a self-contained stretch of the column's wire
    /// encoding.
    pub bytes: Vec<u8>,
    /// The encoding's bookkeeping about the slab's last run.
    pub tail: String,
    /// The slab's weight in the column index.
    pub weight: String,
}

/// A node of a column's slab index, from the root down.
#[derive(Debug, Clone)]
pub enum NodeDebug {
    /// One weight per slab, in slab order.
    Leaf { weights: Vec<String> },
    /// One entry per child: its subtree's merged weight, how many slabs it
    /// covers, and the child itself.
    Internal {
        children: Vec<(String, usize, NodeDebug)>,
    },
}

impl<A: SlabAggregate> SlabBTree<A> {
    fn debug_node(&self, id: NodeId) -> NodeDebug {
        match self.node(id) {
            Node::Leaf(l) => NodeDebug::Leaf {
                weights: l.aggs.iter().map(|a| format!("{a:?}")).collect(),
            },
            Node::Internal(n) => NodeDebug::Internal {
                children: n
                    .children
                    .iter()
                    .map(|c| (format!("{:?}", c.agg), c.slab_count, self.debug_node(c.id)))
                    .collect(),
            },
        }
    }
}

impl<T, C, WF> Column<T, C, WF, SlabBTree<WF::Weight>>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
{
    /// Every slab's bytes and bookkeeping, in order.  Copies all the bytes.
    pub fn debug_slabs(&self) -> Vec<SlabDebug> {
        let mut item_start = 0;
        self.slabs
            .iter()
            .map(|s| {
                let d = SlabDebug {
                    item_start,
                    len: s.len,
                    segments: s.segments,
                    bytes: s.data.clone(),
                    tail: format!("{:?}", s.tail),
                    weight: format!("{:?}", WF::compute(s)),
                };
                item_start += s.len;
                d
            })
            .collect()
    }

    /// The slab index's nodes from the root down.
    pub fn debug_index(&self) -> NodeDebug {
        self.index.debug_node(self.index.root)
    }
}

impl<T: PrefixValue, C: Codec> PrefixColumn<T, C> {
    /// See [`Column::debug_slabs`].
    pub fn debug_slabs(&self) -> Vec<SlabDebug> {
        self.col.debug_slabs()
    }

    /// See [`Column::debug_index`].
    pub fn debug_index(&self) -> NodeDebug {
        self.col.debug_index()
    }
}

impl<T: DeltaValue, C: Codec> DeltaColumn<T, C> {
    /// See [`Column::debug_slabs`]; slab bytes hold deltas, the first
    /// relative to the previous slab's last value.
    pub fn debug_slabs(&self) -> Vec<SlabDebug> {
        self.col.debug_slabs()
    }

    /// See [`Column::debug_index`].
    pub fn debug_index(&self) -> NodeDebug {
        self.col.debug_index()
    }
}
