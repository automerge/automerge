//! Per-op index of each counter's current total.
//!
//! The op columns store only a counter's *base* value; its current total is
//! `base + Σ increments`, which is otherwise re-derived by walking the op's
//! successor list on every read. [`CounterIndex`] caches that total per op, so
//! reading it costs an indexed column lookup and is independent of how many
//! increments the counter has accumulated.
//!
//! Like the other members of [`Indexes`] this is derived state, rebuilt on load
//! & by [`OpSet::recompute_indexes`], and kept in step with the op columns by
//! the same writers that maintain `visible` and `inc`.
//!
//! The semantics for calculating the total adheres to the visibility of ops,
//! including masking from the write-frontier. The index is therefore
//! authoritative for any current read (masked or not), and historical reads
//! must still sum against their clock.
//!
//! [`Indexes`]: super::super::columns::Indexes
//! [`OpSet::recompute_indexes`]: crate::op_set2::OpSet::recompute_indexes

#[cfg(test)]
mod tests;

use crate::op_set2::op::{OpLike, SuccInsert};

/// Each counter op's current total, `None` for non-counter ops.
#[derive(Debug, Clone, Default)]
pub(crate) struct CounterIndex {
    totals: hexane::Column<Option<i64>>,
}

impl CounterIndex {
    /// Construct an empty [`CounterIndex`].
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Build a [`CounterIndex`] from one slot per op.
    pub(crate) fn from_values(totals: Vec<Option<i64>>) -> Self {
        Self {
            totals: hexane::Column::from_values(totals),
        }
    }

    /// Number of ops indexed.
    ///
    /// This must equal the length of the op set.
    pub(crate) fn len(&self) -> usize {
        self.totals.len()
    }

    /// The current total of the counter op at `pos`
    ///
    /// Returns `None` if the op at `pos` is not a counter.
    pub(crate) fn get(&self, pos: usize) -> Option<i64> {
        self.totals.get(pos).flatten()
    }

    /// Ops entering or leaving the set at `pos`, remove `del` slots, and then
    /// insert one slot per op in `ops`.
    ///
    /// An entering counter op is summed from its own successors
    /// ([`OpLike::counter_total`]), so an op carried in with increments already
    /// attached (e.g. from a loaded document) lands with the right total.
    pub(crate) fn splice<'b, O, I>(&mut self, pos: usize, del: usize, ops: I)
    where
        O: OpLike + 'b,
        I: Iterator<Item = &'b O>,
    {
        self.totals.splice(pos, del, ops.map(O::counter_total));
    }

    /// A successor was added to the op at [`SuccInsert::pos`].
    ///
    /// Only an unmasked increment changes the total.
    pub(crate) fn add_succ(&mut self, insert: &SuccInsert) {
        self.adjust(insert.pos, insert.inc, insert.masked);
    }

    /// The successor recorded by `insert` was removed again.
    ///
    /// This is the inverse of [`Self::add_succ`].
    pub(crate) fn undo_succ(&mut self, insert: &SuccInsert) {
        self.adjust(insert.pos, insert.inc.map(i64::wrapping_neg), insert.masked);
    }

    /// Add `delta` to the total at `pos`.
    ///
    /// A `None` slot (non-counter op) is left alone, and `add_succ_with_mask`
    /// already drops increments on non-counters.
    fn adjust(&mut self, pos: usize, inc: Option<i64>, masked: bool) {
        if !masked {
            if let Some((inc, total)) = inc.zip(self.get(pos)) {
                self.totals.splice(pos, 1, [Some(total.wrapping_add(inc))]);
            }
        }
    }

    /// Check the index against the op set it belongs to.
    ///
    /// For every op, the slot must be `Some(base + Σ inc)` over the successors
    /// covered by `mask` (all of them when `mask` is `None`) if the op is a
    /// counter, and `None` otherwise. This is the oracle the tests and
    /// `slow_path_assertions` rely on.
    #[cfg(any(test, feature = "slow_path_assertions"))]
    pub(crate) fn assert_matches(
        &self,
        op_set: &crate::op_set2::OpSet,
        mask: Option<&crate::clock::Clock>,
    ) {
        use crate::op_set2::types::ScalarValue;

        assert_eq!(
            self.len(),
            op_set.len(),
            "CounterIndex length does not match op set length"
        );
        for op in op_set.iter() {
            let expected = match op.value {
                ScalarValue::Counter(base) => {
                    let total = op
                        .succ_inc()
                        .filter(|(id, _)| mask.is_none_or(|m| m.covers(id)))
                        .filter_map(|(_, inc)| inc)
                        .fold(base, i64::wrapping_add);
                    Some(total)
                }
                _ => None,
            };
            assert_eq!(
                self.get(op.pos),
                expected,
                "CounterIndex disagrees at pos {} (op {:?})",
                op.pos,
                op.id
            );
        }
    }
}
