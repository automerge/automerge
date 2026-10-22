//! Counts of how often local writes walk the op set's columns, so that
//! tests can check what a write costs without timing it.
//!
//! Compiled only into the crate's own tests, or with the `splice-counters`
//! feature. The counts are per thread.

use std::cell::Cell;

thread_local! {
    static OP_SPLICES: Cell<u64> = const { Cell::new(0) };
    static SUCC_SPLICES: Cell<u64> = const { Cell::new(0) };
}

/// How many times this thread has walked the op columns to place new ops:
/// each walk positions a cursor in every op column and index from the
/// start. Putting one op into a document is one walk.
pub fn op_splices() -> u64 {
    OP_SPLICES.with(Cell::get)
}

/// How many times this thread has walked the successor columns to mark
/// the ops a local write overwrites or increments: each walk positions a
/// cursor in the `succ` columns and the visibility indexes from the start.
/// A put over one existing value is one walk; a put over two conflicting
/// values is two.
pub fn succ_splices() -> u64 {
    SUCC_SPLICES.with(Cell::get)
}

/// Sets this thread's counts back to zero.
pub fn reset() {
    OP_SPLICES.with(|count| count.set(0));
    SUCC_SPLICES.with(|count| count.set(0));
}

pub(crate) fn count_op_splice() {
    OP_SPLICES.with(|count| count.set(count.get() + 1));
}

pub(crate) fn count_succ_splices(n: usize) {
    SUCC_SPLICES.with(|count| count.set(count.get() + n as u64));
}

#[cfg(test)]
mod tests {
    use super::{op_splices, reset, succ_splices};
    use crate::transaction::Transactable;
    use crate::{ActorId, AutoCommit, ObjType, ROOT};

    /// A put walks the op columns once, and the successor columns once for
    /// each value it overwrites.
    #[test]
    fn a_walk_is_counted_per_op_and_per_overwritten_value() {
        let mut doc = AutoCommit::new().with_actor(ActorId::from([1u8; 16]));
        let m = doc.put_object(ROOT, "m", ObjType::Map).unwrap();
        doc.put(&m, "a", 1).unwrap();
        doc.commit();

        reset();
        assert_eq!((op_splices(), succ_splices()), (0, 0));
        // A new key: one op, nothing overwritten.
        doc.put(&m, "b", 1).unwrap();
        assert_eq!((op_splices(), succ_splices()), (1, 0));

        // An existing key: one op, one value overwritten.
        reset();
        doc.put(&m, "a", 2).unwrap();
        assert_eq!((op_splices(), succ_splices()), (1, 1));

        // A put of the value already there makes no op.
        reset();
        doc.put(&m, "a", 2).unwrap();
        assert_eq!((op_splices(), succ_splices()), (0, 0));

        // Two conflicting values: one op, two overwritten.
        let mut other = doc.fork().with_actor(ActorId::from([2u8; 16]));
        other.put(&m, "c", "theirs").unwrap();
        doc.put(&m, "c", "ours").unwrap();
        doc.merge(&mut other).unwrap();
        reset();
        doc.put(&m, "c", "both").unwrap();
        assert_eq!((op_splices(), succ_splices()), (1, 2));

        // Reading counts nothing.
        reset();
        doc.commit();
        let _ = doc.save();
        assert_eq!((op_splices(), succ_splices()), (0, 0));
    }
}
