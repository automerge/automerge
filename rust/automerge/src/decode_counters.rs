//! Counts of the op columns decoded by the instrumented op readers, so that
//! tests can check what a read costs without timing it.
//!
//! Only `OpIter`, which builds whole ops, and `TopRows`, which reads the
//! columns a whole-document read keeps, count what they decode; readers
//! that decode a few columns for their own purposes, such as the
//! visibility and `top` index walks, are not counted.
//!
//! Compiled only into the crate's own tests, or with the `decode-counters`
//! feature. The counts are per thread.

use std::cell::Cell;

thread_local! {
    static OP_COLUMNS_DECODED: Cell<u64> = const { Cell::new(0) };
}

/// How many op column values the instrumented op readers have decoded on
/// this thread: building a whole op decodes each of its eight columns (id, object,
/// key, insert, action, value, mark and successors), so it counts eight.
pub fn op_columns_decoded() -> u64 {
    OP_COLUMNS_DECODED.with(Cell::get)
}

/// Sets this thread's counts back to zero.
pub fn reset() {
    OP_COLUMNS_DECODED.with(|count| count.set(0));
}

/// Counts `columns` column values decoded.
pub(crate) fn count_op_columns(columns: u64) {
    OP_COLUMNS_DECODED.with(|count| count.set(count.get() + columns));
}

#[cfg(test)]
mod tests {
    use super::{op_columns_decoded, reset};
    use crate::transaction::Transactable;
    use crate::{AutoCommit, ObjType, ROOT};

    /// Reading every op of a document decodes eight columns per op, whether
    /// the ops are read in turn or skipped to.
    #[test]
    fn eight_columns_are_counted_per_op_read() {
        let mut doc = AutoCommit::new();
        let map = doc.put_object(ROOT, "map", ObjType::Map).unwrap();
        for i in 0..10 {
            doc.put(&map, format!("k{i}"), i).unwrap();
        }
        doc.put(&map, "k0", "again").unwrap();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, 1).unwrap();
        doc.commit();
        let ops = doc.doc.ops();
        assert_eq!(ops.len(), 14);

        reset();
        assert_eq!(op_columns_decoded(), 0);
        assert_eq!(ops.iter().count(), 14);
        assert_eq!(op_columns_decoded(), 14 * 8);

        reset();
        let mut iter = ops.iter();
        assert!(iter.nth(5).is_some());
        assert!(iter.nth(3).is_some());
        assert_eq!(op_columns_decoded(), 2 * 8);
    }
}
