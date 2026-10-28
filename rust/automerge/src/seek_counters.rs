//! Counts of how often a read positions the op columns at an object's ops,
//! so that tests can check what a read costs without timing it.
//!
//! Compiled only into the crate's own tests, or with the `seek-counters`
//! feature. The counts are per thread.

use std::cell::Cell;

thread_local! {
    static TOP_OPS_SEEKS: Cell<u64> = const { Cell::new(0) };
}

/// How many times this thread has started reading the top ops of a range of
/// the op set: each start seeks every op column, and the `top` and
/// visibility indexes, to the start of the range. Reading one object's top
/// ops is one seek, so hydrating a document object by object seeks once per
/// object.
pub fn top_ops_seeks() -> u64 {
    TOP_OPS_SEEKS.with(Cell::get)
}

/// Sets this thread's counts back to zero.
pub fn reset() {
    TOP_OPS_SEEKS.with(|count| count.set(0));
}

pub(crate) fn count_top_ops_seek() {
    TOP_OPS_SEEKS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
mod tests {
    use super::{reset, top_ops_seeks};
    use crate::transaction::Transactable;
    use crate::{AutoCommit, ObjType, ReadDoc, ROOT};

    /// Hydrating seeks once for each object it reads, and only then.
    #[test]
    fn a_seek_is_counted_per_object_read() {
        let mut doc = AutoCommit::new();
        let a = doc.put_object(ROOT, "a", ObjType::Map).unwrap();
        doc.put(&a, "x", 1).unwrap();
        let b = doc.put_object(&a, "b", ObjType::Map).unwrap();
        doc.put(&b, "y", 2).unwrap();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, 3).unwrap();
        doc.insert_object(&list, 1, ObjType::Table).unwrap();
        doc.put(ROOT, "z", 4).unwrap();

        reset();
        assert_eq!(top_ops_seeks(), 0);
        doc.hydrate(&ROOT, None).unwrap();
        // The root, `a`, `b`, `list` and the table in it.
        assert_eq!(top_ops_seeks(), 5);

        reset();
        doc.hydrate(&b, None).unwrap();
        assert_eq!(top_ops_seeks(), 1);

        reset();
        doc.commit();
        assert_eq!(top_ops_seeks(), 0);
    }
}
