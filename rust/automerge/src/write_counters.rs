//! Counts of the work done writing ops into a document, so tests can check
//! how much a write costs without timing it.
//!
//! Compiled only for the crate's own tests and with the `write-counters`
//! feature; otherwise the counting calls compile to nothing. The counts are
//! per thread, so tests running in parallel do not see each other's work.

use std::cell::Cell;

/// What [`counts`] returns: totals since the last [`reset`] on this thread.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// Ops spliced into the op set's columns, each column decoded around the
    /// splice point and encoded again.
    pub ops_spliced: u64,
    /// Ops encoded into the op columns of a change.
    pub ops_encoded: u64,
    /// Ops a transaction still held, one record per op, when it committed.
    pub ops_held: u64,
    /// Bytes of op columns decoded to build the op set's columns.
    pub column_bytes_decoded: u64,
}

thread_local! {
    static COUNTS: Cell<Counts> = const {
        Cell::new(Counts {
            ops_spliced: 0,
            ops_encoded: 0,
            ops_held: 0,
            column_bytes_decoded: 0,
        })
    };
}

/// Sets this thread's counts to zero.
pub fn reset() {
    COUNTS.with(|c| c.set(Counts::default()));
}

/// This thread's counts since the last [`reset`].
pub fn counts() -> Counts {
    COUNTS.with(|c| c.get())
}

pub(crate) fn add(f: impl FnOnce(&mut Counts)) {
    COUNTS.with(|c| {
        let mut counts = c.get();
        f(&mut counts);
        c.set(counts);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::Transactable;
    use crate::{hydrate_map, Automerge, ObjType, ReadDoc, ROOT};

    fn actor() -> crate::ActorId {
        crate::ActorId::from(&[1u8; 16][..])
    }

    #[test]
    fn local_ops_are_spliced_held_and_encoded_once_each() {
        let mut doc = Automerge::new().with_actor(actor());
        reset();
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.put(ROOT, "b", 2).unwrap();
        let list = tx.put_object(ROOT, "l", ObjType::List).unwrap();
        tx.insert(&list, 0, "x").unwrap();
        let before_commit = counts();
        tx.commit();
        let after = counts();
        assert_eq!(before_commit.ops_spliced, 4);
        assert_eq!(before_commit.ops_encoded, 0);
        assert_eq!(after.ops_held, 4);
        assert_eq!(after.ops_encoded, 4);
        assert_eq!(after.ops_spliced, 4);
    }

    #[test]
    fn loading_a_document_decodes_its_op_columns() {
        let mut doc = Automerge::new().with_actor(actor());
        doc.init_from_hydrate(&hydrate_map! { "k" => "v", "n" => 1_i64 })
            .unwrap();
        let saved = doc.save();
        reset();
        let loaded = Automerge::load(&saved).unwrap();
        let c = counts();
        assert!(c.column_bytes_decoded > 0);
        // Every op column byte comes from the saved document.
        assert!(c.column_bytes_decoded < saved.len() as u64);
        assert_eq!(c.ops_spliced, 0);
        assert_eq!(c.ops_held, 0);
        assert_eq!(loaded.get(ROOT, "k").unwrap().unwrap().0, "v".into());
    }

    #[test]
    fn counts_are_per_thread() {
        reset();
        add(|c| c.ops_held += 3);
        std::thread::spawn(|| {
            assert_eq!(counts(), Counts::default());
            add(|c| c.ops_held += 5);
        })
        .join()
        .unwrap();
        assert_eq!(counts().ops_held, 3);
    }
}
