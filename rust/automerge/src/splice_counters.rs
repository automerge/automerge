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
/// start. Putting one op into a document is one walk; a
/// [`batch_write`](crate::transaction::Transactable::batch_write) is one
/// walk for every op it holds back and lands together.
pub fn op_splices() -> u64 {
    OP_SPLICES.with(Cell::get)
}

/// How many times this thread has walked the successor columns to mark
/// the ops a local write overwrites or increments: each walk positions a
/// cursor in the `succ` columns and the visibility indexes from the start.
/// A put over one existing value is one walk; a put over two conflicting
/// values is two; the successors of the ops a `batch_write` lands together
/// are one walk.
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
    use crate::transaction::{BatchWrite, Transactable};
    use crate::{ActorId, AutoCommit, ObjType, ReadDoc, ScalarValue, ROOT};

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

    /// A fixed xorshift generator, so the document and the writes are the
    /// same on every run.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    /// A loaded document of `maps` maps under the root, each holding `keys`
    /// keys (every fourth a counter), saved and loaded again.
    fn large_document(maps: usize, keys: usize) -> AutoCommit {
        let mut rng = Rng(0x005e_ed0f_ba7c);
        let mut doc = AutoCommit::new().with_actor(ActorId::from([1u8; 16]));
        for m in 0..maps {
            let map = doc
                .put_object(ROOT, format!("map{m}"), ObjType::Map)
                .unwrap();
            for k in 0..keys {
                if k.is_multiple_of(4) {
                    doc.put(&map, format!("k{k}"), ScalarValue::counter(0))
                        .unwrap();
                } else {
                    doc.put(&map, format!("k{k}"), rng.next() as i64).unwrap();
                }
            }
        }
        doc.commit();
        AutoCommit::load(&doc.save()).unwrap()
    }

    /// `n` writes spread over the document: overwrites of existing keys,
    /// new keys and counter increments, each key at most once.
    fn writes(
        doc: &AutoCommit,
        maps: usize,
        keys: usize,
        n: usize,
    ) -> Vec<(crate::ObjId, String, BatchWrite)> {
        let mut rng = Rng(0x0dd_ba11_5eed);
        let objs: Vec<_> = (0..maps)
            .map(|m| doc.get(ROOT, format!("map{m}")).unwrap().unwrap().1)
            .collect();
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            let m = rng.next() as usize % maps;
            let k = rng.next() as usize % (keys + keys / 4);
            if !seen.insert((m, k)) {
                continue;
            }
            let write = if k < keys && k.is_multiple_of(4) {
                BatchWrite::Increment(1 + (rng.next() % 9) as i64)
            } else {
                BatchWrite::Put(ScalarValue::Int(rng.next() as i64))
            };
            out.push((objs[m].clone(), format!("k{k}"), write));
        }
        out
    }

    /// Writing many map keys into a large loaded document one at a time
    /// walks the op columns once per op and the successor columns once
    /// per overwritten value; `batch_write` makes the same ops, and the
    /// same save, with one walk of each.
    #[test]
    fn batch_write_walks_the_columns_once() {
        const MAPS: usize = 8;
        const KEYS: usize = 1_000;
        const N: usize = 2_000;
        let base = large_document(MAPS, KEYS);
        let ws = writes(&base, MAPS, KEYS, N);

        let mut seq = base.clone();
        reset();
        for (obj, key, write) in ws.clone() {
            match write {
                BatchWrite::Put(v) => seq.put(&obj, key, v).unwrap(),
                BatchWrite::Increment(n) => seq.increment(&obj, key, n).unwrap(),
            }
        }
        let sequential = (op_splices(), succ_splices());

        let mut bat = base.clone();
        reset();
        let ids = bat.batch_write(ws).unwrap();
        let batched = (op_splices(), succ_splices());

        assert_eq!(ids.len(), N);
        assert!(ids.iter().all(Option::is_some));
        assert_eq!(seq.get_heads(), bat.get_heads());
        assert_eq!(seq.save(), bat.save());

        // Every write makes an op; every overwrite and increment marks the
        // one value it replaces.
        assert_eq!(sequential, (N as u64, 1_571));
        // Walks of both kinds, one at a time: 2,000 + 1,571 = 3,571.
        // Batched: 1 + 1 = 2. The gate sits a tenth of the way from the
        // batched count to the sequential one: 2 + (3,571 - 2) / 10 = 358.
        assert!(batched.0 + batched.1 <= 358);
    }
}
