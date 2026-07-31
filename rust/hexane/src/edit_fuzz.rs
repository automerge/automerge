//! Deep fuzzing for [`Column::edit`](crate::Column::edit).
//!
//! The cursor rewrites slab bytes in place across an open-ended sequence
//! of edits, so a bug here is silent data corruption rather than a panic:
//! the column still iterates, it just holds the wrong thing, or holds the
//! right thing in a shape nothing else in the crate can parse. Three
//! oracles, in increasing strength:
//!
//! 1. **Values** — the edited column against a `Vec` model that mirrors
//!    the cursor's contract exactly (see [`Model`]).
//! 2. **Structure** — [`check_invariants`](crate::Column::check_invariants)
//!    for the slab index and lengths, plus the debug-build slab
//!    validation that runs inside every rebuild.
//! 3. **Bytes** — `save()` against a column freshly built from the model.
//!    Saving is canonical, so this asserts the *encoding* is right, not
//!    just the values: a stray count-1 repeat run, two adjacent runs that
//!    should have merged, or a literal group whose header count is off by
//!    one all fail here and nowhere else.
//!
//! The fast tests run in the normal suite. The exhaustive ones are
//! `#[ignore]`d and run with
//!
//! ```text
//! RUSTFLAGS="-C debug-assertions" cargo test -p hexane --release -- --ignored --nocapture
//! ```
//!
//! which is the combination that matters: optimized enough to run
//! millions of edits, with the in-rebuild slab validation still live.

use crate::column::Column;
use crate::edit::SlabEdit;
use crate::{AsColumnRef, ColumnValueRef, Leb128, Run};
use std::fmt::Debug;

/// xorshift64* — reproducible without a dev-dependency.
pub(crate) struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // splitmix so neighbouring seeds do not produce correlated runs
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        Rng((z ^ (z >> 31)) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }

    fn chance(&mut self, n: usize) -> bool {
        self.below(n) == 0
    }
}

/// What a cursor promises, as a `Vec`.
///
/// The column while a cursor is open is `built ++ src[orig..]`: the items
/// written so far, then the untouched tail of the column as it was when
/// the cursor opened. `seek` takes *original* coordinates, which is why
/// the model has to keep both — the mismatch between the two is precisely
/// the class of bug this is looking for.
struct Model<T> {
    src: Vec<T>,
    built: Vec<T>,
    orig: usize,
}

impl<T: Clone + PartialEq + Debug> Model<T> {
    fn new(src: Vec<T>) -> Self {
        Model {
            src,
            built: Vec::new(),
            orig: 0,
        }
    }

    /// Carry original items through, up to the end of the column.
    fn advance(&mut self, n: usize) {
        let n = n.min(self.src.len() - self.orig);
        self.built
            .extend_from_slice(&self.src[self.orig..self.orig + n]);
        self.orig += n;
    }

    fn seek(&mut self, to: usize) {
        let to = to.max(self.orig);
        self.advance(to - self.orig);
    }

    fn insert(&mut self, v: T, n: usize) {
        for _ in 0..n {
            self.built.push(v.clone());
        }
    }

    /// Deletes are clamped at the end of the column, like the cursor's.
    fn delete(&mut self, n: usize) {
        self.orig = (self.orig + n).min(self.src.len());
    }

    fn peek(&self) -> Option<&T> {
        self.src.get(self.orig)
    }

    /// The column as it stands: what a closed cursor must leave behind.
    fn values(&self) -> Vec<T> {
        let mut v = self.built.clone();
        v.extend_from_slice(&self.src[self.orig..]);
        v
    }
}

/// Every oracle, against a closed column.
fn verify<T>(col: &Column<T>, want: &[T], max_seg: usize, ctx: &dyn Fn() -> String)
where
    T: ColumnValueRef + Clone + PartialEq + Debug,
    for<'x> T::Get<'x>: AsColumnRef<T> + PartialEq + Debug,
{
    col.check_invariants();
    let got: Vec<T> = col.iter().map(T::to_owned).collect();
    assert_eq!(got, want, "values: {}", ctx());
    assert_eq!(col.len(), want.len(), "len: {}", ctx());

    // random access goes through the slab index rather than the iterator,
    // so it fails independently when the index and the slabs disagree
    for (i, w) in want.iter().enumerate() {
        let g = col.get(i).map(T::to_owned);
        assert_eq!(g.as_ref(), Some(w), "get({i}): {}", ctx());
    }
    assert!(col.get(want.len()).is_none(), "get past end: {}", ctx());

    // the strong one: an edited column must serialize exactly like a
    // freshly built one holding the same values
    let fresh = Column::<T>::from_values_with_max_segments(want.to_vec(), max_seg);
    let bytes = col.save();
    assert_eq!(bytes, fresh.save(), "bytes: {}", ctx());

    // and those bytes have to come back — a slab whose cached metadata
    // drifted from its contents can still save, but will not reload
    let back = Column::<T>::load(&bytes)
        .unwrap_or_else(|e| panic!("saved bytes do not load: {e}: {}", ctx()));
    let back_vals: Vec<T> = back.iter().map(T::to_owned).collect();
    assert_eq!(back_vals, want, "reloaded values: {}", ctx());
}

/// The cursor has to leave a column the *other* write paths still accept:
/// their prefix/postfix handling reads the same slab metadata the cursor
/// maintains, so a drift that iteration survives shows up here.
#[test]
fn splice_after_cursor_edits() {
    for max_seg in [2usize, 3, 8, 64] {
        for seed in 0..60u64 {
            let mut rng = Rng::new(seed);
            let vals = shape(&mut rng, 80, &small_u64);
            let mut col = Column::<u64>::from_values_with_max_segments(vals.clone(), max_seg);
            let mut model = Model::new(vals);

            // a cursor pass
            {
                let mut e = col.edit();
                for _ in 0..6 {
                    let ahead = rng.below(9);
                    let to = model.orig + ahead;
                    let del = rng.below(4);
                    let v = small_u64(&mut rng);
                    let ins = rng.below(4);
                    e.seek(to).delete(del).insert_run(v, ins);
                    model.seek(to);
                    model.delete(del);
                    model.insert(v, ins);
                }
                e.finish();
            }
            let mut want = model.values();

            // then splices, back to front, into what the cursor left
            let mut splices: Vec<(usize, usize, usize, u64)> = (0..5)
                .map(|_| {
                    let pos = rng.below(want.len() + 1);
                    let del = rng.below(3).min(want.len() - pos);
                    (pos, del, rng.below(3), small_u64(&mut rng))
                })
                .collect();
            splices.sort_by_key(|s| s.0);
            for (pos, del, ins, v) in splices.iter().rev() {
                col.splice(*pos, *del, std::iter::repeat_n(*v, *ins));
                want.splice(*pos..*pos + *del, std::iter::repeat_n(*v, *ins));
            }
            verify(&col, &want, max_seg, &|| {
                format!("seed={seed} max_seg={max_seg}")
            });

            // and a cursor again, over the spliced result
            let mut model = Model::new(want);
            {
                let mut e = col.edit();
                e.seek(1).delete(2).insert_run(9u64, 3);
                model.seek(1);
                model.delete(2);
                model.insert(9, 3);
                e.finish();
            }
            verify(&col, &model.values(), max_seg, &|| {
                format!("seed={seed} max_seg={max_seg} (cursor after splice)")
            });
        }
    }
}

/// One op in a script. Generated as data before anything runs, so a
/// failure prints the exact sequence that produced it.
#[derive(Debug, Clone)]
enum Op<T> {
    Seek(usize),
    Advance(usize),
    Insert(T),
    InsertRun(T, usize),
    InsertRuns(Vec<(T, usize)>),
    Delete(usize),
    Peek,
    /// replace with the value already there — must leave the slab clean
    ReplaceSame,
    ReplaceWith(T),
}

/// A script: batches of ops, one batch per cursor. Splitting into
/// batches is what lets the column be checked part-way through — the
/// column is only consistent once a cursor closes.
fn gen_script<T, F>(rng: &mut Rng, len: usize, make: &F, ops: usize) -> Vec<Vec<Op<T>>>
where
    T: Clone,
    F: Fn(&mut Rng) -> T,
{
    let mut batches = vec![Vec::new()];
    for _ in 0..ops {
        let op = match rng.below(10) {
            0 => Op::Seek(rng.below(len / 4 + 3)),
            1 => Op::Advance(rng.below(5)),
            2 => Op::Insert(make(rng)),
            3 => Op::InsertRun(make(rng), rng.below(9)),
            4 => Op::InsertRuns(
                (0..1 + rng.below(3))
                    .map(|_| (make(rng), rng.below(4)))
                    .collect(),
            ),
            // deletes, sometimes running well past the slab and the column
            5 | 6 => Op::Delete(if rng.chance(6) {
                rng.below(len + 5)
            } else {
                rng.below(6)
            }),
            7 => Op::Peek,
            8 => {
                if rng.chance(2) {
                    Op::ReplaceSame
                } else {
                    Op::ReplaceWith(make(rng))
                }
            }
            // close this cursor and open a fresh one
            _ => {
                batches.push(Vec::new());
                continue;
            }
        };
        batches.last_mut().expect("a batch").push(op);
    }
    batches
}

/// Run a generated script, checking every oracle each time a cursor closes.
fn run_script<T>(
    mut col: Column<T>,
    vals: Vec<T>,
    batches: &[Vec<Op<T>>],
    max_seg: usize,
    tag: &str,
) where
    T: ColumnValueRef + Clone + PartialEq + Debug,
    T::Encoding<Leb128>: SlabEdit<Value = T>,
    for<'x> T::Get<'x>: AsColumnRef<T> + PartialEq + Debug,
{
    let mut model = Model::new(vals);
    for (b, batch) in batches.iter().enumerate() {
        let ctx = || format!("{tag} batch={b}\n  ops={batch:?}");
        {
            let mut e = col.edit();
            for op in batch {
                match op {
                    Op::Seek(ahead) => {
                        let to = model.orig + ahead;
                        e.seek(to);
                        model.seek(to);
                    }
                    Op::Advance(n) => {
                        e.advance(*n);
                        model.advance(*n);
                    }
                    Op::Insert(v) => {
                        e.insert(v.clone());
                        model.insert(v.clone(), 1);
                    }
                    Op::InsertRun(v, n) => {
                        e.insert_run(v.clone(), *n);
                        model.insert(v.clone(), *n);
                    }
                    Op::InsertRuns(runs) => {
                        e.insert_runs(runs.iter().map(|(v, n)| Run {
                            value: v.clone(),
                            count: *n,
                        }));
                        for (v, n) in runs {
                            model.insert(v.clone(), *n);
                        }
                    }
                    Op::Delete(n) => {
                        e.delete(*n);
                        model.delete(*n);
                    }
                    Op::Peek => {
                        // the only read a cursor exposes mid-rebuild
                        let got = e.peek().map(T::to_owned);
                        assert_eq!(got.as_ref(), model.peek(), "peek: {}", ctx());
                    }
                    Op::ReplaceSame => {
                        // no assertion on `rebuilds()` here: a replace
                        // that steps over a slab boundary closes the
                        // rebuild for reasons of its own. The property
                        // that a no-op replace never dirties a clean
                        // cursor is `read_only_cursor_is_inert`.
                        if let Some(cur) = model.peek().cloned() {
                            e.replace(T::to_owned);
                            model.built.push(cur);
                            model.orig += 1;
                        }
                    }
                    Op::ReplaceWith(v) => {
                        if model.peek().is_some() {
                            let v2 = v.clone();
                            e.replace(move |_| v2.clone());
                            model.built.push(v.clone());
                            model.orig += 1;
                        }
                    }
                }
            }
            // every other batch is dropped rather than finished, so the
            // Drop impl closing the rebuild is covered too
            if b % 2 == 0 {
                e.finish();
            }
        }
        let want = model.values();
        verify(&col, &want, max_seg, &ctx);
        model = Model::new(want);
    }
}

/// One randomized script: a column shape, a budget, and a stream of ops
/// spread over several cursors.
fn fuzz_one<T, F>(seed: u64, max_seg: usize, len: usize, make: &F, ops: usize)
where
    T: ColumnValueRef + Clone + PartialEq + Debug,
    T::Encoding<Leb128>: SlabEdit<Value = T>,
    for<'x> T::Get<'x>: AsColumnRef<T> + PartialEq + Debug,
    F: Fn(&mut Rng) -> T,
{
    let mut rng = Rng::new(seed);
    let vals = shape(&mut rng, len, make);
    let batches = gen_script(&mut rng, len, make, ops);
    let col = Column::<T>::from_values_with_max_segments(vals.clone(), max_seg);
    run_script(
        col,
        vals,
        &batches,
        max_seg,
        &format!("seed={seed} max_seg={max_seg} len={len}"),
    );
}

/// Column shapes. Random values alone miss the cases that live at slab
/// and run boundaries, which is where the encoder's state machine is
/// hardest — so half of these are structured.
fn shape<T, F>(rng: &mut Rng, len: usize, make: &F) -> Vec<T>
where
    T: Clone,
    F: Fn(&mut Rng) -> T,
{
    match rng.below(6) {
        // one long run
        0 => {
            let v = make(rng);
            vec![v; len]
        }
        // two values alternating: every item its own literal
        1 => {
            let a = make(rng);
            let b = make(rng);
            (0..len)
                .map(|i| if i % 2 == 0 { a.clone() } else { b.clone() })
                .collect()
        }
        // runs of a fixed width, so runs line up with slab cuts
        2 => {
            let w = 1 + rng.below(9);
            let vs: Vec<T> = (0..4).map(|_| make(rng)).collect();
            (0..len).map(|i| vs[(i / w) % vs.len()].clone()).collect()
        }
        // a long run, then literals, then a long run
        3 => {
            let a = make(rng);
            let b = make(rng);
            let third = len / 3;
            (0..len)
                .map(|i| {
                    if i < third || i >= 2 * third {
                        a.clone()
                    } else if i % 2 == 0 {
                        b.clone()
                    } else {
                        make(rng)
                    }
                })
                .collect()
        }
        // runs of random width
        4 => {
            let mut out = Vec::with_capacity(len);
            while out.len() < len {
                let v = make(rng);
                for _ in 0..1 + rng.below(6) {
                    out.push(v.clone());
                    if out.len() == len {
                        break;
                    }
                }
            }
            out
        }
        _ => (0..len).map(|_| make(rng)).collect(),
    }
}

// ── value alphabets ─────────────────────────────────────────────────────────

fn small_u64(r: &mut Rng) -> u64 {
    r.below(4) as u64
}

fn wide_u64(r: &mut Rng) -> u64 {
    // straddles the leb128 size boundaries
    match r.below(4) {
        0 => r.below(3) as u64,
        1 => 100 + r.below(50) as u64,
        2 => 1 << (r.below(60) as u64),
        _ => r.next(),
    }
}

fn opt_u64(r: &mut Rng) -> Option<u64> {
    match r.below(3) {
        0 => None,
        _ => Some(r.below(4) as u64),
    }
}

fn opt_str(r: &mut Rng) -> Option<String> {
    match r.below(4) {
        0 => None,
        1 => Some(String::new()),
        _ => Some("abcdefgh"[..1 + r.below(7)].to_string()),
    }
}

// ── the suite ───────────────────────────────────────────────────────────────

/// Budgets small enough that nearly every edit spills, merges, or both.
const TIGHT: [usize; 4] = [2, 3, 4, 5];

#[test]
fn random_scripts_u64() {
    for seed in 0..400u64 {
        for max_seg in TIGHT {
            fuzz_one::<u64, _>(seed, max_seg, 40, &small_u64, 40);
        }
        fuzz_one::<u64, _>(seed, 64, 300, &wide_u64, 60);
    }
}

/// Booleans: a slab is a handful of run counts, so the cursor rebuilds
/// it whole rather than splicing bytes into it — a different enough shape
/// to be worth its own seeds.
#[test]
fn random_scripts_bool() {
    for seed in 0..300u64 {
        for max_seg in TIGHT {
            fuzz_one::<bool, _>(seed, max_seg, 40, &|r| r.below(2) == 0, 40);
        }
        fuzz_one::<bool, _>(seed, 64, 400, &|r| r.below(4) == 0, 60);
    }
}

#[test]
fn random_scripts_nullable() {
    for seed in 0..300u64 {
        for max_seg in [2usize, 4, 64] {
            fuzz_one::<Option<u64>, _>(seed, max_seg, 60, &opt_u64, 40);
        }
    }
}

#[test]
fn random_scripts_strings() {
    for seed in 0..200u64 {
        for max_seg in [2usize, 4, 64] {
            fuzz_one::<Option<String>, _>(seed, max_seg, 50, &opt_str, 30);
        }
    }
}

/// Every position of every small column over a two-value alphabet,
/// against every small (delete, insert) pair — the space where an
/// off-by-one in run splitting has nowhere to hide.
#[test]
fn exhaustive_tiny() {
    for len in 0..=6usize {
        for bits in 0..(1u32 << len) {
            let vals: Vec<u64> = (0..len).map(|i| ((bits >> i) & 1) as u64).collect();
            for max_seg in [2usize, 3] {
                for pos in 0..=len {
                    for del in 0..=(len - pos) {
                        for ins in 0..=2usize {
                            for v in 0..2u64 {
                                let mut col = Column::<u64>::from_values_with_max_segments(
                                    vals.clone(),
                                    max_seg,
                                );
                                let mut m = Model::new(vals.clone());
                                {
                                    let mut e = col.edit();
                                    e.seek(pos).delete(del).insert_run(v, ins);
                                    m.seek(pos);
                                    m.delete(del);
                                    m.insert(v, ins);
                                    e.finish();
                                }
                                let want = m.values();
                                verify(&col, &want, max_seg, &|| {
                                    format!(
                                        "vals={vals:?} max_seg={max_seg} pos={pos} del={del} ins={ins} v={v}"
                                    )
                                });
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Deletes that run off the end of their slab and consume whole slabs
/// after it — the cascade path, driven from every start position with
/// the smallest budget the column allows.
#[test]
fn delete_cascades() {
    let len = 120usize;
    for max_seg in [2usize, 4] {
        for shape_id in 0..3usize {
            let vals: Vec<u64> = match shape_id {
                0 => vec![7; len],
                1 => (0..len as u64).collect(),
                _ => (0..len).map(|i| (i / 5) as u64).collect(),
            };
            for start in (0..len).step_by(7) {
                for del in [1usize, 2, 9, 31, len, len * 2] {
                    for tail_insert in [0usize, 1, 6] {
                        let mut col =
                            Column::<u64>::from_values_with_max_segments(vals.clone(), max_seg);
                        let mut m = Model::new(vals.clone());
                        {
                            let mut e = col.edit();
                            e.seek(start).delete(del).insert_run(99u64, tail_insert);
                            m.seek(start);
                            m.delete(del);
                            m.insert(99, tail_insert);
                            e.finish();
                        }
                        let want = m.values();
                        verify(&col, &want, max_seg, &|| {
                            format!("shape={shape_id} max_seg={max_seg} start={start} del={del} ins={tail_insert}")
                        });
                    }
                }
            }
        }
    }
}

/// A delete that runs past the end of its slab leaves the items it will
/// consume still standing, so the cursor is not where it appears to be —
/// found by the fuzzer, as a `peek` that reported the end of the column
/// in the middle of it.
#[test]
fn peek_after_a_cascading_delete() {
    for max_seg in [2usize, 3, 4, 64] {
        let vals: Vec<u64> = (0..20).collect();
        let mut col = Column::<u64>::from_values_with_max_segments(vals, max_seg);
        {
            let mut e = col.edit();
            e.delete(5);
            assert_eq!(e.peek(), Some(5), "peek after delete, max_seg={max_seg}");
            e.replace(|v| v + 100);
            e.finish();
        }
        let want: Vec<u64> = std::iter::once(105).chain(6..20).collect();
        verify(&col, &want, max_seg, &|| format!("max_seg={max_seg}"));
    }
}

/// A cursor that only reads must leave the column byte-identical, and
/// must never open a rebuild — the property `replace` relies on to keep
/// scanning callers cheap.
#[test]
fn read_only_cursor_is_inert() {
    for max_seg in [2usize, 4, 64] {
        let vals: Vec<Option<u64>> = (0..200)
            .map(|i| match i % 7 {
                0 => None,
                n => Some(n as u64),
            })
            .collect();
        let col = Column::<Option<u64>>::from_values_with_max_segments(vals.clone(), max_seg);
        let before = col.save();
        let mut col = col;
        {
            let mut e = col.edit();
            for (i, want) in vals.iter().enumerate() {
                let got = e.peek().map(<Option<u64> as ColumnValueRef>::to_owned);
                assert_eq!(got.as_ref(), Some(want));
                e.replace(<Option<u64> as ColumnValueRef>::to_owned);
                assert_eq!(e.rebuilds(), 0, "a no-op replace dirtied the slab at {i}");
            }
            e.finish();
        }
        assert_eq!(col.save(), before, "read-only cursor changed the bytes");
        col.check_invariants();
    }
}

/// Degenerate columns: empty, one item, and edits at the very ends.
#[test]
fn degenerate_columns() {
    for max_seg in [2usize, 3, 64] {
        for len in 0..3usize {
            let vals: Vec<u64> = vec![5; len];
            for at in 0..=len {
                for del in 0..=len {
                    for ins in 0..3usize {
                        let mut col =
                            Column::<u64>::from_values_with_max_segments(vals.clone(), max_seg);
                        let mut m = Model::new(vals.clone());
                        {
                            let mut e = col.edit_at(at.min(len));
                            m.seek(at.min(len));
                            e.delete(del);
                            m.delete(del);
                            e.insert_run(5u64, ins);
                            m.insert(5, ins);
                            // dropped, not finished: the Drop impl has to
                            // close the rebuild
                        }
                        let want = m.values();
                        verify(&col, &want, max_seg, &|| {
                            format!("len={len} max_seg={max_seg} at={at} del={del} ins={ins}")
                        });
                    }
                }
            }
        }
    }
}

// ── the long runs ───────────────────────────────────────────────────────────

/// Hours of scripts across every shape, alphabet and budget.
#[test]
#[ignore = "deep fuzz: run explicitly"]
fn deep_random() {
    let iters: u64 = std::env::var("HEXANE_FUZZ_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    for seed in 0..iters {
        // sampled rather than cycled, so budget, length and shape vary
        // independently instead of marching in lockstep
        let mut pick = Rng::new(seed ^ 0xD1CE);
        let max_seg = [2usize, 3, 4, 5, 8, 13, 17, 64][pick.below(8)];
        let len = [0usize, 1, 2, 3, 7, 33, 64, 65, 129, 400, 900][pick.below(11)];
        let ops = 5 + pick.below(80);
        // the failing assertion is often inside the column's own
        // invariant check, which knows nothing about the script — so name
        // the seed here, where it is known
        let r = std::panic::catch_unwind(|| match seed % 5 {
            0 => fuzz_one::<u64, _>(seed, max_seg, len, &small_u64, ops),
            1 => fuzz_one::<u64, _>(seed, max_seg, len, &wide_u64, ops),
            2 => fuzz_one::<Option<u64>, _>(seed, max_seg, len, &opt_u64, ops),
            3 => fuzz_one::<bool, _>(seed, max_seg, len, &|r| r.below(3) == 0, ops),
            _ => fuzz_one::<Option<String>, _>(seed, max_seg, len, &opt_str, ops),
        });
        if r.is_err() {
            panic!("deep_random failed at seed={seed} max_seg={max_seg} len={len} ops={ops}");
        }
        if seed % 2_000 == 0 {
            println!("deep_random: {seed}/{iters}");
        }
    }
}

/// The tiny exhaustive space, widened: longer columns, a three-value
/// alphabet, and two edits per script instead of one.
#[test]
#[ignore = "deep fuzz: run explicitly"]
fn exhaustive_two_edits() {
    for len in 0..=5usize {
        for bits in 0..3u32.pow(len as u32) {
            let vals: Vec<u64> = (0..len)
                .map(|i| (bits / 3u32.pow(i as u32) % 3) as u64)
                .collect();
            for max_seg in [2usize, 3, 4] {
                for p0 in 0..=len {
                    for d0 in 0..=(len - p0) {
                        for i0 in 0..=2usize {
                            for p1 in (p0 + d0)..=len {
                                for d1 in 0..=(len - p1) {
                                    for i1 in 0..=2usize {
                                        let mut col = Column::<u64>::from_values_with_max_segments(
                                            vals.clone(),
                                            max_seg,
                                        );
                                        let mut m = Model::new(vals.clone());
                                        {
                                            let mut e = col.edit();
                                            e.seek(p0).delete(d0).insert_run(7u64, i0);
                                            e.seek(p1).delete(d1).insert_run(8u64, i1);
                                            m.seek(p0);
                                            m.delete(d0);
                                            m.insert(7, i0);
                                            m.seek(p1);
                                            m.delete(d1);
                                            m.insert(8, i1);
                                            e.finish();
                                        }
                                        let want = m.values();
                                        verify(&col, &want, max_seg, &|| {
                                            format!(
                                                "vals={vals:?} max_seg={max_seg} \
                                                 p0={p0} d0={d0} i0={i0} p1={p1} d1={d1} i1={i1}"
                                            )
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// The script that first produced an unmerged interior pair: a cursor
/// that spills two slabs and then cascades a delete into a third, leaving
/// segments `[8, 4, 1, 8]` — the 1 is interior, and the old merge walk
/// only ever inspected the outermost pairs. Kept as-is because the
/// generator is deterministic.
#[cfg(test)]
mod repro {
    use super::*;

    #[test]
    fn seed_2913_unmerged_interior_pair() {
        let mut rng = Rng::new(2913);
        let vals = shape(&mut rng, 2, &wide_u64);
        let batches: Vec<Vec<Op<u64>>> = gen_script(&mut rng, 2, &wide_u64, 60);
        let col = Column::<u64>::from_values_with_max_segments(vals.clone(), 8);
        run_script(col, vals, &batches, 8, "repro");
    }
}

/// `try_merge_range` is shared with the one-shot splice path, so an
/// interior pair it never inspects is not a cursor bug — this drives the
/// same shape (a spill of several slabs, then a delete cascading into a
/// partial one) through `splice` alone.
#[test]
fn splice_leaves_no_unmerged_pair() {
    for max_seg in [4usize, 8, 16] {
        for seed in 0..200u64 {
            let mut rng = Rng::new(seed);
            let len = 20 + rng.below(60);
            let vals: Vec<u64> = (0..len).map(|_| small_u64(&mut rng)).collect();
            let mut col = Column::<u64>::from_values_with_max_segments(vals.clone(), max_seg);
            let mut want = vals;
            for _ in 0..8 {
                let pos = rng.below(want.len() + 1);
                let del = rng.below(30).min(want.len() - pos);
                let ins = rng.below(40);
                let v = small_u64(&mut rng);
                col.splice(pos, del, std::iter::repeat_n(v, ins));
                want.splice(pos..pos + del, std::iter::repeat_n(v, ins));
                col.check_invariants();
            }
            let got: Vec<u64> = col.iter().collect();
            assert_eq!(got, want, "seed={seed} max_seg={max_seg}");
        }
    }
}

// ── delta cursor ────────────────────────────────────────────────────────────

/// The delta cursor against the same three oracles as the plain one, plus
/// the property that matters most for it: the values *after* the cursor
/// must be untouched, which is the whole job of the boundary fix-up.
mod delta {
    use super::{Model, Rng};
    use crate::delta::{DeltaColumn, DeltaValue};
    use std::fmt::Debug;

    fn build<T: DeltaValue>(vals: &[T], max_seg: usize) -> DeltaColumn<T> {
        let mut col = DeltaColumn::<T>::with_max_segments(max_seg);
        col.splice(0, 0, vals.iter().copied());
        col
    }

    fn verify<T: DeltaValue + Debug + PartialEq>(
        col: &DeltaColumn<T>,
        want: &[T],
        max_seg: usize,
        ctx: &dyn Fn() -> String,
    ) {
        let got: Vec<T> = col.iter().collect();
        assert_eq!(got, want, "values: {}", ctx());
        assert_eq!(col.len(), want.len(), "len: {}", ctx());
        col.check_invariants();
        // random access re-anchors the running value from the index
        for (i, w) in want.iter().enumerate() {
            assert_eq!(col.get(i).as_ref(), Some(w), "get({i}): {}", ctx());
        }
        let fresh = build(want, max_seg);
        let bytes = col.save();
        assert_eq!(bytes, fresh.save(), "bytes: {}", ctx());
        let back = DeltaColumn::<T>::load(&bytes).expect("reload");
        assert_eq!(back.iter().collect::<Vec<T>>(), want, "reloaded: {}", ctx());
    }

    /// Ascending-ish data with plateaus and dips, which is the shape a
    /// delta column is for.
    fn shape<T: DeltaValue, F: Fn(&mut Rng, i64) -> T>(
        r: &mut Rng,
        len: usize,
        make: &F,
    ) -> Vec<T> {
        let mut out = Vec::with_capacity(len);
        let mut v = 0i64;
        for _ in 0..len {
            v += match r.below(4) {
                0 => 0,
                1 => 1,
                2 => r.below(7) as i64,
                _ => -(r.below(3) as i64),
            };
            out.push(make(r, v.max(0)));
        }
        out
    }

    fn one<T, F>(seed: u64, max_seg: usize, len: usize, edits: usize, make: &F)
    where
        T: DeltaValue + Debug + PartialEq,
        F: Fn(&mut Rng, i64) -> T,
    {
        let mut rng = Rng::new(seed);
        let vals = shape(&mut rng, len, make);
        let mut col = build(&vals, max_seg);
        let mut m = Model::new(vals.clone());
        let mut log = Vec::new();
        {
            let mut e = col.edit();
            for _ in 0..edits {
                let ahead = rng.below(len / 3 + 2);
                let to = m.orig + ahead;
                e.seek(to);
                m.seek(to);
                log.push(format!("seek({to})"));
                match rng.below(4) {
                    0 => {
                        // peek is checked live against the model
                        let got = e.peek();
                        assert_eq!(
                            got.as_ref(),
                            m.peek(),
                            "peek: seed={seed} max_seg={max_seg} log={log:?}"
                        );
                        log.push("peek".into());
                    }
                    1 => {
                        if let Some(cur) = m.peek().copied() {
                            let v = make(&mut rng, 0);
                            e.replace(|_| v);
                            m.built.push(v);
                            m.orig += 1;
                            log.push(format!("replace({cur:?} -> {v:?})"));
                        }
                    }
                    2 => {
                        // replace with the value already there
                        if let Some(cur) = m.peek().copied() {
                            e.replace(|v| v);
                            m.built.push(cur);
                            m.orig += 1;
                            log.push("replace(same)".into());
                        }
                    }
                    _ => {}
                }
                let del = rng.below(4);
                e.delete(del);
                m.delete(del);
                log.push(format!("delete({del})"));
                if rng.chance(3) {
                    // the run-aware path: evenly-spaced values, sometimes
                    // anchored where the cursor already stands (so they
                    // copy verbatim) and sometimes not
                    let count = 1 + rng.below(4);
                    // a descending run has to stay inside the type's
                    // domain — an unsigned column cannot hold one that
                    // walks below zero
                    let step = if T::MIN_I64 >= 0 {
                        rng.below(3) as i64
                    } else {
                        rng.below(4) as i64 - 1
                    };
                    let prefix = if rng.chance(2) {
                        // whatever the cursor's running value is
                        m.built
                            .last()
                            .or(m.src.get(m.orig.wrapping_sub(1)))
                            .and_then(|v| v.to_i64())
                            .unwrap_or(0)
                    } else {
                        rng.below(40) as i64
                    };
                    let nul = T::NULLABLE && rng.chance(4);
                    e.insert_runs([crate::delta::DeltaRun {
                        prefix,
                        delta: if nul { None } else { Some(step) },
                        count,
                    }]);
                    for k in 1..=count {
                        m.built.push(if nul {
                            T::null_value()
                        } else {
                            T::try_from_i64(prefix + step * k as i64).expect("generated in domain")
                        });
                    }
                    log.push(format!(
                        "insert_runs(prefix={prefix} step={step} n={count} null={nul})"
                    ));
                } else {
                    let ins = rng.below(4);
                    let base = rng.below(50) as i64;
                    let v = make(&mut rng, base);
                    e.insert_run(v, ins);
                    m.insert(v, ins);
                    log.push(format!("insert_run({v:?}, {ins})"));
                }
            }
            e.finish();
        }
        verify(&col, &m.values(), max_seg, &|| {
            format!("seed={seed} max_seg={max_seg} len={len} log={log:?}")
        });
    }

    fn plain(r: &mut Rng, base: i64) -> i64 {
        base + r.below(3) as i64
    }

    fn unsigned(r: &mut Rng, base: i64) -> u64 {
        (base + r.below(3) as i64) as u64
    }

    fn nullable(r: &mut Rng, base: i64) -> Option<u64> {
        match r.below(4) {
            0 => None,
            _ => Some((base + r.below(3) as i64) as u64),
        }
    }

    #[test]
    fn random_scripts() {
        for seed in 0..300u64 {
            for max_seg in [2usize, 4, 64] {
                one(seed, max_seg, 40, 6, &plain);
            }
        }
    }

    #[test]
    fn random_scripts_unsigned() {
        for seed in 0..200u64 {
            for max_seg in [2usize, 5, 64] {
                one(seed, max_seg, 60, 8, &unsigned);
            }
        }
    }

    /// Nulls carry no delta, so the fix-up has to step over them to find
    /// the item that owes it.
    #[test]
    fn random_scripts_nullable() {
        for seed in 0..200u64 {
            for max_seg in [2usize, 4, 64] {
                one(seed, max_seg, 50, 8, &nullable);
            }
        }
    }

    /// One edit at every position of a small column, so the fix-up is
    /// driven onto every kind of neighbour: a run, a literal, a null, the
    /// end of the column.
    #[test]
    fn every_position() {
        for max_seg in [2usize, 3, 64] {
            for len in 1..12usize {
                for nulls in [false, true] {
                    let vals: Vec<Option<u64>> = (0..len)
                        .map(|i| {
                            if nulls && i % 3 == 0 {
                                None
                            } else {
                                Some(i as u64 * 3)
                            }
                        })
                        .collect();
                    for at in 0..=len {
                        for del in 0..=(len - at).min(3) {
                            for ins in 0..3usize {
                                let mut col = build(&vals, max_seg);
                                let mut m = Model::new(vals.clone());
                                {
                                    let mut e = col.edit_at(at);
                                    m.seek(at);
                                    e.delete(del);
                                    m.delete(del);
                                    e.insert_run(Some(77), ins);
                                    m.insert(Some(77), ins);
                                    e.finish();
                                }
                                verify(&col, &m.values(), max_seg, &|| {
                                    format!(
                                        "len={len} nulls={nulls} max_seg={max_seg} \
                                         at={at} del={del} ins={ins}"
                                    )
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    /// A cursor that only reads leaves the bytes alone.
    #[test]
    fn read_only_is_inert() {
        for max_seg in [2usize, 64] {
            let vals: Vec<Option<u64>> = (0..80)
                .map(|i| if i % 5 == 0 { None } else { Some(i as u64) })
                .collect();
            let mut col = build(&vals, max_seg);
            let before = col.save();
            {
                let mut e = col.edit();
                for want in &vals {
                    assert_eq!(e.peek().as_ref(), Some(want));
                    e.replace(|v| v);
                }
                e.finish();
            }
            assert_eq!(col.save(), before, "a read-only delta cursor wrote");
        }
    }

    /// The deep run, alongside the plain cursor's.
    #[test]
    #[ignore = "deep fuzz: run explicitly"]
    fn deep_random() {
        let iters: u64 = std::env::var("HEXANE_FUZZ_ITERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20_000);
        for seed in 0..iters {
            let mut pick = Rng::new(seed ^ 0xDE17A);
            let max_seg = [2usize, 3, 4, 5, 8, 13, 64][pick.below(7)];
            let len = [0usize, 1, 2, 3, 7, 33, 64, 65, 129, 400][pick.below(10)];
            let edits = 1 + pick.below(12);
            let r = std::panic::catch_unwind(|| match seed % 3 {
                0 => one(seed, max_seg, len, edits, &plain),
                1 => one(seed, max_seg, len, edits, &unsigned),
                _ => one(seed, max_seg, len, edits, &nullable),
            });
            if r.is_err() {
                panic!("delta deep_random failed at seed={seed} max_seg={max_seg} len={len}");
            }
            if seed % 2_000 == 0 {
                println!("delta deep_random: {seed}/{iters}");
            }
        }
    }
}

/// Every small boolean column against every small edit — the space where
/// an off-by-one in run splitting or parity has nowhere to hide.
#[test]
fn exhaustive_tiny_bool() {
    for len in 0..=7usize {
        for bits in 0..(1u32 << len) {
            let vals: Vec<bool> = (0..len).map(|i| (bits >> i) & 1 == 1).collect();
            for max_seg in [2usize, 3, 64] {
                for pos in 0..=len {
                    for del in 0..=(len - pos) {
                        for ins in 0..=2usize {
                            for v in [false, true] {
                                let mut col = Column::<bool>::from_values_with_max_segments(
                                    vals.clone(),
                                    max_seg,
                                );
                                let mut m = Model::new(vals.clone());
                                {
                                    let mut e = col.edit();
                                    e.seek(pos).delete(del).insert_run(v, ins);
                                    m.seek(pos);
                                    m.delete(del);
                                    m.insert(v, ins);
                                    e.finish();
                                }
                                verify(&col, &m.values(), max_seg, &|| {
                                    format!("vals={vals:?} max_seg={max_seg} pos={pos} del={del} ins={ins} v={v}")
                                });
                            }
                        }
                    }
                }
            }
        }
    }
}
