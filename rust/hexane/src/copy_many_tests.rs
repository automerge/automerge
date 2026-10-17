//! `copy_many_ranges` against applying the same splices one at a time.

use crate::{Column, DeltaColumn, PrefixColumn, RawColumn, Splice};
use rand::{Rng, RngExt, SeedableRng};
use std::ops::Range;

type Plan = Vec<(usize, Splice)>;

/// Splices ascending in a destination of `dest_len` rows, each from a
/// random source with ascending ranges per source; some ranges run past
/// their source to exercise clamping.
fn plan(rng: &mut impl Rng, dest_len: usize, src_lens: &[usize]) -> Plan {
    let mut out = Vec::new();
    let mut at = 0;
    let mut next = vec![0usize; src_lens.len()];
    while rng.random_bool(0.9) {
        at += rng.random_range(0..=(dest_len - at).min(25));
        let delete = rng.random_range(0..=(dest_len - at).min(6));
        let s = rng.random_range(0..src_lens.len());
        let start = next[s] + rng.random_range(0..=4);
        let end = start + rng.random_range(0..60);
        next[s] = end;
        out.push((
            s,
            Splice {
                pos: at,
                delete,
                range: start..end,
            },
        ));
        at += delete;
    }
    out
}

fn reference<T: Clone>(dest: &[T], srcs: &[Vec<T>], plan: &Plan) -> Vec<T> {
    let mut out = Vec::new();
    let mut at = 0;
    for (s, sp) in plan {
        out.extend_from_slice(&dest[at..sp.pos]);
        let src = &srcs[*s];
        let clamp = |r: &Range<usize>| r.start.min(src.len())..r.end.min(src.len());
        out.extend_from_slice(&src[clamp(&sp.range)]);
        at = sp.pos + sp.delete;
    }
    out.extend_from_slice(&dest[at..]);
    out
}

/// Random lengths and slab budgets: mostly matching the destination's,
/// so whole slabs move, sometimes not, so they cannot.
fn shapes(rng: &mut impl Rng) -> (usize, Vec<(usize, usize)>, usize) {
    let max = [2, 4, 8, 16][rng.random_range(0..4)];
    let dest_len = rng.random_range(0..300);
    let srcs = (0..rng.random_range(2..6))
        .map(|_| {
            let budget = if rng.random_bool(0.85) { max } else { max * 2 };
            (rng.random_range(0..400), budget)
        })
        .collect();
    (dest_len, srcs, max)
}

const SEEDS: u64 = 400;

#[test]
fn copy_many_ranges_matches_one_at_a_time() {
    for seed in 0..SEEDS {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
        let (dest_len, srcs, max) = shapes(&mut rng);
        // short runs of a few values, so slabs hold real runs
        let values = |n: usize, rng: &mut rand::rngs::SmallRng| -> Vec<u64> {
            let mut v = Vec::with_capacity(n);
            while v.len() < n {
                let x = rng.random_range(0..5u64);
                let run = rng.random_range(1..6).min(n - v.len());
                v.extend(std::iter::repeat_n(x, run));
            }
            v
        };
        let dest_vals = values(dest_len, &mut rng);
        let src_vals: Vec<Vec<u64>> = srcs.iter().map(|(n, _)| values(*n, &mut rng)).collect();
        let lens: Vec<usize> = src_vals.iter().map(|v| v.len()).collect();
        let p = plan(&mut rng, dest_len, &lens);

        let mut dest: Column<u64> = Column::with_max_segments(max);
        dest.splice(0, 0, dest_vals.clone());
        let cols: Vec<Column<u64>> = src_vals
            .iter()
            .zip(&srcs)
            .map(|(v, (_, budget))| {
                let mut c = Column::with_max_segments(*budget);
                c.splice(0, 0, v.clone());
                c
            })
            .collect();
        dest.copy_many_ranges(cols, p.clone());

        let want = reference(&dest_vals, &src_vals, &p);
        assert_eq!(dest.to_vec(), want, "seed {seed}");
        let mut canon: Column<u64> = Column::with_max_segments(max);
        canon.splice(0, 0, want);
        assert_eq!(dest.save(), canon.save(), "seed {seed}: save bytes");
        dest.check_invariants();
    }
}

#[test]
fn copy_many_ranges_on_nullable_strings_and_bools() {
    for seed in 0..SEEDS {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
        let (dest_len, srcs, max) = shapes(&mut rng);
        let s_vals = |n: usize, rng: &mut rand::rngs::SmallRng| -> Vec<Option<String>> {
            (0..n)
                .map(|_| match rng.random_range(0..4) {
                    0 => None,
                    k => Some(format!("k{k}")),
                })
                .collect()
        };
        let dest_vals = s_vals(dest_len, &mut rng);
        let src_vals: Vec<_> = srcs.iter().map(|(n, _)| s_vals(*n, &mut rng)).collect();
        let lens: Vec<usize> = src_vals.iter().map(|v| v.len()).collect();
        let p = plan(&mut rng, dest_len, &lens);
        let build = |v: &[Option<String>], m: usize| {
            let mut c: Column<Option<String>> = Column::with_max_segments(m);
            c.splice(0, 0, v.to_vec());
            c
        };
        let mut dest = build(&dest_vals, max);
        let cols = src_vals
            .iter()
            .zip(&srcs)
            .map(|(v, (_, m))| build(v, *m))
            .collect();
        dest.copy_many_ranges(cols, p.clone());
        let want = reference(&dest_vals, &src_vals, &p);
        let got: Vec<Option<String>> = dest.iter().map(|v| v.map(str::to_owned)).collect();
        assert_eq!(got, want, "seed {seed}");
        assert_eq!(
            dest.save(),
            build(&want, max).save(),
            "seed {seed}: save bytes"
        );
        dest.check_invariants();

        let b = |v: &[Option<String>]| -> Vec<bool> { v.iter().map(|x| x.is_some()).collect() };
        let build_b = |v: &[bool], m: usize| {
            let mut c: Column<bool> = Column::with_max_segments(m);
            c.splice(0, 0, v.to_vec());
            c
        };
        let mut dest = build_b(&b(&dest_vals), max);
        let cols = src_vals
            .iter()
            .zip(&srcs)
            .map(|(v, (_, m))| build_b(&b(v), *m))
            .collect();
        dest.copy_many_ranges(cols, p.clone());
        let src_b: Vec<Vec<bool>> = src_vals.iter().map(|v| b(v)).collect();
        let want = reference(&b(&dest_vals), &src_b, &p);
        assert_eq!(dest.to_vec(), want, "seed {seed}: bools");
        assert_eq!(
            dest.save(),
            build_b(&want, max).save(),
            "seed {seed}: bool save bytes"
        );
        dest.check_invariants();
    }
}

#[test]
fn copy_many_ranges_on_delta_columns() {
    for seed in 0..SEEDS {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
        let (dest_len, srcs, max) = shapes(&mut rng);
        // counters climb within a source but start anywhere, so every
        // seam needs its first delta rewritten
        let vals = |n: usize, rng: &mut rand::rngs::SmallRng| -> Vec<Option<u32>> {
            let mut x = rng.random_range(0..1000u32);
            (0..n)
                .map(|_| {
                    if rng.random_range(0..6) == 0 {
                        None
                    } else {
                        x += rng.random_range(0..3);
                        Some(x)
                    }
                })
                .collect()
        };
        let dest_vals = vals(dest_len, &mut rng);
        let src_vals: Vec<_> = srcs.iter().map(|(n, _)| vals(*n, &mut rng)).collect();
        let lens: Vec<usize> = src_vals.iter().map(|v| v.len()).collect();
        let p = plan(&mut rng, dest_len, &lens);
        let build = |v: &[Option<u32>], m: usize| {
            let mut c: DeltaColumn<Option<u32>> = DeltaColumn::with_max_segments(m);
            c.splice(0, 0, v.to_vec());
            c
        };
        let mut dest = build(&dest_vals, max);
        let cols = src_vals
            .iter()
            .zip(&srcs)
            .map(|(v, (_, m))| build(v, *m))
            .collect();
        dest.copy_many_ranges(cols, p.clone());
        let want = reference(&dest_vals, &src_vals, &p);
        assert_eq!(dest.to_vec(), want, "seed {seed}");
        assert_eq!(
            dest.save(),
            build(&want, max).save(),
            "seed {seed}: save bytes"
        );
        // random reads jump through the index's per-slab totals
        for i in (0..want.len()).step_by(7) {
            assert_eq!(dest.get(i), Some(want[i]), "seed {seed}: get {i}");
        }
        dest.check_invariants();

        // and the non-nullable kind
        let nn = |v: &[Option<u32>]| -> Vec<u32> { v.iter().map(|x| x.unwrap_or(7)).collect() };
        let build = |v: &[u32], m: usize| {
            let mut c: DeltaColumn<u32> = DeltaColumn::with_max_segments(m);
            c.splice(0, 0, v.to_vec());
            c
        };
        let mut dest = build(&nn(&dest_vals), max);
        let cols = src_vals
            .iter()
            .zip(&srcs)
            .map(|(v, (_, m))| build(&nn(v), *m))
            .collect();
        dest.copy_many_ranges(cols, p.clone());
        let src_nn: Vec<Vec<u32>> = src_vals.iter().map(|v| nn(v)).collect();
        let want = reference(&nn(&dest_vals), &src_nn, &p);
        assert_eq!(dest.to_vec(), want, "seed {seed}: non-nullable");
        assert_eq!(
            dest.save(),
            build(&want, max).save(),
            "seed {seed}: non-nullable save"
        );
        dest.check_invariants();
    }
}

#[test]
fn copy_many_ranges_on_prefix_columns() {
    for seed in 0..SEEDS {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
        let (dest_len, srcs, max) = shapes(&mut rng);
        let vals = |n: usize, rng: &mut rand::rngs::SmallRng| -> Vec<u32> {
            (0..n).map(|_| rng.random_range(0..3)).collect()
        };
        let dest_vals = vals(dest_len, &mut rng);
        let src_vals: Vec<_> = srcs.iter().map(|(n, _)| vals(*n, &mut rng)).collect();
        let lens: Vec<usize> = src_vals.iter().map(|v| v.len()).collect();
        let p = plan(&mut rng, dest_len, &lens);
        let build = |v: &[u32], m: usize| {
            let mut c: PrefixColumn<u32> = PrefixColumn::with_max_segments(m);
            c.splice(0, 0, v.to_vec());
            c
        };
        let mut dest = build(&dest_vals, max);
        let cols = src_vals
            .iter()
            .zip(&srcs)
            .map(|(v, (_, m))| build(v, *m))
            .collect();
        dest.copy_many_ranges(cols, p.clone());
        let want = reference(&dest_vals, &src_vals, &p);
        assert_eq!(dest.to_vec(), want, "seed {seed}");
        let canon = build(&want, max);
        assert_eq!(dest.save(), canon.save(), "seed {seed}: save bytes");
        for i in 0..=want.len() {
            assert_eq!(
                dest.get_prefix(i),
                canon.get_prefix(i),
                "seed {seed}: prefix {i}"
            );
        }
        assert_eq!(
            dest.col.slab_count() > 0,
            !want.is_empty() || dest.col.slab_count() > 0
        );
    }
}

#[test]
fn copy_many_ranges_on_raw_columns() {
    for seed in 0..SEEDS {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
        let (dest_len, srcs, _) = shapes(&mut rng);
        let max = [8, 16, 64][rng.random_range(0..3)];
        let bytes = |n: usize, rng: &mut rand::rngs::SmallRng| -> Vec<u8> {
            (0..n).map(|_| rng.random()).collect()
        };
        let dest_vals = bytes(dest_len, &mut rng);
        let src_vals: Vec<_> = srcs.iter().map(|(n, _)| bytes(*n, &mut rng)).collect();
        let lens: Vec<usize> = src_vals.iter().map(|v| v.len()).collect();
        let p = plan(&mut rng, dest_len, &lens);
        let build = |v: &[u8]| {
            let mut c = RawColumn::with_max_segments(max);
            // several splices, so the column holds several slabs
            for chunk in v.chunks(max / 2 + 1) {
                let at = c.len();
                c.splice_slice(at, 0, chunk);
            }
            c
        };
        let mut dest = build(&dest_vals);
        let cols = src_vals.iter().map(|v| build(v)).collect();
        dest.copy_many_ranges(cols, p.clone());
        let want = reference(&dest_vals, &src_vals, &p);
        assert_eq!(dest.save(), want, "seed {seed}");
        assert_eq!(dest.len(), want.len(), "seed {seed}: len");
    }
}

#[test]
fn copy_many_ranges_delta_two_sources_small() {
    let build = |v: &[u32], m: usize| {
        let mut c: DeltaColumn<u32> = DeltaColumn::with_max_segments(m);
        c.splice(0, 0, v.to_vec());
        c
    };
    let a: Vec<u32> = (100..140).collect();
    let b: Vec<u32> = (500..540).collect();
    let mut dest = build(&[], 4);
    let p = vec![
        (
            0,
            Splice {
                pos: 0,
                delete: 0,
                range: 0..10,
            },
        ),
        (
            1,
            Splice {
                pos: 0,
                delete: 0,
                range: 0..30,
            },
        ),
        (
            0,
            Splice {
                pos: 0,
                delete: 0,
                range: 10..40,
            },
        ),
    ];
    dest.copy_many_ranges(vec![build(&a, 4), build(&b, 4)], p.clone());
    let want = reference(&[], &[a, b], &p);
    assert_eq!(dest.to_vec(), want);
}

#[test]
fn column_encoders_build_the_columns_their_values_make() {
    use crate::prefix::PrefixWeightFn;
    use crate::{ColumnEncoder, DeltaColumnEncoder};
    for seed in 0..SEEDS {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
        let max = [2, 4, 8, 64][rng.random_range(0..4)];
        let n = rng.random_range(0..500);
        let vals: Vec<u32> = (0..n)
            .map(|_| {
                if rng.random_bool(0.3) {
                    0
                } else {
                    rng.random_range(0..4)
                }
            })
            .collect();

        let mut enc = ColumnEncoder::<u32>::with_max_segments(max);
        let mut prefix_enc =
            ColumnEncoder::<u32, crate::Leb128, PrefixWeightFn<u32>>::with_max_segments(max);
        let mut delta_enc = DeltaColumnEncoder::<Option<u32>>::with_max_segments(max);
        let mut deltas = Vec::new();
        let mut x = 500u32;
        for v in &vals {
            let k = rng.random_range(1..4);
            enc.append_n(*v, k);
            prefix_enc.append_n(*v, k);
            x = x.wrapping_add(*v);
            let d = (v % 3 != 0).then_some(x);
            delta_enc.append_n(d, k);
            deltas.extend(std::iter::repeat_n(d, k));
        }
        let col: Column<u32> = enc.into_column();
        let mut canon: Column<u32> = Column::with_max_segments(max);
        canon.splice(0, 0, col.to_vec());
        assert_eq!(col.save(), canon.save(), "seed {seed}");
        col.check_invariants();

        let prefix = PrefixColumn::from_column(prefix_enc.into_column());
        let canon_p: PrefixColumn<u32> = PrefixColumn::from_values(prefix.to_vec());
        for i in 0..=prefix.len() {
            assert_eq!(
                prefix.get_prefix(i),
                canon_p.get_prefix(i),
                "seed {seed}: prefix {i}"
            );
        }

        let delta = delta_enc.into_column();
        assert_eq!(delta.to_vec(), deltas, "seed {seed}: deltas");
        for i in (0..deltas.len()).step_by(5) {
            assert_eq!(delta.get(i), Some(deltas[i]), "seed {seed}: delta get {i}");
        }
        delta.check_invariants();
    }
}
