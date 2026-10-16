//! How much a document's inputs interleave when it is split the way
//! subduction stores it: level-1+ fragments plus loose level-0 commits.
//!
//!   AM_CORPUS=dir1:dir2 cargo test --release -p automerge --lib \
//!     intake_stats -- --ignored --nocapture

use super::Automerge;
use crate::types::ObjId;
use std::collections::HashMap;

struct Stats {
    changes: usize,
    rows: usize,
    fragments: usize,
    loose: usize,
    overlapping_changes: usize,
    contained: usize,
    partial: usize,
    runs: usize,
    max_pool: usize,
    mean_pool: f64,
    single_input_rows: usize,
}

fn stats(doc: &Automerge) -> Stats {
    let graph = &doc.change_graph;
    let cached = doc.fragments(1..);
    let loose = doc.fragments(0..=0);

    let member_sets: Vec<std::collections::HashSet<crate::ChangeId>> = cached
        .iter()
        .chain(loose.iter())
        .map(|f| f.members.iter().cloned().collect())
        .collect();
    let (mut contained, mut partial) = (0, 0);
    for (i, a) in member_sets.iter().enumerate() {
        let mut inside = false;
        let mut overlaps = false;
        for (j, b) in member_sets.iter().enumerate() {
            if i == j || a.len() > b.len() {
                continue;
            }
            let common = a.iter().filter(|m| b.contains(*m)).count();
            if common == a.len() && (a.len() < b.len() || i > j) {
                inside = true;
            } else if common > 0 {
                overlaps = true;
            }
        }
        if inside {
            contained += 1;
        } else if overlaps {
            partial += 1;
        }
    }

    // input index for each (actor, op counter range)
    let mut spans: HashMap<usize, Vec<(u64, u64, usize)>> = HashMap::new();
    let mut owners: HashMap<(usize, u64), usize> = HashMap::new();
    for (input, f) in cached.iter().chain(loose.iter()).enumerate() {
        let mut nodes: Vec<_> = f
            .members
            .iter()
            .map(|id| graph.node_for_change_id(id, &doc.ops.actors).unwrap())
            .collect();
        nodes.sort_unstable();
        nodes.dedup();
        for (actor, seq, start, max) in graph.op_spans(&nodes) {
            *owners.entry((actor, seq)).or_default() += 1;
            spans.entry(actor).or_default().push((start, max, input));
        }
    }
    for v in spans.values_mut() {
        v.sort_unstable();
    }
    let input_of = |actor: usize, ctr: u64| -> usize {
        let v = &spans[&actor];
        let i = v.partition_point(|(start, _, _)| *start <= ctr) - 1;
        debug_assert!(v[i].1 >= ctr);
        v[i].2
    };

    // rows in document order, grouped by object
    let mut by_obj: Vec<(ObjId, Vec<usize>)> = Vec::new();
    let mut iter = doc.ops.iter();
    let mut rows = 0;
    while let Some(op) = iter.try_next().unwrap() {
        rows += 1;
        let input = input_of(op.id.actor(), op.id.counter());
        match by_obj.last_mut() {
            Some((obj, v)) if *obj == op.obj => v.push(input),
            _ => by_obj.push((op.obj, vec![input])),
        }
    }

    let mut runs = 0;
    let mut last = None;
    let mut max_pool = 0;
    let mut pool_sum = 0usize;
    let mut single_input_rows = 0;
    for (_, inputs) in &by_obj {
        let mut first_last: HashMap<usize, (usize, usize)> = HashMap::new();
        for (i, input) in inputs.iter().enumerate() {
            if last != Some(*input) {
                runs += 1;
                last = Some(*input);
            }
            first_last
                .entry(*input)
                .and_modify(|fl| fl.1 = i)
                .or_insert((i, i));
        }
        if first_last.len() == 1 {
            single_input_rows += inputs.len();
        }
        // inputs with rows both at or before and after each position
        let mut delta = vec![0i64; inputs.len() + 1];
        for (first, last) in first_last.values() {
            delta[*first] += 1;
            delta[*last + 1] -= 1;
        }
        let mut open = 0i64;
        for d in &delta[..inputs.len()] {
            open += d;
            max_pool = max_pool.max(open as usize);
            pool_sum += open as usize;
        }
    }

    Stats {
        changes: graph.len(),
        rows,
        fragments: cached.len(),
        loose: loose.len(),
        overlapping_changes: owners.values().filter(|n| **n > 1).count(),
        contained,
        partial,
        runs,
        max_pool,
        mean_pool: pool_sum as f64 / rows.max(1) as f64,
        single_input_rows,
    }
}

fn corpus() -> Vec<std::path::PathBuf> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let dirs = std::env::var("AM_CORPUS").unwrap_or_else(|_| {
        format!(
            "{}:{}",
            root.join("benchmark-battery/egwalker-paper").display(),
            root.join("benchmark-battery/data").display()
        )
    });
    let mut files = Vec::new();
    for dir in dirs.split(':') {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if matches!(
                p.extension().and_then(|x| x.to_str()),
                Some("am" | "amrg" | "automerge")
            ) {
                files.push(p);
            }
        }
    }
    files.sort();
    files
}

#[test]
#[ignore]
fn intake_stats() {
    println!(
        "{:<28} {:>7} {:>8} {:>6} {:>6} {:>5} {:>8} {:>9} {:>8} {:>9} {:>7}",
        "doc",
        "changes",
        "rows",
        "frags",
        "loose",
        "ovlp",
        "runs",
        "rows/run",
        "max pool",
        "mean pool",
        "single%"
    );
    println!("(contained/partial: inputs wholly inside another input / overlapping without being inside)");
    for path in corpus() {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let Ok(doc) = Automerge::load(&bytes) else {
            println!("{name:<28} failed to load");
            continue;
        };
        let s = stats(&doc);
        println!(
            "{:<28} {:>7} {:>8} {:>6} {:>6} {:>5} {:>8} {:>9.1} {:>8} {:>9.2} {:>6.1}%",
            name,
            s.changes,
            s.rows,
            s.fragments,
            s.loose,
            s.overlapping_changes,
            s.runs,
            s.rows as f64 / s.runs.max(1) as f64,
            s.max_pool,
            s.mean_pool,
            100.0 * s.single_input_rows as f64 / s.rows.max(1) as f64,
        );
        if s.overlapping_changes > 0 {
            println!("    contained {} partial {}", s.contained, s.partial);
        }
    }
}

/// Loads each corpus document from its subduction decomposition, one
/// bundle at a time and as feeds, and checks the feeds load matches.
#[test]
#[ignore]
fn intake_corpus() {
    use crate::hash_retention::Retained;
    use crate::storage::Bundle;
    use std::time::Instant;

    println!(
        "{:<28} {:>7} {:>9} {:>11} {:>11} {:>8}",
        "doc", "inputs", "rows", "one by one", "feeds", "speedup"
    );
    for path in corpus() {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        // a `next` document cannot yet bundle fragments with boundaries
        let Ok(doc) =
            Automerge::<crate::hash_retention::Full>::load_with_options_and_mark_validation(
                &bytes,
                Default::default(),
                crate::storage::load::MarkOrderValidation::Validate,
            )
        else {
            continue;
        };
        let fragments = doc.fragments(1..).into_iter().chain(doc.fragments(0..=0));
        let chunks = match doc.bundles_for_fragments(fragments) {
            Ok(c) => c,
            Err(e) => {
                println!("{name:<28} bundles_for_fragments failed: {e:?}");
                continue;
            }
        };
        if chunks.len() < 2 {
            continue;
        }

        let skip = std::env::var("AM_SKIP_ONE_BY_ONE").is_ok();
        let t = Instant::now();
        let mut seq = Automerge::<Retained>::empty(doc.text_encoding());
        let mut pending: Vec<Bundle> = if skip {
            Vec::new()
        } else {
            chunks
                .iter()
                .map(|b| Bundle::try_from(&b[..]).unwrap())
                .collect()
        };
        while !pending.is_empty() {
            let before = pending.len();
            let mut waiting = Vec::new();
            for b in pending {
                match seq.apply_bundle(b.clone()) {
                    Err(crate::AutomergeError::MissingDeps) => waiting.push(b),
                    r => r.unwrap(),
                }
            }
            assert!(waiting.len() < before, "{name}: bundles never became ready");
            pending = waiting;
        }
        let one_by_one = t.elapsed();

        let all: Vec<u8> = chunks.concat();
        let t = Instant::now();
        let fed = Automerge::<Retained>::load(&all).unwrap();
        let feeds = t.elapsed();

        assert_eq!(fed.get_heads(), doc.get_heads(), "{name}: heads");
        if !skip {
            assert_eq!(seq.get_heads(), doc.get_heads(), "{name}: sequential heads");
        }
        fed.debug_cmp(&doc);
        assert!(fed.ops.validate_op_order(), "{name}: op order");
        fed.ops.validate_indexes();

        println!(
            "{:<28} {:>7} {:>9} {:>10.1}ms {:>10.1}ms {:>7.1}x",
            name,
            chunks.len(),
            doc.ops.len(),
            one_by_one.as_secs_f64() * 1e3,
            feeds.as_secs_f64() * 1e3,
            one_by_one.as_secs_f64() / feeds.as_secs_f64(),
        );
    }
}

#[test]
#[ignore]
fn intake_bundles_for_fragments_probe() {
    for path in corpus() {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let bytes = std::fs::read(&path).unwrap();
        let Ok(full) =
            Automerge::<crate::hash_retention::Full>::load_with_options_and_mark_validation(
                &bytes,
                Default::default(),
                crate::storage::load::MarkOrderValidation::Validate,
            )
        else {
            continue;
        };
        let full_failures = full
            .fragments(1..)
            .iter()
            .filter(|f| full.bundle_for_fragment(f).is_err())
            .count();
        print!("full failures {full_failures:>4}  ");
        let Ok(doc) = Automerge::load(&bytes) else {
            continue;
        };
        let hi = doc.fragments(1..);
        let lo = doc.fragments(0..=0);
        let r_hi = doc.bundles_for_fragments(hi.clone()).map(|v| v.len());
        let r_lo = doc.bundles_for_fragments(lo.clone()).map(|v| v.len());
        let one_by_one = hi
            .iter()
            .filter(|f| doc.bundle_for_fragment(f).is_err())
            .count();
        println!(
            "{name:<28} hi {:>4} {:?}  lo {:>4} {:?}  single-fragment failures {one_by_one}",
            hi.len(),
            r_hi.err(),
            lo.len(),
            r_lo.err()
        );
    }
}

/// Writes each corpus document's subduction feeds to `$AM_FEEDS/<name>.feeds`.
#[test]
#[ignore]
fn intake_dump_feeds() {
    let out = std::env::var("AM_FEEDS").expect("AM_FEEDS");
    for path in corpus() {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let bytes = std::fs::read(&path).unwrap();
        let Ok(doc) =
            Automerge::<crate::hash_retention::Full>::load_with_options_and_mark_validation(
                &bytes,
                Default::default(),
                crate::storage::load::MarkOrderValidation::Validate,
            )
        else {
            continue;
        };
        let fragments = doc.fragments(1..).into_iter().chain(doc.fragments(0..=0));
        let chunks = doc.bundles_for_fragments(fragments).unwrap();
        std::fs::write(format!("{out}/{name}.feeds"), chunks.concat()).unwrap();
    }
}

/// Loads `$AM_FEED` `$AM_RUNS` times.
#[test]
#[ignore]
fn intake_load_feeds() {
    let bytes = std::fs::read(std::env::var("AM_FEED").expect("AM_FEED")).unwrap();
    let runs: usize = std::env::var("AM_RUNS").map_or(1, |r| r.parse().unwrap());
    for _ in 0..runs {
        let t = std::time::Instant::now();
        let doc = Automerge::<crate::hash_retention::Retained>::load(&bytes).unwrap();
        println!(
            "loaded {} rows, {} objects in {:.1}ms",
            doc.ops.len(),
            doc.ops.obj_info.0.len(),
            t.elapsed().as_secs_f64() * 1e3
        );
    }
}

/// Compares ways of loading each corpus document's subduction feeds:
/// one bundle at a time, as feeds, the level-1+ fragments one at a
/// time and the loose commits as one batch of changes, and every bundle
/// as changes through the untangler. `AM_SKIP_ONE_BY_ONE` skips the first.
#[test]
#[ignore]
fn intake_strategies() {
    use crate::hash_retention::{Full, Retained};
    use crate::storage::Bundle;
    use std::time::Instant;

    fn one_at_a_time(doc: &mut Automerge<Full>, bundles: Vec<Bundle>) {
        let mut pending = bundles;
        while !pending.is_empty() {
            let before = pending.len();
            let mut waiting = Vec::new();
            for b in pending {
                match doc.apply_bundle(b.clone()) {
                    Err(crate::AutomergeError::MissingDeps) => waiting.push(b),
                    r => r.unwrap(),
                }
            }
            assert!(waiting.len() < before, "bundles never became ready");
            pending = waiting;
        }
    }
    fn as_changes(bundles: &[Bundle]) -> Vec<crate::Change> {
        bundles
            .iter()
            .flat_map(|b| b.to_changes().unwrap())
            .collect()
    }
    let ms = |t: Instant| t.elapsed().as_secs_f64() * 1e3;
    let skip_slow = std::env::var("AM_SKIP_ONE_BY_ONE").is_ok();

    println!(
        "{:<28} {:>6} {:>6} {:>11} {:>10} {:>10} {:>11} {:>10}",
        "doc", "frags", "loose", "one by one", "feeds", "hybrid", "untangler", "next feeds"
    );
    for path in corpus() {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let bytes = std::fs::read(&path).unwrap();
        let Ok(doc) =
            Automerge::<crate::hash_retention::Full>::load_with_options_and_mark_validation(
                &bytes,
                Default::default(),
                crate::storage::load::MarkOrderValidation::Validate,
            )
        else {
            continue;
        };
        let parse = |chunks: Vec<Vec<u8>>| -> Vec<Bundle> {
            chunks
                .iter()
                .map(|b| Bundle::try_from(&b[..]).unwrap())
                .collect()
        };
        let frags = parse(doc.bundles_for_fragments(doc.fragments(1..)).unwrap());
        let loose = parse(doc.bundles_for_fragments(doc.fragments(0..=0)).unwrap());
        let all: Vec<Bundle> = frags.iter().chain(&loose).cloned().collect();
        if all.len() < 2 {
            continue;
        }
        let enc = doc.text_encoding();

        let one = (!skip_slow).then(|| {
            let t = Instant::now();
            let mut d = Automerge::<Full>::empty(enc);
            one_at_a_time(&mut d, all.clone());
            assert_eq!(d.get_heads(), doc.get_heads());
            ms(t)
        });

        let bytes: Vec<u8> = all.iter().flat_map(|b| b.bytes()).collect();
        let t = Instant::now();
        let d = Automerge::<Retained>::load(&bytes).unwrap();
        let feeds_next = ms(t);
        assert_eq!(d.get_heads(), doc.get_heads());

        let t = Instant::now();
        let d = Automerge::<Full>::load_with_options_and_mark_validation(
            &bytes,
            Default::default(),
            crate::storage::load::MarkOrderValidation::Validate,
        )
        .unwrap();
        let feeds = ms(t);
        assert_eq!(d.get_heads(), doc.get_heads());

        let t = Instant::now();
        let mut d = Automerge::<Full>::empty(enc);
        one_at_a_time(&mut d, frags.clone());
        d.apply_changes(as_changes(&loose)).unwrap();
        let hybrid = ms(t);
        assert_eq!(d.get_heads(), doc.get_heads());

        let t = Instant::now();
        let mut d = Automerge::<Full>::empty(enc);
        d.apply_changes(as_changes(&all)).unwrap();
        let untangler = ms(t);
        assert_eq!(d.get_heads(), doc.get_heads());

        println!(
            "{:<28} {:>6} {:>6} {:>11} {:>8.1}ms {:>8.1}ms {:>9.1}ms {:>8.1}ms",
            name,
            frags.len(),
            loose.len(),
            one.map_or("-".to_string(), |v| format!("{v:.1}ms")),
            feeds,
            hybrid,
            untangler,
            feeds_next
        );
    }
}
