// What `SaveOptions::format` trades: the loose commits are bytes in
// the save file, and leaving it out makes the load rehash it. Sweep
// document size and print both settings.
//
//   cargo run --release -p automerge --example bench_minimize
//
// Pass paths to `.am` files to measure those instead of the synthetic
// sweep — they are loaded once and re-saved both ways.
use automerge::{
    transaction::Transactable, ActorId, AutoCommit, Automerge, ReadDoc, SaveFormat, SaveOptions,
    ROOT,
};
use std::time::Instant;

fn best_of<T>(n: u32, mut f: impl FnMut() -> T) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..n {
        let t = Instant::now();
        std::hint::black_box(f());
        best = best.min(t.elapsed().as_secs_f64());
    }
    best
}

fn opts(format: SaveFormat) -> SaveOptions {
    SaveOptions {
        format,
        ..Default::default()
    }
}

/// `actors` peers; each round every peer commits `burst` changes then
/// learns every other peer's round, so a loose commit can depend on a
/// change interior to a fragment. A linear history never does.
fn mesh(actors: usize, rounds: usize, burst: usize) -> AutoCommit {
    let mut peers: Vec<AutoCommit> = (0..actors)
        .map(|i| AutoCommit::new().with_actor(ActorId::from(&[i as u8, 1, 2, 3][..])))
        .collect();
    for r in 0..rounds {
        for (i, p) in peers.iter_mut().enumerate() {
            for b in 0..burst {
                p.put(ROOT, format!("k{i}"), (r * burst + b) as i64)
                    .unwrap();
                p.commit();
            }
        }
        let snaps: Vec<Vec<u8>> = peers.iter_mut().map(|p| p.save()).collect();
        for (i, p) in peers.iter_mut().enumerate() {
            for (j, s) in snaps.iter().enumerate() {
                if i != j {
                    let mut o = AutoCommit::load(s).unwrap();
                    p.merge(&mut o).unwrap();
                }
            }
        }
    }
    peers.swap_remove(0)
}

fn build(n: usize) -> AutoCommit {
    let mut doc = AutoCommit::new();
    for i in 0..n {
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit();
    }
    doc
}

fn header() {
    println!(
        "{:<13} {:>6} {:>9} {:>9} {:>8} {:>9} {:>9} {:>8} {:>8} {:>9}",
        "doc",
        "loose",
        "Fast B",
        "Small B",
        "saved",
        "Fast ms",
        "Small ms",
        "cost",
        "B/loose",
        "us/loose"
    );
}

fn row(label: String, doc: &Automerge, fast: &[u8], small: &[u8]) {
    let loose = doc.fragments(0..=0).len();
    let t_fast = best_of(5, || Automerge::load(fast).unwrap());
    let t_small = best_of(5, || Automerge::load(small).unwrap());
    let saved = fast.len() - small.len();
    println!(
        "{:<13} {:>6} {:>9} {:>9} {:>8} {:>9.3} {:>9.3} {:>+8.3} {:>8.1} {:>9.2}",
        label,
        loose,
        fast.len(),
        small.len(),
        saved,
        t_fast * 1e3,
        t_small * 1e3,
        (t_small - t_fast) * 1e3,
        saved as f64 / loose as f64,
        (t_small - t_fast) * 1e6 / loose as f64,
    );
}

fn main() {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    header();

    if paths.is_empty() {
        let mut cases: Vec<(String, AutoCommit)> = [1_000, 4_000, 16_000, 64_000]
            .into_iter()
            .map(|n| (format!("lin-{n}"), build(n)))
            .collect();
        for (a, r, b) in [(12usize, 10usize, 4usize), (16, 8, 3), (16, 20, 3)] {
            cases.push((format!("mesh-{a}x{r}x{b}"), mesh(a, r, b)));
        }
        for (n, mut doc) in cases {
            let fast = doc.save_with_options(opts(SaveFormat::Fast));
            let small = doc.save_with_options(opts(SaveFormat::Small));
            row(n, doc.document(), &fast, &small);
        }
        return;
    }

    for path in paths {
        let Ok(bytes) = std::fs::read(&path) else {
            eprintln!("skipping {path}");
            continue;
        };
        let mut doc = AutoCommit::load(&bytes).unwrap();
        let fast = doc.save_with_options(opts(SaveFormat::Fast));
        let small = doc.save_with_options(opts(SaveFormat::Small));
        let label = doc.document().stats().num_changes.to_string();
        row(label, doc.document(), &fast, &small);
    }
}
