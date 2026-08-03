// Time `change_sets_for_fragments` over a document's whole fragment set, and
// over a single small fragment, on the corpus docs.
//
//   cargo run --release -p automerge --example bench_change set
use automerge::{Automerge, LoadOptions, ReadDoc};
use std::time::Instant;

fn best_of<T>(n: u32, mut f: impl FnMut() -> T) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..n {
        let t = Instant::now();
        std::hint::black_box(f());
        let e = t.elapsed().as_secs_f64();
        if e < best {
            best = e;
        }
    }
    best
}

fn main() {
    println!(
        "{:>4} {:>8} {:>7} {:>12} {:>14} {:>12}",
        "doc", "changes", "frags", "all frags", "per frag (avg)", "smallest"
    );
    for name in ["S1", "S2", "S3", "C1", "C2", "A1", "A2"] {
        let path = format!("/Users/orion/automerge-blog/data/{name}.am");
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let doc =
            Automerge::load_with_options(&bytes, LoadOptions::new().with_audit_mode()).unwrap();
        let fragments = doc.fragments(..);
        let n = fragments.len();

        let all = best_of(3, || {
            doc.change_sets_for_fragments(fragments.clone()).unwrap()
        });

        // the loose-keystroke case: one single-member fragment, which
        // should touch only the ops it carries
        let smallest = fragments
            .iter()
            .min_by_key(|f| f.members.len())
            .unwrap()
            .clone();
        let one = best_of(50, || doc.change_set_for_fragment(&smallest).unwrap());

        println!(
            "{:>4} {:>8} {:>7} {:>11.4}s {:>13.4}ms {:>10.4}ms",
            name,
            doc.stats().num_changes,
            n,
            all,
            all * 1e3 / n as f64,
            one * 1e3,
        );
    }
}
