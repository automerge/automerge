// Load a doc repeatedly — profiler target for the full-load path.
// Args: [doc] [iterations] [mode: full|none|frags]
// none/frags re-save the doc first (the raw egwalker files predate the
// head-index suffix the unchecked load needs).
use automerge::{Automerge, HashGraphRebuild, LoadOptions};
fn main() {
    let name = std::env::args().nth(1).unwrap_or_else(|| "S3".into());
    let n: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let mode = std::env::args().nth(3).unwrap_or_else(|| "full".into());
    let raw = std::fs::read(format!("/Users/orion/automerge-blog/data/{name}.am")).unwrap();
    let bytes = if mode == "full" {
        raw
    } else {
        // re-save so the head-index suffix (and fragment hashes) exist
        Automerge::load(&raw).unwrap().save()
    };
    let rebuild = match mode.as_str() {
        "none" => HashGraphRebuild::None,
        "frags" => HashGraphRebuild::Fragments,
        _ => HashGraphRebuild::Full,
    };
    let t = std::time::Instant::now();
    for _ in 0..n {
        let d =
            Automerge::load_with_options(&bytes, LoadOptions::new().hash_graph(rebuild)).unwrap();
        std::hint::black_box(&d);
    }
    eprintln!(
        "{} x{} mode={} avg {:.4}s",
        name,
        n,
        mode,
        t.elapsed().as_secs_f64() / n as f64
    );
}
