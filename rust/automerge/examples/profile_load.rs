// Load a doc repeatedly — profiler target for the full-load path.
// Args: [doc] [iterations] [mode: audit|default]
// default re-saves the doc first (the raw egwalker files predate the
// head-index suffix the column-trusting load needs).
use automerge::{AuditMode, Automerge, LoadOptions};
fn main() {
    let name = std::env::args().nth(1).unwrap_or_else(|| "S3".into());
    let n: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let mode = std::env::args().nth(3).unwrap_or_else(|| "audit".into());
    let raw = std::fs::read(format!("/Users/orion/automerge-blog/data/{name}.am")).unwrap();
    let bytes = if mode == "audit" {
        raw
    } else {
        // re-save so the head-index suffix (and fragment hashes) exist
        Automerge::load(&raw).unwrap().save()
    };
    let audit = match mode.as_str() {
        "audit" => AuditMode::Enabled,
        _ => AuditMode::Disabled,
    };
    let t = std::time::Instant::now();
    for _ in 0..n {
        let d = Automerge::load_with_options(&bytes, LoadOptions::new().audit(audit)).unwrap();
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
