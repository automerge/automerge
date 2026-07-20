// Load a doc repeatedly — profiler target for the full-load path.
use automerge::Automerge;
fn main() {
    let name = std::env::args().nth(1).unwrap_or_else(|| "S3".into());
    let n: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(10);
    let bytes = std::fs::read(format!("/Users/orion/automerge-blog/data/{name}.am")).unwrap();
    let t = std::time::Instant::now();
    for _ in 0..n {
        let d = Automerge::load(&bytes).unwrap();
        std::hint::black_box(&d);
    }
    eprintln!("{} x{} avg {:.3}s", name, n, t.elapsed().as_secs_f64() / n as f64);
}
