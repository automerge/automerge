// Apply each doc's whole history as a v2 fragment chain, once. Run
// with FRAG_TIMING=1 and aggregate the per-fragment TIMING laps to see
// where chain time goes:
//
//   FRAG_TIMING=1 cargo run --release -p automerge --example profile_frag C1 A1 A2 2>prof.log
use automerge::Automerge;

fn main() {
    for name in std::env::args().skip(1) {
        let path = format!("/Users/orion/automerge-blog/data/{name}.am");
        let bytes = std::fs::read(&path).unwrap();
        let doc = Automerge::load(&bytes).unwrap();

        let v2: Vec<automerge::BundleV2> = doc
            .bundle_fragments_v2(doc.fragments(..).unwrap())
            .unwrap()
            .iter()
            .map(|b| automerge::BundleV2::try_from(&b[..]).unwrap())
            .collect();

        let t = std::time::Instant::now();
        let mut d = Automerge::new();
        for b in &v2 {
            d.apply_fragment(b).unwrap();
        }
        eprintln!(
            "TOTAL {} {:.3}s over {} fragments",
            name,
            t.elapsed().as_secs_f64(),
            v2.len()
        );
        automerge::dump_manifold_stats();
        assert_eq!(d.get_heads(), doc.get_heads());
    }
}
