// Apply each doc's whole history as a v2 fragment chain, once — with
// and without an active patch log:
//
//   cargo run --release -p automerge --example profile_frag C1 A1 A2
use automerge::Automerge;

fn main() {
    for name in std::env::args().skip(1) {
        let path = format!("/Users/orion/automerge-blog/data/{name}.am");
        let bytes = std::fs::read(&path).unwrap();
        let doc = Automerge::load(&bytes).unwrap();

        let v2: Vec<automerge::Bundle> = doc
            .bundle_fragments(doc.fragments(..).unwrap())
            .unwrap()
            .iter()
            .map(|b| automerge::Bundle::try_from(&b[..]).unwrap())
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
        assert_eq!(d.get_heads(), doc.get_heads());

        // same chain with an active patch log — the JS-facing path
        let t = std::time::Instant::now();
        let mut d2 = Automerge::new();
        for b in &v2 {
            d2.apply_fragment(b).unwrap();
            let _patches = d2.diff_incremental();
        }
        eprintln!(
            "TOTAL {} {:.3}s with active patch log",
            name,
            t.elapsed().as_secs_f64()
        );
    }
}
