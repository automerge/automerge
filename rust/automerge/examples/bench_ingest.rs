// Ingest each egwalker doc as one load_incremental() call, three ways:
// concatenated raw changes (comparable to automerge main), concatenated
// v1 bundles through the walk, and the same bundles through the batch
// manifold (BATCH_MANIFOLD). Plus a plain full-doc load for context.
use automerge::Automerge;
use std::time::Instant;

fn best_of<F: Fn() -> Automerge>(f: F, expect: &Automerge) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        let d = f();
        let e = t.elapsed().as_secs_f64();
        if e < best {
            best = e;
        }
        assert_eq!(d.get_heads(), expect.get_heads());
    }
    best
}

fn main() {
    for name in ["S1", "S2", "S3", "C1", "C2", "A1", "A2"] {
        let path = format!("/Users/orion/automerge-blog/data/{name}.am");
        let Ok(bytes) = std::fs::read(&path) else {
            println!("{name}: missing");
            continue;
        };
        let doc = Automerge::load(&bytes).unwrap();
        // measured FIRST (best of 3): a clean-heap baseline, before
        // the ingest paths churn the allocator
        let full = best_of(|| Automerge::load(&bytes).unwrap(), &doc);

        let mut changes = Vec::new();
        for c in doc.get_changes(&[]).unwrap() {
            changes.extend_from_slice(c.raw_bytes());
        }
        let fragments = doc.fragments(..).unwrap();
        let bundles: Vec<u8> = doc
            .bundle_fragments(fragments)
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        let ingest = |payload: &[u8]| {
            let mut d = Automerge::new();
            d.load_incremental(payload).unwrap();
            d
        };

        let t_changes = best_of(|| ingest(&changes), &doc);
        let t_bundles = best_of(|| ingest(&bundles), &doc);
        let t_load_p = best_of(
            || {
                let mut log = automerge::PatchLog::active();
                Automerge::load_with(
                    &bytes,
                    automerge::OnPartialLoad::Error,
                    automerge::VerificationMode::Check,
                    &mut log,
                )
                .unwrap()
            },
            &doc,
        );
        let t_changes_p = best_of(
            || {
                let mut d = Automerge::new();
                let mut log = automerge::PatchLog::active();
                d.load_incremental_log_patches(&changes, &mut log).unwrap();
                d
            },
            &doc,
        );

        // the branch's own path: v2 fragment chain via apply_fragment
        // (parsed from bytes, so the wire round-trip is included)
        let v2: Vec<automerge::BundleV2> = doc
            .bundle_fragments_v2(doc.fragments(..).unwrap())
            .unwrap()
            .iter()
            .map(|b| automerge::BundleV2::try_from(&b[..]).unwrap())
            .collect();
        let t_frag = best_of(
            || {
                let mut d = Automerge::new();
                for b in &v2 {
                    d.apply_fragment(b).unwrap();
                }
                d
            },
            &doc,
        );

        println!(
            "{name}: changes {:>7.3}s (patches {:>7.3}s) | fragments {:>7.3}s | full load {:>6.3}s (patches {:>6.3}s) | bundles {:>7.3}s",
            t_changes,
            t_changes_p,
            t_frag,
            full,
            t_load_p,
            t_bundles,
        );
    }
}
