// Ingest each egwalker doc in one load_incremental() call: as raw changes and
// as change sets, plus a plain full-doc load for context.
use automerge::next::Automerge;
use automerge::ChangeId;
use std::time::Instant;

fn best_of<F: Fn() -> Automerge>(f: F, expect: &[ChangeId]) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        let d = f();
        let e = t.elapsed().as_secs_f64();
        if e < best {
            best = e;
        }
        assert_eq!(d.get_heads(), expect);
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
        // emitting changes needs audit mode; the measured paths use default documents
        let doc = Automerge::load(&bytes)
            .unwrap()
            .enable_audit_mode()
            .unwrap();
        let heads = doc.get_heads();
        // measured first: a clean-heap baseline before the allocator churns
        let full = best_of(|| Automerge::load(&bytes).unwrap(), &heads);

        let mut changes = Vec::new();
        for c in doc.get_changes(&[]).unwrap() {
            changes.extend_from_slice(c.raw_bytes());
        }
        let fragments = doc.fragments(..);
        let change_sets: Vec<u8> = doc
            .change_sets_for_fragments(fragments)
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        let ingest = |payload: &[u8]| {
            let mut d = Automerge::new();
            d.load_incremental(payload).unwrap();
            d
        };

        let t_changes = best_of(|| ingest(&changes), &heads);
        let t_change_sets = best_of(|| ingest(&change_sets), &heads);
        let t_load_p = best_of(|| Automerge::load(&bytes).unwrap(), &heads);
        let t_changes_p = best_of(
            || {
                let mut d = Automerge::new();
                d.load_incremental(&changes).unwrap();
                let _patches = d.diff_incremental();
                d
            },
            &heads,
        );

        // parsing is timed: an applied change set is consumed, so it can't be reused
        let v2_bytes: Vec<Vec<u8>> = doc
            .change_sets_for_fragments(doc.fragments(..))
            .unwrap()
            .into_iter()
            .collect();
        let t_frag = best_of(
            || {
                let mut d = Automerge::new();
                for b in &v2_bytes {
                    d.apply_change_set(automerge::ChangeSet::try_from(&b[..]).unwrap())
                        .unwrap();
                }
                d
            },
            &heads,
        );

        println!(
            "{name}: changes {:>7.3}s (patches {:>7.3}s) | fragments {:>7.3}s | full load {:>6.3}s (patches {:>6.3}s) | change_sets {:>7.3}s",
            t_changes,
            t_changes_p,
            t_frag,
            full,
            t_load_p,
            t_change_sets,
        );
    }
}
