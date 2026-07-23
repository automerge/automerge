// Steady-state keystroke apply + diff_incremental on a large loaded
// doc — the JS-facing typing path with patches on. Timings are split
// so the change-apply cost and the dirty-diff cost are visible
// separately (the fragment path in bench_keystroke covers the apply
// side; this bench watches the patch side).
//
//   cargo run --release -p automerge --example bench_keystroke_patch [S1 S2 S3 ...]
use automerge::transaction::Transactable;
use automerge::{Automerge, ObjType, ReadDoc, Value, ROOT};
use std::time::Instant;

fn find_text(doc: &Automerge) -> automerge::ObjId {
    let mut queue = vec![ROOT];
    while let Some(obj) = queue.pop() {
        for key in doc.keys(&obj) {
            if let Ok(Some((Value::Object(t), id))) = doc.get(&obj, &key) {
                if t == ObjType::Text {
                    return id;
                }
                queue.push(id);
            }
        }
    }
    panic!("no text object found");
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let names = if args.is_empty() {
        vec!["S1".into(), "S2".into(), "S3".into()]
    } else {
        args
    };
    const N: usize = 200;

    for name in names {
        let path = format!("/Users/orion/automerge-blog/data/{name}.am");
        let bytes = std::fs::read(&path).unwrap();
        let base = Automerge::load(&bytes).unwrap();
        let text = find_text(&base);
        let pos = base.length(&text) / 2;

        let mut recv = base.fork();
        // prime the cursor: everything up to now is not our concern
        let _ = recv.diff_incremental();

        let mut src = recv.fork();
        let mut cursor = recv.get_heads();
        let mut best = f64::MAX;
        let mut best_apply = f64::MAX;
        let mut best_diff = f64::MAX;
        let mut total_patches = 0usize;
        for i in 0..N {
            let mut tx = src.transaction();
            tx.splice_text(&text, pos + i, 0, "x").unwrap();
            tx.commit();
            let change = src.get_changes(&cursor).unwrap();

            let t = Instant::now();
            recv.apply_changes(change).unwrap();
            let applied = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let patches = recv.diff_incremental();
            let diffed = t.elapsed().as_secs_f64();
            best = best.min(applied + diffed);
            best_apply = best_apply.min(applied);
            best_diff = best_diff.min(diffed);
            total_patches += patches.len();
            cursor = recv.get_heads();
        }
        println!(
            "{name}: steady keystroke best {:.1}µs (apply {:.1}µs + diff {:.1}µs; {} patches over {N} keystrokes)",
            best * 1e6,
            best_apply * 1e6,
            best_diff * 1e6,
            total_patches
        );
    }
}
