// Patch-generation strategy benchmarks, written against the AutoCommit
// API surface shared by main / patchless2 / apply_changes_rework so the
// same scenarios can run on each branch (copy into the branch worktree
// and fix small API deltas).
//
// Scenarios per doc:
//   local_typing      N × { splice 1 char; diff_incremental }
//   remote_keystroke  N × { apply_changes(one 1-op change); diff_incremental }
//   merge_burst       apply 200 buffered single-op changes at once; one diff_incremental
//   first_materialize fresh load; diff_incremental with empty cursor (full state)
//
//   cargo run --release -p automerge --example bench_patches [S1 S3 C2 ...]
use automerge::transaction::Transactable;
use automerge::{AutoCommit, ObjType, ReadDoc, Value, ROOT};
use std::time::Instant;

fn find_text(doc: &AutoCommit) -> automerge::ObjId {
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
        vec!["S1".into(), "S3".into(), "C2".into()]
    } else {
        args
    };
    const N: usize = 100;

    for name in &names {
        let path = format!("/Users/orion/automerge-blog/data/{name}.am");
        let bytes = std::fs::read(&path).unwrap();

        // ── local_typing ────────────────────────────────────────────
        {
            let mut doc = AutoCommit::load(&bytes).unwrap();
            let text = find_text(&doc);
            let pos = doc.length(&text) / 2;
            doc.update_diff_cursor();
            let mut best = f64::MAX;
            let mut patches = 0usize;
            for i in 0..N {
                let t = Instant::now();
                doc.splice_text(&text, pos + i, 0, "x").unwrap();
                patches += doc.diff_incremental().len();
                best = best.min(t.elapsed().as_secs_f64());
            }
            println!(
                "{name} local_typing        best {:>10.1}µs  ({patches} patches / {N})",
                best * 1e6
            );
        }

        // ── remote_keystroke ────────────────────────────────────────
        {
            let mut recv = AutoCommit::load(&bytes).unwrap();
            let text = find_text(&recv);
            let pos = recv.length(&text) / 2;
            recv.update_diff_cursor();
            let mut src = recv.fork();
            let mut best_apply = f64::MAX;
            let mut best_diff = f64::MAX;
            let mut patches = 0usize;
            for i in 0..N {
                let heads = recv.get_heads();
                src.splice_text(&text, pos + i, 0, "x").unwrap();
                src.commit();
                let changes = src.get_changes(&heads).unwrap();
                let t = Instant::now();
                recv.apply_changes(changes).unwrap();
                let applied = t.elapsed().as_secs_f64();
                let t = Instant::now();
                patches += recv.diff_incremental().len();
                let diffed = t.elapsed().as_secs_f64();
                best_apply = best_apply.min(applied);
                best_diff = best_diff.min(diffed);
            }
            println!(
                "{name} remote_keystroke   apply {:>10.1}µs + diff {:>10.1}µs  ({patches} patches / {N})",
                best_apply * 1e6,
                best_diff * 1e6
            );
        }

        // ── remote_keystroke_nopatch: same loop, cursor never set, no
        // diff — the pure apply cost with patch machinery idle ───────
        {
            let mut recv = AutoCommit::load(&bytes).unwrap();
            let text = find_text(&recv);
            let pos = recv.length(&text) / 2;
            let mut src = recv.fork();
            let mut best = f64::MAX;
            for i in 0..N {
                let heads = recv.get_heads();
                src.splice_text(&text, pos + i, 0, "x").unwrap();
                src.commit();
                let changes = src.get_changes(&heads).unwrap();
                let t = Instant::now();
                recv.apply_changes(changes).unwrap();
                best = best.min(t.elapsed().as_secs_f64());
            }
            println!("{name} remote_ks_nopatch  apply {:>10.1}µs", best * 1e6);
        }

        // ── merge_burst ─────────────────────────────────────────────
        {
            let mut recv = AutoCommit::load(&bytes).unwrap();
            let text = find_text(&recv);
            let pos = recv.length(&text) / 2;
            recv.update_diff_cursor();
            let heads = recv.get_heads();
            let mut src = recv.fork();
            for i in 0..200 {
                src.splice_text(&text, pos + i, 0, "x").unwrap();
                src.commit();
            }
            let changes = src.get_changes(&heads).unwrap();
            let t = Instant::now();
            recv.apply_changes(changes).unwrap();
            let applied = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let patches = recv.diff_incremental().len();
            let diffed = t.elapsed().as_secs_f64();
            println!(
                "{name} merge_burst(200)   apply {:>8.2}ms + diff {:>8.2}ms  ({patches} patches)",
                applied * 1e3,
                diffed * 1e3
            );
        }

        // ── first_materialize ───────────────────────────────────────
        {
            let mut doc = AutoCommit::load(&bytes).unwrap();
            let t = Instant::now();
            let patches = doc.diff_incremental().len();
            println!(
                "{name} first_materialize  {:>10.2}ms  ({patches} patches)",
                t.elapsed().as_secs_f64() * 1e3
            );
        }
    }
}
