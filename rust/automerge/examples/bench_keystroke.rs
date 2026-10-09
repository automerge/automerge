// Keystroke latency: apply one text-insert change to a large document as a
// change set, via load_incremental, and via apply_changes.
//
//   cargo run --release -p automerge --example bench_keystroke [S1 S2 S3 ...]
use automerge::next::transaction::Transactable;
use automerge::next::{Automerge, Fragment, ReadDoc};
use automerge::{ChangeHash, ChangeId, ChangeSet, ObjType, Value, ROOT};
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
        let heads = base.get_heads();
        let text = find_text(&base);
        let pos = base.length(&text) / 2;

        // the first keystroke brings in a new actor; the second is steady-state typing
        let src = base.fork();
        // emitting changes needs every hash; the measured targets stay in default mode
        let mut src = src.enable_audit_mode().unwrap();
        for ch in ["x", "y"] {
            let mut tx = src.transaction();
            tx.splice_text(&text, pos, 0, ch).unwrap();
            tx.commit();
        }
        let changes = src.get_changes(&heads).unwrap();

        let v2_bytes: Vec<Vec<u8>> = changes
            .iter()
            .map(|change| {
                let boundary: Vec<ChangeHash> = change.deps().to_vec();
                let frag = Fragment {
                    head: change.hash(),
                    level: change.hash().fragment_level(),
                    boundary,
                    checkpoints: vec![],
                    members: vec![ChangeId::from_parts(
                        change.actor_id().clone(),
                        std::num::NonZeroU64::new(change.seq()).unwrap(),
                    )],
                };
                src.change_set_for_fragment(&frag).unwrap().bytes()
            })
            .collect();

        // (first apply, second apply) best-of-N; only heads checked
        let bench = |f: &dyn Fn(&mut Automerge, usize)| {
            let (mut b0, mut b1) = (f64::MAX, f64::MAX);
            for _ in 0..N {
                let mut d = base.clone();
                let t = Instant::now();
                f(&mut d, 0);
                b0 = b0.min(t.elapsed().as_secs_f64());
                let t = Instant::now();
                f(&mut d, 1);
                b1 = b1.min(t.elapsed().as_secs_f64());
                assert_eq!(d.get_heads(), src.get_heads());
            }
            (b0 * 1e6, b1 * 1e6)
        };

        let (frag_new, frag_steady) = bench(&|d: &mut Automerge, i: usize| {
            let v2 = ChangeSet::try_from(&v2_bytes[i][..]).unwrap();
            d.apply_change_set(v2).unwrap();
        });
        let (inc_new, inc_steady) = bench(&|d: &mut Automerge, i: usize| {
            d.apply_changes([changes[i].clone()]).unwrap();
        });

        println!(
            "{name}: text len {:>7} | fragment new-actor {:>8.1}µs steady {:>8.1}µs | apply_changes new-actor {:>8.1}µs steady {:>8.1}µs",
            base.length(&text),
            frag_new,
            frag_steady,
            inc_new,
            inc_steady,
        );
    }
}
