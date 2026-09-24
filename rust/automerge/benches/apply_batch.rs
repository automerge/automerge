use automerge::{transaction::Transactable, ActorId, AutoCommit, Automerge, Change, ObjType, ROOT};
use std::{hint::black_box, time::Instant};

fn fixture(n: usize, scenario: &str) -> (Automerge, Vec<Change>) {
    let mut source = AutoCommit::new().with_actor(ActorId::from(b"benchmark".as_slice()));
    let mut base = Automerge::new();
    match scenario {
        "insert_map" => {
            for i in 0..n {
                source.put(ROOT, format!("key{i:08}"), i as i64).unwrap();
            }
        }
        "update_map" | "batch_map" => {
            for i in 0..n {
                source.put(ROOT, format!("key{i:08}"), i as i64).unwrap();
            }
            source.commit();
            if scenario == "update_map" {
                base.apply_changes(source.get_changes(&[])).unwrap();
            }
            for i in 0..n {
                source
                    .put(ROOT, format!("key{i:08}"), -(i as i64) - 1)
                    .unwrap();
            }
        }
        "batch_list" => {
            let list = source.put_object(ROOT, "items", ObjType::List).unwrap();
            for i in 0..n {
                source.insert(&list, i, i as i64).unwrap();
            }
            source.commit();
            for i in 0..n {
                source.put(&list, i, -(i as i64) - 1).unwrap();
            }
        }
        _ => unreachable!(),
    }
    source.commit();
    (base.clone(), source.get_changes(&base.get_heads()))
}

fn main() {
    let n = std::env::var("BENCH_OPS")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(10_000);
    let samples = 31;
    println!("scenario,items,median_us,p10_us,p90_us");
    for scenario in ["insert_map", "update_map", "batch_map", "batch_list"] {
        let (base, changes) = fixture(n, scenario);
        let mut timings = Vec::new();
        for sample in 0..samples + 5 {
            let mut doc = base.clone();
            let changes = changes.clone();
            let start = Instant::now();
            doc.apply_changes(black_box(changes)).unwrap();
            let elapsed = start.elapsed().as_secs_f64() * 1_000_000.0;
            black_box(&doc);
            if sample >= 5 {
                timings.push(elapsed);
            }
        }
        timings.sort_by(f64::total_cmp);
        println!(
            "{scenario},{n},{:.1},{:.1},{:.1}",
            timings[samples / 2],
            timings[samples / 10],
            timings[samples * 9 / 10]
        );
    }
}
