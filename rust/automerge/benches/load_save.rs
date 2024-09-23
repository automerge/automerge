use automerge::{transaction::Transactable, Automerge, ObjType, ROOT};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

fn random_string(n: u64) -> String {
    let rand_string: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n as usize)
        .map(char::from)
        .collect();

    rand_string
}

fn big_paste_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "content", random_string(n)).unwrap();
    tx.commit();

    doc
}

fn poorly_simulated_typing_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();

    let mut tx = doc.transaction();
    let obj = tx.put_object(ROOT, "content", ObjType::Text).unwrap();
    tx.commit();

    for i in 0..n {
        let mut tx = doc.transaction();
        let pos: usize = i.try_into().unwrap();
        tx.splice_text(&obj, pos, 0, &random_string(1)).unwrap();
        tx.commit();
    }

    doc
}

fn maps_in_maps_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();

    let mut map = ROOT;

    for i in 0..n {
        // we make a map
        map = tx.put_object(map, i.to_string(), ObjType::Map).unwrap();
    }

    tx.commit();
    doc
}

fn deep_history_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    for i in 0..n {
        let mut tx = doc.transaction();
        tx.put(ROOT, "x", i.to_string()).unwrap();
        tx.put(ROOT, "y", i.to_string()).unwrap();
        tx.commit();
    }

    doc
}

fn save_load(doc: &Automerge) {
    let save_data = doc.save();
    let new_doc = Automerge::load(save_data.as_slice()).unwrap();
    assert_eq!(doc.get_heads(), new_doc.get_heads());
}

fn criterion_benchmark(c: &mut Criterion) {
    let n = 10_000;

    c.bench_function(&format!("load_save_big_paste_doc {}", n), |b| {
        let doc = big_paste_doc(n);
        b.iter(|| save_load(black_box(&doc)))
    });

    c.bench_function(
        &format!("load_save_poorly_simulated_typing_doc {}", n), |b| {
            let doc = poorly_simulated_typing_doc(n);
            b.iter(|| save_load(black_box(&doc)))
    });

    c.bench_function(&format!("load_save_maps_in_maps_doc {}", n), |b| {
        let doc = maps_in_maps_doc(n);
        b.iter(|| save_load(black_box(&doc)))
    });

    c.bench_function(&format!("load_save_deep_history_doc {}", n), |b| {
        let doc = deep_history_doc(n);
        b.iter(|| save_load(black_box(&doc)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
