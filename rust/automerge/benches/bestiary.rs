use automerge::{transaction::Transactable, Automerge, ObjType, ROOT};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn load_save(data: &Vec<u8>) {
    let doc = Automerge::load(data.as_slice()).unwrap();
    let _save_data = doc.save();
}

fn criterion_benchmark(c: &mut Criterion) {
    let filename = "./benches/embark.automerge";
    let data =  std::fs::read(filename).unwrap();
    c.bench_function(&format!("load_save_embark {}", filename), |b| {
        b.iter(|| load_save(black_box(&data)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
