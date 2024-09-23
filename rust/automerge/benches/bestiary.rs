use automerge::Automerge;
use criterion::{black_box, criterion_group, criterion_main, Criterion};


fn bench_file(c: &mut Criterion, filename: &str) {
    let data =  std::fs::read(filename).unwrap();
    c.bench_function(&format!("bestiary_load {}", filename), |b| {
        b.iter(|| Automerge::load(data.as_slice()))
    });

    let doc = Automerge::load(data.as_slice()).unwrap();
    c.bench_function(&format!("bestiary_save {}", filename), |b| {
        b.iter(|| doc.save())
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_file(c, "./benches/embark.automerge");
    bench_file(c, "./benches/moby-dick.automerge");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
