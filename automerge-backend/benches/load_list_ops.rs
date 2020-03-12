use criterion::{criterion_group, criterion_main, Criterion};
use automerge_backend::{Backend, ChangeRequest};
use std::fs::File;
use std::io::BufReader;

pub fn criterion_benchmark(c: &mut Criterion) {
    let file = File::open("./benches/1000_list_ops.json").unwrap();
    let buf_reader = BufReader::new(file);
    let change_requests: Vec<ChangeRequest> = serde_json::from_reader(buf_reader).unwrap();

    c.bench_function("load 1000 list ops", |b| {
        b.iter(|| {
            let mut backend = Backend::init();
            change_requests.clone().into_iter().for_each(|change_request| {
                backend.apply_local_change(change_request).unwrap();
            });
        });
    });

}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark);
criterion_main!(benches);
