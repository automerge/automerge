use automerge_frontend::{Frontend, InvalidChangeRequest, LocalChange, Path, Value};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use rand::{thread_rng, Rng};
use smol_str::SmolStr;
use unicode_segmentation::UnicodeSegmentation;

pub fn insert_long_string(c: &mut Criterion) {
    c.bench_function("Frontend::change insert long string", move |b| {
        b.iter_batched(
            || {
                let doc = Frontend::new();
                let random_string: SmolStr = thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .take(6000)
                    .map(char::from)
                    .collect();
                (doc, random_string)
            },
            |(mut doc, string)| {
                #[allow(clippy::unit_arg)]
                black_box({
                    doc.change::<_, _, InvalidChangeRequest>(None, |d| {
                        d.add_change(LocalChange::set(
                            Path::root().key("text"),
                            Value::Text(string.graphemes(true).map(|s| s.into()).collect()),
                        ))
                    })
                    .unwrap()
                })
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = frontend_benches;
    config = Criterion::default().sample_size(10);
    targets = insert_long_string,
}
criterion_main!(frontend_benches);
