use automerge::{Backend, Frontend, InvalidChangeRequest, LocalChange, Path, Value};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};
use std::convert::TryInto;
use std::default::Default;

pub fn b1(c: &mut Criterion) {
    c.bench_function("B1.1 Append N characters", move |b| {
        b.iter(|| {
            #[allow(clippy::unit_arg)]
            black_box({
                let mut doc1 = Frontend::new();
                let changedoc1 = doc1
                    .change::<_, InvalidChangeRequest>(None, |d| {
                        d.add_change(LocalChange::set(
                            Path::root().key("text"),
                            Value::Text(Vec::new()),
                        ))?;
                        Ok(())
                    })
                    .unwrap()
                    .unwrap();
                let mut backend1 = Backend::init();
                let (patch1, _) = backend1.apply_local_change(changedoc1).unwrap();
                doc1.apply_patch(patch1).unwrap();

                let mut doc2 = Frontend::new();
                let changedoc2 = backend1.get_changes(&[]);
                let mut backend2 = Backend::init();
                let patch2 = backend2
                    .apply_changes(changedoc2.into_iter().cloned().collect())
                    .unwrap();
                doc2.apply_patch(patch2).unwrap();

                let random_string: String = thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .take(6000)
                    .map(char::from)
                    .collect();

                for (index, c) in random_string.chars().enumerate() {
                    let index: u32 = index.try_into().unwrap();
                    let doc1_insert_change = doc1
                        .change::<_, InvalidChangeRequest>(None, |d| {
                            d.add_change(LocalChange::insert(
                                Path::root().key("text").index(index),
                                Value::Primitive(c.into()),
                            ))?;
                            Ok(())
                        })
                        .unwrap()
                        .unwrap();
                    let (patch, change_to_send) =
                        backend1.apply_local_change(doc1_insert_change).unwrap();
                    doc1.apply_patch(patch).unwrap();

                    let patch2 = backend2
                        .apply_changes(vec![(*change_to_send).clone()])
                        .unwrap();
                    doc2.apply_patch(patch2).unwrap()
                }
            })
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = b1
}
criterion_main!(benches);
