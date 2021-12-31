use automerge::{Automerge, Value, ROOT};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::fs;

fn replay_trace(commands: Vec<(usize, usize, Vec<Value>)>) -> Automerge {
    let mut doc = Automerge::new();

    let text = doc.set(&ROOT, "text", Value::text()).unwrap().unwrap();
    for (pos, del, vals) in commands {
        doc.splice(&text, pos, del, vals).unwrap();
    }
    doc.commit(None, None);
    doc
}

fn save_trace(mut doc: Automerge) {
    doc.save().unwrap();
}

fn load_trace(bytes: &[u8]) {
    Automerge::load(bytes).unwrap();
}

fn bench(c: &mut Criterion) {
    let contents = fs::read_to_string("edits.json").expect("cannot read edits file");
    let edits = json::parse(&contents).expect("cant parse edits");
    let mut commands = vec![];
    for i in 0..edits.len() {
        let pos: usize = edits[i][0].as_usize().unwrap();
        let del: usize = edits[i][1].as_usize().unwrap();
        let mut vals = vec![];
        for j in 2..edits[i].len() {
            let v = edits[i][j].as_str().unwrap();
            vals.push(Value::str(v));
        }
        commands.push((pos, del, vals));
    }

    let mut group = c.benchmark_group("edit trace");
    group.throughput(Throughput::Elements(commands.len() as u64));

    group.bench_with_input(
        BenchmarkId::new("replay", commands.len()),
        &commands,
        |b, commands| {
            b.iter_batched(
                || commands.clone(),
                replay_trace,
                criterion::BatchSize::LargeInput,
            )
        },
    );

    let commands_len = commands.len();
    let mut doc = replay_trace(commands);
    group.bench_with_input(BenchmarkId::new("save", commands_len), &doc, |b, doc| {
        b.iter_batched(|| doc.clone(), save_trace, criterion::BatchSize::LargeInput)
    });

    let bytes = doc.save().unwrap();
    group.bench_with_input(
        BenchmarkId::new("load", commands_len),
        &bytes,
        |b, bytes| b.iter(|| load_trace(bytes)),
    );

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
