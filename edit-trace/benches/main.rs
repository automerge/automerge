use automerge::{transaction::Transactable, AutoCommit, Automerge, ObjType, ScalarValue, ROOT};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::fs;

fn replay_trace_tx(commands: Vec<(usize, usize, Vec<ScalarValue>)>) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text = tx.set_object(ROOT, "text", ObjType::Text).unwrap();
    for (pos, del, vals) in commands {
        tx.splice(&text, pos, del, vals).unwrap();
    }
    tx.commit();
    doc
}

fn replay_trace_autotx(commands: Vec<(usize, usize, Vec<ScalarValue>)>) -> AutoCommit {
    let mut doc = AutoCommit::new();
    let text = doc.set_object(ROOT, "text", ObjType::Text).unwrap();
    for (pos, del, vals) in commands {
        doc.splice(&text, pos, del, vals).unwrap();
    }
    doc.commit();
    doc
}

fn save_trace(mut doc: Automerge) {
    doc.save().unwrap();
}

fn save_trace_autotx(mut doc: AutoCommit) {
    doc.save().unwrap();
}

fn load_trace(bytes: &[u8]) {
    Automerge::load(bytes).unwrap();
}

fn load_trace_autotx(bytes: &[u8]) {
    AutoCommit::load(bytes).unwrap();
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
            vals.push(ScalarValue::Str(v.into()));
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
                replay_trace_tx,
                criterion::BatchSize::LargeInput,
            )
        },
    );

    let commands_len = commands.len();
    let mut doc = replay_trace_tx(commands.clone());
    group.bench_with_input(BenchmarkId::new("save", commands_len), &doc, |b, doc| {
        b.iter_batched(|| doc.clone(), save_trace, criterion::BatchSize::LargeInput)
    });

    let bytes = doc.save().unwrap();
    group.bench_with_input(
        BenchmarkId::new("load", commands_len),
        &bytes,
        |b, bytes| b.iter(|| load_trace(bytes)),
    );

    group.bench_with_input(
        BenchmarkId::new("replay autotx", commands_len),
        &commands,
        |b, commands| {
            b.iter_batched(
                || commands.clone(),
                replay_trace_autotx,
                criterion::BatchSize::LargeInput,
            )
        },
    );

    let commands_len = commands.len();
    let mut doc = replay_trace_autotx(commands);
    group.bench_with_input(
        BenchmarkId::new("save autotx", commands_len),
        &doc,
        |b, doc| {
            b.iter_batched(
                || doc.clone(),
                save_trace_autotx,
                criterion::BatchSize::LargeInput,
            )
        },
    );

    let bytes = doc.save().unwrap();
    group.bench_with_input(
        BenchmarkId::new("load autotx", commands_len),
        &bytes,
        |b, bytes| b.iter(|| load_trace_autotx(bytes)),
    );

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
