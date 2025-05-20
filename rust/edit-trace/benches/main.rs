use automerge::{transaction::Transactable, AutoCommit, Automerge, ObjType, ROOT};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::fs;

fn replay_trace_tx(commands: Vec<(usize, isize, String)>) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    for (pos, del, vals) in commands {
        tx.splice_text(&text, pos, del, &vals).unwrap();
    }
    tx.commit();
    doc
}

fn replay_trace_autotx(commands: Vec<(usize, isize, String)>) -> AutoCommit {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    for (pos, del, vals) in commands {
        doc.splice_text(&text, pos, del, &vals).unwrap();
    }
    doc.commit();
    doc
}

fn save_trace(doc: Automerge) {
    doc.save();
}

fn save_trace_autotx(mut doc: AutoCommit) {
    doc.save();
}

fn load_trace(bytes: &[u8]) {
    Automerge::load(bytes).unwrap();
}

fn load_trace_autotx(bytes: &[u8]) {
    AutoCommit::load(bytes).unwrap();
}

fn bench(c: &mut Criterion) {
    let contents = fs::read_to_string("edits.json").expect("cannot read edits file");
    let edits = jzon::parse(&contents).expect("cant parse edits");
    let mut commands = vec![];
    for edit in edits.as_array().unwrap() {
        let pos: usize = edit.as_array().unwrap()[0].as_u64().unwrap() as usize;
        let del: isize = edit.as_array().unwrap()[1].as_i64().unwrap() as isize;
        let mut vals = String::new();
        for j in 2..edit.as_array().unwrap().len() {
            let v = edit.as_array().unwrap()[j].as_str().unwrap();
            vals.push_str(v);
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
    let doc = replay_trace_tx(commands.clone());
    group.bench_with_input(BenchmarkId::new("save", commands_len), &doc, |b, doc| {
        b.iter_batched(|| doc.clone(), save_trace, criterion::BatchSize::LargeInput)
    });

    let bytes = doc.save();
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

    let bytes = doc.save();
    group.bench_with_input(
        BenchmarkId::new("load autotx", commands_len),
        &bytes,
        |b, bytes| b.iter(|| load_trace_autotx(bytes)),
    );

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
