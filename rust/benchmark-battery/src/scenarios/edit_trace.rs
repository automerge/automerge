use super::SampledBenchmark;
use benchmark_battery::automerge::{transaction::Transactable, AutoCommit, ObjId, ObjType, ROOT};

pub fn benchmarks() -> Vec<SampledBenchmark> {
    vec![
        SampledBenchmark::batched(
            "edit_trace",
            "edit_trace/edit_trace_single_tx",
            load_commands,
            edit_trace_single_tx,
        ),
        SampledBenchmark::batched(
            "edit_trace",
            "edit_trace/edit_trace_many_tx",
            load_commands,
            edit_trace_many_tx,
        ),
    ]
}

type Command = (usize, isize, String);

fn load_commands() -> (Vec<Command>, AutoCommit, ObjId) {
    let contents = include_str!("../../data/edits.json");
    let edits: serde_json::Value = serde_json::from_str(contents).expect("cant parse edits");
    let edits = edits.as_array().unwrap();
    let mut commands = vec![];
    for edit in edits {
        let edit = edit.as_array().unwrap();
        let pos = edit[0].as_u64().unwrap() as usize;
        let del = edit[1].as_i64().unwrap() as isize;
        let mut vals = String::new();
        for value in &edit[2..] {
            vals.push_str(value.as_str().unwrap());
        }
        commands.push((pos, del, vals));
    }
    let mut doc = AutoCommit::new();
    doc.update_diff_cursor();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    (commands, doc, text)
}

fn edit_trace_single_tx(
    (commands, mut doc, text): (Vec<(usize, isize, String)>, AutoCommit, ObjId),
) -> (Vec<Command>, AutoCommit, ObjId) {
    for (pos, del, vals) in commands.iter() {
        doc.splice_text(&text, *pos, *del, vals).unwrap();
    }
    doc.commit();
    (commands, doc, text)
}

fn edit_trace_many_tx(
    (commands, mut doc, text): (Vec<Command>, AutoCommit, ObjId),
) -> (Vec<Command>, AutoCommit, ObjId) {
    for (pos, del, vals) in commands.iter() {
        doc.splice_text(&text, *pos, *del, vals).unwrap();
        doc.commit();
    }
    (commands, doc, text)
}
