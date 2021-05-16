use automerge_frontend::Frontend;
use automerge_protocol as amp;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use maplit::hashmap;

pub fn sequential_inserts_in_multiple_patches(c: &mut Criterion) {
    let actor_id = amp::ActorId::random();
    let make_list_opid = actor_id.op_id_at(1);
    let mut patches: Vec<amp::Patch> = vec![amp::Patch {
        actor: None,
        seq: None,
        clock: hashmap! {actor_id.clone() => 1},
        deps: Vec::new(),
        max_op: 1,
        pending_changes: 0,
        diffs: amp::MapDiff {
            object_id: amp::ObjectId::Root,
            obj_type: amp::MapType::Map,
            props: hashmap! {
                "text".to_string() => hashmap!{
                    make_list_opid.clone() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: make_list_opid.clone().into(),
                        obj_type: amp::SequenceType::Text,
                        edits: Vec::new(),
                    }),
                }
            },
        },
    }];
    for index in 0..6000 {
        let op_num = index + 2;
        let this_op_id = actor_id.op_id_at(op_num as u64);
        patches.push(amp::Patch {
            actor: None,
            seq: None,
            clock: hashmap! {actor_id.clone() => op_num as u64},
            deps: Vec::new(),
            max_op: op_num as u64,
            pending_changes: 0,
            diffs: amp::MapDiff {
                object_id: amp::ObjectId::Root,
                obj_type: amp::MapType::Map,
                props: hashmap! {
                    "text".to_string() => hashmap!{
                        make_list_opid.clone() => amp::Diff::Seq(amp::SeqDiff{
                            object_id: make_list_opid.clone().into(),
                            obj_type: amp::SequenceType::Text,
                            edits: vec![amp::DiffEdit::SingleElementInsert{
                                index,
                                elem_id: this_op_id.clone().into(),
                                op_id: this_op_id.clone(),
                                value: amp::Diff::Value(amp::ScalarValue::Str("c".to_string())),
                            }],
                        })
                    }
                },
            },
        });
    }
    c.bench_function(
        "StateTreeValue::apply_diff sequential text inserts across multiple patches",
        move |b| {
            b.iter_batched(
                || {
                    let doc = Frontend::new();
                    (doc, patches.clone())
                },
                |(mut doc, patches)| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for patch in patches.into_iter() {
                            doc.apply_patch(patch).unwrap();
                        }
                        doc
                    })
                },
                BatchSize::SmallInput,
            )
        },
    );
}

pub fn sequential_inserts_in_single_patch(c: &mut Criterion) {
    let actor_id = amp::ActorId::random();
    let make_list_opid = actor_id.op_id_at(1);
    let mut edits: Vec<amp::DiffEdit> = Vec::new();
    for index in 0..6000 {
        let op_num = index + 2;
        let this_op_id = actor_id.op_id_at(op_num as u64);
        edits.push(amp::DiffEdit::SingleElementInsert {
            index,
            elem_id: this_op_id.clone().into(),
            op_id: this_op_id.clone(),
            value: amp::Diff::Value(amp::ScalarValue::Str("c".to_string())),
        });
    }
    let patch: amp::Patch = amp::Patch {
        actor: None,
        seq: None,
        clock: hashmap! {actor_id => 1},
        deps: Vec::new(),
        max_op: 1,
        pending_changes: 0,
        diffs: amp::MapDiff {
            object_id: amp::ObjectId::Root,
            obj_type: amp::MapType::Map,
            props: hashmap! {
                "text".to_string() => hashmap!{
                    make_list_opid.clone() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: make_list_opid.into(),
                        obj_type: amp::SequenceType::Text,
                        edits,
                    }),
                }
            },
        },
    };
    c.bench_function(
        "StateTreeValue::apply_diff sequential text inserts in a single patch",
        move |b| {
            b.iter_batched(
                || patch.clone(),
                |patch| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        let mut doc = Frontend::new();
                        doc.apply_patch(patch).unwrap()
                    })
                },
                BatchSize::SmallInput,
            )
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = sequential_inserts_in_multiple_patches, sequential_inserts_in_single_patch,
}
criterion_main!(benches);
