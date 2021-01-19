use automerge_frontend::Frontend;
use automerge_protocol as amp;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use maplit::hashmap;

pub fn statetree_apply_diff(c: &mut Criterion) {
    let actor_id = amp::ActorID::random();
    let make_list_opid = actor_id.op_id_at(1);
    let mut patches: Vec<amp::Patch> = vec![amp::Patch {
        actor: None,
        seq: None,
        clock: hashmap! {actor_id.clone() => 1},
        deps: Vec::new(),
        max_op: 1,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::MapType::Map,
            props: hashmap! {
                "text".to_string() => hashmap!{
                    make_list_opid.clone() => amp::Diff::Unchanged(amp::ObjDiff{
                        object_id: make_list_opid.clone().into(),
                        obj_type: amp::ObjType::text(),
                    }),
                }
            },
        })),
    }];
    for index in 0..6000 {
        let op_num = index + 2;
        let this_op_id = actor_id.op_id_at(op_num as u64);
        patches.push(amp::Patch{
            actor: None,
            seq: None,
            clock: hashmap!{actor_id.clone() => op_num as u64},
            deps: Vec::new(),
            max_op: op_num as u64,
            diffs: Some(amp::Diff::Map(amp::MapDiff{
                object_id: amp::ObjectID::Root,
                obj_type: amp::MapType::Map,
                props: hashmap!{
                    "text".to_string() => hashmap!{
                        make_list_opid.clone() => amp::Diff::Seq(amp::SeqDiff{
                            object_id: make_list_opid.clone().into(),
                            obj_type: amp::SequenceType::Text,
                            edits: vec![amp::DiffEdit::Insert{
                                index,
                                elem_id: this_op_id.clone().into(),
                            }],
                            props: hashmap!{
                                index => hashmap!{
                                    this_op_id => amp::Diff::Value(amp::ScalarValue::Str("c".to_string()))
                                }
                            }
                        })
                    }
                }
            })),
        });
    }
    c.bench_function("StateTreeValue::apply_diff", move |b| {
        b.iter_batched(
            || patches.clone(),
            |patches| {
                #[allow(clippy::unit_arg)]
                black_box({
                    let mut doc = Frontend::new();
                    for patch in patches.into_iter() {
                        doc.apply_patch(patch).unwrap()
                    }
                })
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = statetree_apply_diff,
}
criterion_main!(benches);
