use automerge::marks::{ExpandMark, Mark};
use automerge::op_tree::B;
use automerge::patches::TextRepresentation;
use automerge::transaction::Transactable;
use automerge::{
    ActorId, AutoCommit, Automerge, AutomergeError, Change, ExpandedChange, ObjId, ObjType, Patch,
    PatchAction, PatchLog, Prop, ReadDoc, ScalarValue, SequenceTree, Value, ROOT,
};
use std::fs;

// set up logging for all the tests
use test_log::test;

#[allow(unused_imports)]
use automerge_test::{
    assert_doc, assert_obj, list, map, mk_counter, new_doc, new_doc_with_actor, pretty_print,
    realize, realize_obj, sorted_actors, RealizedObject,
};
use pretty_assertions::assert_eq;

#[test]
fn no_conflict_on_repeated_assignment() {
    let mut doc = AutoCommit::new();
    doc.put(&automerge::ROOT, "foo", 1).unwrap();
    doc.put(&automerge::ROOT, "foo", 2).unwrap();
    assert_doc!(
        &doc,
        map! {
            "foo" => { 2 },
        }
    );
}

#[test]
fn repeated_map_assignment_which_resolves_conflict_not_ignored() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    doc1.put(&automerge::ROOT, "field", 123).unwrap();
    doc2.merge(&mut doc1).unwrap();
    doc2.put(&automerge::ROOT, "field", 456).unwrap();
    doc1.put(&automerge::ROOT, "field", 789).unwrap();
    doc1.merge(&mut doc2).unwrap();
    assert_eq!(doc1.get_all(&automerge::ROOT, "field").unwrap().len(), 2);

    doc1.put(&automerge::ROOT, "field", 123).unwrap();
    assert_doc!(
        &doc1,
        map! {
            "field" => { 123 }
        }
    );
}

#[test]
fn repeated_list_assignment_which_resolves_conflict_not_ignored() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .put_object(&automerge::ROOT, "list", ObjType::List)
        .unwrap();
    doc1.insert(&list_id, 0, 123).unwrap();
    doc2.merge(&mut doc1).unwrap();
    doc2.put(&list_id, 0, 456).unwrap();
    doc1.merge(&mut doc2).unwrap();
    doc1.put(&list_id, 0, 789).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "list" => {
                 list![
                    { 789 },
                ]
            }
        }
    );
}

#[test]
fn list_deletion() {
    let mut doc = new_doc();
    let list_id = doc
        .put_object(&automerge::ROOT, "list", ObjType::List)
        .unwrap();
    doc.insert(&list_id, 0, 123).unwrap();
    doc.insert(&list_id, 1, 456).unwrap();
    doc.insert(&list_id, 2, 789).unwrap();
    doc.delete(&list_id, 1).unwrap();
    assert_doc!(
        &doc,
        map! {
            "list" => { list![
                { 123 },
                { 789 },
            ]}
        }
    )
}

#[test]
fn merge_concurrent_map_prop_updates() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    doc1.put(&automerge::ROOT, "foo", "bar").unwrap();
    doc2.put(&automerge::ROOT, "hello", "world").unwrap();
    doc1.merge(&mut doc2).unwrap();
    assert_eq!(
        doc1.get(&automerge::ROOT, "foo").unwrap().unwrap().0,
        "bar".into()
    );
    assert_doc!(
        &doc1,
        map! {
            "foo" => {  "bar" },
            "hello" => { "world" },
        }
    );
    doc2.merge(&mut doc1).unwrap();
    assert_doc!(
        &doc2,
        map! {
            "foo" => { "bar" },
            "hello" => { "world" },
        }
    );
    assert_eq!(realize(doc1.document()), realize(doc2.document()));
}

#[test]
fn add_concurrent_increments_of_same_property() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    doc1.put(&automerge::ROOT, "counter", mk_counter(0))
        .unwrap();
    doc2.merge(&mut doc1).unwrap();
    doc1.increment(&automerge::ROOT, "counter", 1).unwrap();
    doc2.increment(&automerge::ROOT, "counter", 2).unwrap();
    doc1.merge(&mut doc2).unwrap();
    assert_doc!(
        &doc1,
        map! {
            "counter" => {
                mk_counter(3)
            }
        }
    );
}

#[test]
fn add_increments_only_to_preceeded_values() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    doc1.put(&automerge::ROOT, "counter", mk_counter(0))
        .unwrap();
    doc1.increment(&automerge::ROOT, "counter", 1).unwrap();

    // create a counter in doc2
    doc2.put(&automerge::ROOT, "counter", mk_counter(0))
        .unwrap();
    doc2.increment(&automerge::ROOT, "counter", 3).unwrap();

    // The two values should be conflicting rather than added
    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "counter" => {
                mk_counter(1),
                mk_counter(3),
            }
        }
    );
}

#[test]
fn concurrent_updates_of_same_field() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    doc1.put(&automerge::ROOT, "field", "one").unwrap();
    doc2.put(&automerge::ROOT, "field", "two").unwrap();

    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "field" => {
                 "one",
                 "two",
            }
        }
    );
}

#[test]
fn concurrent_updates_of_same_list_element() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .put_object(&automerge::ROOT, "birds", ObjType::List)
        .unwrap();
    doc1.insert(&list_id, 0, "finch").unwrap();
    doc2.merge(&mut doc1).unwrap();
    doc1.put(&list_id, 0, "greenfinch").unwrap();
    doc2.put(&list_id, 0, "goldfinch").unwrap();

    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "birds" => {
                 list![{
                    "greenfinch",
                    "goldfinch",
                }]
            }
        }
    );
}

#[test]
fn assignment_conflicts_of_different_types() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let mut doc3 = new_doc();
    doc1.put(&automerge::ROOT, "field", "string").unwrap();
    doc2.put_object(&automerge::ROOT, "field", ObjType::List)
        .unwrap();
    doc3.put_object(&automerge::ROOT, "field", ObjType::Map)
        .unwrap();
    doc1.merge(&mut doc2).unwrap();
    doc1.merge(&mut doc3).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "field" => {
                "string",
                list!{},
                 map!{},
            }
        }
    );
}

#[test]
fn changes_within_conflicting_map_field() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    doc1.put(&automerge::ROOT, "field", "string").unwrap();
    let map_id = doc2
        .put_object(&automerge::ROOT, "field", ObjType::Map)
        .unwrap();
    doc2.put(&map_id, "innerKey", 42).unwrap();
    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "field" => {
                "string",
                map!{
                    "innerKey" => {
                        42,
                    }
                }
            }
        }
    );
}

#[test]
fn changes_within_conflicting_list_element() {
    let (actor1, actor2) = sorted_actors();
    let mut doc1 = new_doc_with_actor(actor1);
    let mut doc2 = new_doc_with_actor(actor2);
    let list_id = doc1
        .put_object(&automerge::ROOT, "list", ObjType::List)
        .unwrap();
    doc1.insert(&list_id, 0, "hello").unwrap();
    doc2.merge(&mut doc1).unwrap();

    let map_in_doc1 = doc1.put_object(&list_id, 0, ObjType::Map).unwrap();
    doc1.put(&map_in_doc1, "map1", true).unwrap();
    doc1.put(&map_in_doc1, "key", 1).unwrap();

    let map_in_doc2 = doc2.put_object(&list_id, 0, ObjType::Map).unwrap();
    doc1.merge(&mut doc2).unwrap();
    doc2.put(&map_in_doc2, "map2", true).unwrap();
    doc2.put(&map_in_doc2, "key", 2).unwrap();

    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "list" => {
                list![
                    {
                        map!{
                            "map2" => { true },
                            "key" => { 2 },
                        },
                        map!{
                            "key" => { 1 },
                            "map1" => { true },
                        }
                    }
                ]
            }
        }
    );
}

#[test]
fn concurrently_assigned_nested_maps_should_not_merge() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    let doc1_map_id = doc1
        .put_object(&automerge::ROOT, "config", ObjType::Map)
        .unwrap();
    doc1.put(&doc1_map_id, "background", "blue").unwrap();

    let doc2_map_id = doc2
        .put_object(&automerge::ROOT, "config", ObjType::Map)
        .unwrap();
    doc2.put(&doc2_map_id, "logo_url", "logo.png").unwrap();

    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "config" => {
                map!{
                    "background" => {"blue"}
                },
                map!{
                    "logo_url" => {"logo.png"}
                }
            }
        }
    );
}

#[test]
fn concurrent_insertions_at_different_list_positions() {
    let (actor1, actor2) = sorted_actors();
    let mut doc1 = new_doc_with_actor(actor1);
    let mut doc2 = new_doc_with_actor(actor2);
    assert!(doc1.get_actor() < doc2.get_actor());

    let list_id = doc1
        .put_object(&automerge::ROOT, "list", ObjType::List)
        .unwrap();

    doc1.insert(&list_id, 0, "one").unwrap();
    doc1.insert(&list_id, 1, "three").unwrap();
    doc2.merge(&mut doc1).unwrap();
    doc1.splice(&list_id, 1, 0, vec!["two".into()]).unwrap();
    doc2.insert(&list_id, 2, "four").unwrap();

    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "list" => {
                list![
                    {"one"},
                    {"two"},
                    {"three"},
                    {"four"},
                ]
            }
        }
    );
}

#[test]
fn concurrent_insertions_at_same_list_position() {
    let (actor1, actor2) = sorted_actors();
    let mut doc1 = new_doc_with_actor(actor1);
    let mut doc2 = new_doc_with_actor(actor2);
    assert!(doc1.get_actor() < doc2.get_actor());

    let list_id = doc1
        .put_object(&automerge::ROOT, "birds", ObjType::List)
        .unwrap();
    doc1.insert(&list_id, 0, "parakeet").unwrap();

    doc2.merge(&mut doc1).unwrap();
    doc1.insert(&list_id, 1, "starling").unwrap();
    doc2.insert(&list_id, 1, "chaffinch").unwrap();
    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "birds" => {
                 list![
                    {
                        "parakeet",
                    },
                    {
                        "chaffinch",
                    },
                    {
                        "starling",
                    },
                ]
            },
        }
    );
}

#[test]
fn concurrent_assignment_and_deletion_of_a_map_entry() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    doc1.put(&automerge::ROOT, "bestBird", "robin").unwrap();
    doc2.merge(&mut doc1).unwrap();
    doc1.delete(&automerge::ROOT, "bestBird").unwrap();
    doc2.put(&automerge::ROOT, "bestBird", "magpie").unwrap();

    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "bestBird" => {
                "magpie",
            }
        }
    );
}

#[test]
fn concurrent_assignment_and_deletion_of_list_entry() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .put_object(&automerge::ROOT, "birds", ObjType::List)
        .unwrap();
    doc1.insert(&list_id, 0, "blackbird").unwrap();
    doc1.insert(&list_id, 1, "thrush").unwrap();
    doc1.insert(&list_id, 2, "goldfinch").unwrap();
    doc2.merge(&mut doc1).unwrap();
    doc1.put(&list_id, 1, "starling").unwrap();
    doc2.delete(&list_id, 1).unwrap();

    assert_doc!(
        &doc2,
        map! {
            "birds" => {list![
                {"blackbird"},
                {"goldfinch"},
            ]}
        }
    );

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list![
                { "blackbird" },
                { "starling" },
                { "goldfinch" },
            ]}
        }
    );

    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list![
                { "blackbird" },
                { "starling" },
                { "goldfinch" },
            ]}
        }
    );
}

#[test]
fn insertion_after_a_deleted_list_element() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .put_object(&automerge::ROOT, "birds", ObjType::List)
        .unwrap();

    doc1.insert(&list_id, 0, "blackbird").unwrap();
    doc1.insert(&list_id, 1, "thrush").unwrap();
    doc1.insert(&list_id, 2, "goldfinch").unwrap();

    doc2.merge(&mut doc1).unwrap();

    doc1.splice(&list_id, 1, 2, Vec::new()).unwrap();

    doc2.splice(&list_id, 2, 0, vec!["starling".into()])
        .unwrap();

    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list![
                { "blackbird" },
                { "starling" }
            ]}
        }
    );

    doc2.merge(&mut doc1).unwrap();
    assert_doc!(
        &doc2,
        map! {
            "birds" => {list![
                { "blackbird" },
                { "starling" }
            ]}
        }
    );
}

#[test]
fn concurrent_deletion_of_same_list_element() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .put_object(&automerge::ROOT, "birds", ObjType::List)
        .unwrap();

    doc1.insert(&list_id, 0, "albatross").unwrap();
    doc1.insert(&list_id, 1, "buzzard").unwrap();
    doc1.insert(&list_id, 2, "cormorant").unwrap();

    doc2.merge(&mut doc1).unwrap();

    doc1.delete(&list_id, 1).unwrap();

    doc2.delete(&list_id, 1).unwrap();

    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list![
                { "albatross" },
                { "cormorant" }
            ]}
        }
    );

    doc2.merge(&mut doc1).unwrap();
    assert_doc!(
        &doc2,
        map! {
            "birds" => {list![
                { "albatross" },
                { "cormorant" }
            ]}
        }
    );
}

#[test]
fn concurrent_updates_at_different_levels() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    let animals = doc1
        .put_object(&automerge::ROOT, "animals", ObjType::Map)
        .unwrap();
    let birds = doc1.put_object(&animals, "birds", ObjType::Map).unwrap();
    doc1.put(&birds, "pink", "flamingo").unwrap();
    doc1.put(&birds, "black", "starling").unwrap();

    let mammals = doc1.put_object(&animals, "mammals", ObjType::List).unwrap();
    doc1.insert(&mammals, 0, "badger").unwrap();

    doc2.merge(&mut doc1).unwrap();

    doc1.put(&birds, "brown", "sparrow").unwrap();

    doc2.delete(&animals, "birds").unwrap();
    doc1.merge(&mut doc2).unwrap();

    assert_obj!(
        &doc1,
        &automerge::ROOT,
        "animals",
        map! {
            "mammals" => {
                list![{ "badger" }],
            }
        }
    );

    assert_obj!(
        doc2.document(),
        &automerge::ROOT,
        "animals",
        map! {
            "mammals" => {
                list![{ "badger" }],
            }
        }
    );
}

#[test]
fn concurrent_updates_of_concurrently_deleted_objects() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    let birds = doc1
        .put_object(&automerge::ROOT, "birds", ObjType::Map)
        .unwrap();
    let blackbird = doc1.put_object(&birds, "blackbird", ObjType::Map).unwrap();
    doc1.put(&blackbird, "feathers", "black").unwrap();

    doc2.merge(&mut doc1).unwrap();

    doc1.delete(&birds, "blackbird").unwrap();

    doc2.put(&blackbird, "beak", "orange").unwrap();

    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "birds" => {
                map!{},
            }
        }
    );
}

#[test]
fn does_not_interleave_sequence_insertions_at_same_position() {
    let (actor1, actor2) = sorted_actors();
    let mut doc1 = new_doc_with_actor(actor1);
    let mut doc2 = new_doc_with_actor(actor2);

    let wisdom = doc1
        .put_object(&automerge::ROOT, "wisdom", ObjType::List)
        .unwrap();
    doc2.merge(&mut doc1).unwrap();

    doc1.splice(
        &wisdom,
        0,
        0,
        vec![
            "to".into(),
            "be".into(),
            "is".into(),
            "to".into(),
            "do".into(),
        ],
    )
    .unwrap();

    doc2.splice(
        &wisdom,
        0,
        0,
        vec![
            "to".into(),
            "do".into(),
            "is".into(),
            "to".into(),
            "be".into(),
        ],
    )
    .unwrap();

    doc1.merge(&mut doc2).unwrap();

    assert_doc!(
        &doc1,
        map! {
            "wisdom" => {list![
                {"to"},
                {"do"},
                {"is"},
                {"to"},
                {"be"},
                {"to"},
                {"be"},
                {"is"},
                {"to"},
                {"do"},
            ]}
        }
    );
}

#[test]
fn mutliple_insertions_at_same_list_position_with_insertion_by_greater_actor_id() {
    let (actor1, actor2) = sorted_actors();
    assert!(actor2 > actor1);
    let mut doc1 = new_doc_with_actor(actor1);
    let mut doc2 = new_doc_with_actor(actor2);

    let list = doc1
        .put_object(&automerge::ROOT, "list", ObjType::List)
        .unwrap();
    doc1.insert(&list, 0, "two").unwrap();
    doc2.merge(&mut doc1).unwrap();

    doc2.insert(&list, 0, "one").unwrap();
    assert_doc!(
        &doc2,
        map! {
            "list" => { list![
                { "one" },
                { "two" },
            ]}
        }
    );
}

#[test]
fn mutliple_insertions_at_same_list_position_with_insertion_by_lesser_actor_id() {
    let (actor2, actor1) = sorted_actors();
    assert!(actor2 < actor1);
    let mut doc1 = new_doc_with_actor(actor1);
    let mut doc2 = new_doc_with_actor(actor2);

    let list = doc1
        .put_object(&automerge::ROOT, "list", ObjType::List)
        .unwrap();
    doc1.insert(&list, 0, "two").unwrap();
    doc2.merge(&mut doc1).unwrap();

    doc2.insert(&list, 0, "one").unwrap();
    assert_doc!(
        &doc2,
        map! {
            "list" => { list![
                { "one" },
                { "two" },
            ]}
        }
    );
}

#[test]
fn insertion_consistent_with_causality() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    let list = doc1
        .put_object(&automerge::ROOT, "list", ObjType::List)
        .unwrap();
    doc1.insert(&list, 0, "four").unwrap();
    doc2.merge(&mut doc1).unwrap();
    doc2.insert(&list, 0, "three").unwrap();
    doc1.merge(&mut doc2).unwrap();
    doc1.insert(&list, 0, "two").unwrap();
    doc2.merge(&mut doc1).unwrap();
    doc2.insert(&list, 0, "one").unwrap();

    assert_doc!(
        &doc2,
        map! {
            "list" => { list![
                {"one"},
                {"two"},
                {"three" },
                {"four"},
            ]}
        }
    );
}

#[test]
fn save_and_restore_empty() {
    let mut doc = new_doc();
    let loaded = Automerge::load(&doc.save()).unwrap();

    assert_doc!(&loaded, map! {});
}

#[test]
fn save_restore_complex() {
    let mut doc1 = new_doc();
    let todos = doc1
        .put_object(&automerge::ROOT, "todos", ObjType::List)
        .unwrap();

    let first_todo = doc1.insert_object(&todos, 0, ObjType::Map).unwrap();
    doc1.put(&first_todo, "title", "water plants").unwrap();
    doc1.put(&first_todo, "done", false).unwrap();

    let mut doc2 = new_doc();
    doc2.merge(&mut doc1).unwrap();
    doc2.put(&first_todo, "title", "weed plants").unwrap();

    doc1.put(&first_todo, "title", "kill plants").unwrap();
    doc1.merge(&mut doc2).unwrap();

    let reloaded = Automerge::load(&doc1.save()).unwrap();

    assert_doc!(
        &reloaded,
        map! {
            "todos" => {list![
                {map!{
                    "title" => {
                        "weed plants",
                        "kill plants",
                    },
                    "done" => {false},
                }}
            ]}
        }
    );
}

#[test]
fn handle_repeated_out_of_order_changes() -> Result<(), automerge::AutomergeError> {
    let mut doc1 = new_doc();
    let list = doc1.put_object(ROOT, "list", ObjType::List)?;
    doc1.insert(&list, 0, "a")?;
    let mut doc2 = doc1.fork();
    doc1.insert(&list, 1, "b")?;
    doc1.commit();
    doc1.insert(&list, 2, "c")?;
    doc1.commit();
    doc1.insert(&list, 3, "d")?;
    doc1.commit();
    let changes = doc1
        .get_changes(&[])
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    doc2.apply_changes(changes[2..].to_vec())?;
    doc2.apply_changes(changes[2..].to_vec())?;
    doc2.apply_changes(changes)?;
    assert_eq!(doc1.save(), doc2.save());
    Ok(())
}

#[test]
fn save_restore_complex_transactional() {
    let mut doc1 = Automerge::new();
    let first_todo = doc1
        .transact::<_, _, automerge::AutomergeError>(|d| {
            let todos = d.put_object(&automerge::ROOT, "todos", ObjType::List)?;
            let first_todo = d.insert_object(&todos, 0, ObjType::Map)?;
            d.put(&first_todo, "title", "water plants")?;
            d.put(&first_todo, "done", false)?;
            Ok(first_todo)
        })
        .unwrap()
        .result;

    let mut doc2 = Automerge::new();
    doc2.merge(&mut doc1).unwrap();
    doc2.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.put(&first_todo, "title", "weed plants")?;
        Ok(())
    })
    .unwrap();

    doc1.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.put(&first_todo, "title", "kill plants")?;
        Ok(())
    })
    .unwrap();
    doc1.merge(&mut doc2).unwrap();

    let reloaded = Automerge::load(&doc1.save()).unwrap();

    assert_doc!(
        &reloaded,
        map! {
            "todos" => {list![
                {map!{
                    "title" => {
                        "weed plants",
                        "kill plants",
                    },
                    "done" => {false},
                }}
            ]}
        }
    );
}

#[test]
fn list_counter_del() -> Result<(), automerge::AutomergeError> {
    let mut v = vec![ActorId::random(), ActorId::random(), ActorId::random()];
    v.sort();
    let actor1 = v[0].clone();
    let actor2 = v[1].clone();
    let actor3 = v[2].clone();

    let mut doc1 = new_doc_with_actor(actor1);

    let list = doc1.put_object(ROOT, "list", ObjType::List)?;
    doc1.insert(&list, 0, "a")?;
    doc1.insert(&list, 1, "b")?;
    doc1.insert(&list, 2, "c")?;

    let mut doc2 = AutoCommit::load(&doc1.save())?;
    doc2.set_actor(actor2);

    let mut doc3 = AutoCommit::load(&doc1.save())?;
    doc3.set_actor(actor3);

    doc1.put(&list, 1, ScalarValue::counter(0))?;
    doc2.put(&list, 1, ScalarValue::counter(10))?;
    doc3.put(&list, 1, ScalarValue::counter(100))?;

    doc1.put(&list, 2, ScalarValue::counter(0))?;
    doc2.put(&list, 2, ScalarValue::counter(10))?;
    doc3.put(&list, 2, 100)?;

    doc1.increment(&list, 1, 1)?;
    doc1.increment(&list, 2, 1)?;

    doc1.merge(&mut doc2).unwrap();
    doc1.merge(&mut doc3).unwrap();

    assert_obj!(
        doc1.document(),
        &automerge::ROOT,
        "list",
        list![
            {
                "a",
            },
            {
                ScalarValue::counter(1),
                ScalarValue::counter(10),
                ScalarValue::counter(100)
            },
            {
                ScalarValue::Int(100),
                ScalarValue::counter(1),
                ScalarValue::counter(10),
            }
        ]
    );

    doc1.increment(&list, 1, 1)?;
    doc1.increment(&list, 2, 1)?;

    assert_obj!(
        doc1.document(),
        &automerge::ROOT,
        "list",
        list![
            {
                "a",
            },
            {
                ScalarValue::counter(2),
                ScalarValue::counter(11),
                ScalarValue::counter(101)
            },
            {
                ScalarValue::counter(2),
                ScalarValue::counter(11),
            }
        ]
    );

    doc1.delete(&list, 2)?;

    assert_eq!(doc1.length(&list), 2);

    let doc4 = AutoCommit::load(&doc1.save())?;

    assert_eq!(doc4.length(&list), 2);

    doc1.delete(&list, 1)?;

    assert_eq!(doc1.length(&list), 1);

    let doc5 = AutoCommit::load(&doc1.save())?;

    assert_eq!(doc5.length(&list), 1);

    Ok(())
}

#[test]
fn observe_counter_change_application() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
    doc.increment(ROOT, "counter", 2).unwrap();
    doc.increment(ROOT, "counter", 5).unwrap();
    let changes = doc.get_changes(&[]).into_iter().cloned();

    let mut doc = AutoCommit::new();
    doc.apply_changes(changes).unwrap();
}

#[test]
fn increment_non_counter_map() {
    let mut doc = AutoCommit::new();
    // can't increment nothing
    assert!(matches!(
        doc.increment(ROOT, "nothing", 2),
        Err(AutomergeError::MissingCounter)
    ));

    // can't increment a non-counter
    doc.put(ROOT, "non-counter", "mystring").unwrap();
    assert!(matches!(
        doc.increment(ROOT, "non-counter", 2),
        Err(AutomergeError::MissingCounter)
    ));

    // can increment a counter still
    doc.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
    assert!(matches!(doc.increment(ROOT, "counter", 2), Ok(())));

    // can increment a counter that is part of a conflict
    let mut doc1 = AutoCommit::new();
    doc1.set_actor(ActorId::from([1]));
    let mut doc2 = AutoCommit::new();
    doc2.set_actor(ActorId::from([2]));

    doc1.put(ROOT, "key", ScalarValue::counter(1)).unwrap();
    doc2.put(ROOT, "key", "mystring").unwrap();
    doc1.merge(&mut doc2).unwrap();

    assert!(matches!(doc1.increment(ROOT, "key", 2), Ok(())));
}

#[test]
fn increment_non_counter_list() {
    let mut doc = AutoCommit::new();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();

    // can't increment a non-counter
    doc.insert(&list, 0, "mystring").unwrap();
    assert!(matches!(
        doc.increment(&list, 0, 2),
        Err(AutomergeError::MissingCounter)
    ));

    // can increment a counter
    doc.insert(&list, 0, ScalarValue::counter(1)).unwrap();
    assert!(matches!(doc.increment(&list, 0, 2), Ok(())));

    // can increment a counter that is part of a conflict
    let mut doc1 = AutoCommit::new();
    doc1.set_actor(ActorId::from([1]));
    let list = doc1.put_object(ROOT, "list", ObjType::List).unwrap();
    doc1.insert(&list, 0, ()).unwrap();
    let mut doc2 = doc1.fork();
    doc2.set_actor(ActorId::from([2]));

    doc1.put(&list, 0, ScalarValue::counter(1)).unwrap();
    doc2.put(&list, 0, "mystring").unwrap();
    doc1.merge(&mut doc2).unwrap();

    assert!(matches!(doc1.increment(&list, 0, 2), Ok(())));
}

#[test]
fn test_local_inc_in_map() {
    let mut v = vec![ActorId::random(), ActorId::random(), ActorId::random()];
    v.sort();
    let actor1 = v[0].clone();
    let actor2 = v[1].clone();
    let actor3 = v[2].clone();

    let mut doc1 = new_doc_with_actor(actor1);
    doc1.put(&automerge::ROOT, "hello", "world").unwrap();

    let mut doc2 = AutoCommit::load(&doc1.save()).unwrap();
    doc2.set_actor(actor2);

    let mut doc3 = AutoCommit::load(&doc1.save()).unwrap();
    doc3.set_actor(actor3);

    doc1.put(ROOT, "cnt", 20_u64).unwrap();
    doc2.put(ROOT, "cnt", ScalarValue::counter(0)).unwrap();
    doc3.put(ROOT, "cnt", ScalarValue::counter(10)).unwrap();
    doc1.merge(&mut doc2).unwrap();
    doc1.merge(&mut doc3).unwrap();

    assert_doc! {doc1.document(), map!{
        "cnt" => {
            20_u64,
            ScalarValue::counter(0),
            ScalarValue::counter(10),
        },
        "hello" => {"world"},
    }};

    doc1.increment(ROOT, "cnt", 5).unwrap();

    assert_doc! {doc1.document(), map!{
        "cnt" => {
            ScalarValue::counter(5),
            ScalarValue::counter(15),
        },
        "hello" => {"world"},
    }};
    let mut doc4 = AutoCommit::load(&doc1.save()).unwrap();
    assert_eq!(doc4.save(), doc1.save());
}

#[test]
fn test_merging_test_conflicts_then_saving_and_loading() {
    let (actor1, actor2) = sorted_actors();

    let mut doc1 = new_doc_with_actor(actor1);
    let text = doc1.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&text, 0, 0, "hello").unwrap();

    let mut doc2 = AutoCommit::load(&doc1.save()).unwrap();
    doc2.set_actor(actor2);

    assert_doc! {&doc2, map!{
        "text" => { list![{"h"}, {"e"}, {"l"}, {"l"}, {"o"}]},
    }};

    doc2.splice_text(&text, 4, 1, "").unwrap();
    doc2.splice_text(&text, 4, 0, "!").unwrap();
    doc2.splice_text(&text, 5, 0, " ").unwrap();
    doc2.splice_text(&text, 6, 0, "world").unwrap();

    assert_doc!(
        &doc2,
        map! {
            "text" => { list![{"h"}, {"e"}, {"l"}, {"l"}, {"!"}, {" "}, {"w"} , {"o"}, {"r"}, {"l"}, {"d"}]}
        }
    );

    let doc3 = AutoCommit::load(&doc2.save()).unwrap();

    assert_doc!(
        &doc3,
        map! {
            "text" => { list![{"h"}, {"e"}, {"l"}, {"l"}, {"!"}, {" "}, {"w"} , {"o"}, {"r"}, {"l"}, {"d"}]}
        }
    );
}

/// Surfaces an error which occurs when loading a document with a change which only contains a
/// delete operation. In this case the delete operation doesn't appear in the encoded document
/// operations except as a succ, so the max_op was calculated incorectly.
#[test]
fn delete_only_change() {
    let actor = automerge::ActorId::random();
    let mut doc1 = automerge::Automerge::new().with_actor(actor.clone());
    let list = doc1
        .transact::<_, _, automerge::AutomergeError>(|d| {
            let l = d.put_object(&automerge::ROOT, "list", ObjType::List)?;
            d.insert(&l, 0, 'a')?;
            Ok(l)
        })
        .unwrap()
        .result;

    let mut doc2 = automerge::Automerge::load(&doc1.save())
        .unwrap()
        .with_actor(actor.clone());
    doc2.transact::<_, _, automerge::AutomergeError>(|d| d.delete(&list, 0))
        .unwrap();

    let mut doc3 = automerge::Automerge::load(&doc2.save())
        .unwrap()
        .with_actor(actor.clone());
    doc3.transact(|d| d.insert(&list, 0, "b")).unwrap();

    let doc4 = automerge::Automerge::load(&doc3.save())
        .unwrap()
        .with_actor(actor);

    let changes = doc4.get_changes(&[]);
    assert_eq!(changes.len(), 3);
    let c = changes[2];
    assert_eq!(c.start_op().get(), 4);
}

/// Expose an error where a document which contained a create operation without any subsequent
/// operations targeting the created object did not load the object correctly.
#[test]
fn save_and_reload_create_object() {
    let actor = automerge::ActorId::random();
    let mut doc = automerge::Automerge::new().with_actor(actor);

    // Create a change containing an object but no other operations
    let list = doc
        .transact::<_, _, automerge::AutomergeError>(|d| {
            d.put_object(&automerge::ROOT, "foo", ObjType::List)
        })
        .unwrap()
        .result;

    // Save and load the change
    let mut doc2 = automerge::Automerge::load(&doc.save()).unwrap();
    doc2.transact::<_, _, automerge::AutomergeError>(|d| {
        d.insert(&list, 0, 1_u64)?;
        Ok(())
    })
    .unwrap();

    assert_doc!(&doc2, map! {"foo" => { list! [{1_u64}]}});

    let _doc3 = automerge::Automerge::load(&doc2.save()).unwrap();
}

#[test]
fn test_compressed_changes() {
    let mut doc = new_doc();
    // crate::storage::DEFLATE_MIN_SIZE is 250, so this should trigger compression
    doc.put(ROOT, "bytes", ScalarValue::Bytes(vec![10; 300]))
        .unwrap();
    let mut change = doc.get_last_local_change().unwrap().clone();
    let uncompressed = change.raw_bytes().to_vec();
    assert!(uncompressed.len() > 256);
    let compressed = change.bytes().to_vec();
    assert!(compressed.len() < uncompressed.len());

    let reloaded = automerge::Change::try_from(&compressed[..]).unwrap();
    assert_eq!(change.raw_bytes(), reloaded.raw_bytes());
}

#[test]
fn test_compressed_doc_cols() {
    // In this test, the keyCtr column is long enough for deflate compression to kick in, but the
    // keyStr column is short. Thus, the deflate bit gets set for keyCtr but not for keyStr.
    // When checking whether the columns appear in ascending order, we must ignore the deflate bit.
    let mut doc = new_doc();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    let mut expected = Vec::new();
    for i in 0..200 {
        doc.insert(&list, i, i as u64).unwrap();
        expected.push(i as u64);
    }
    let uncompressed = doc.save_nocompress();
    let compressed = doc.save();
    assert!(compressed.len() < uncompressed.len());
    let loaded = automerge::Automerge::load(&compressed).unwrap();
    assert_doc!(
        &loaded,
        map! {
            "list" => { expected}
        }
    );
}

#[test]
fn test_change_encoding_expanded_change_round_trip() {
    let change_bytes: Vec<u8> = vec![
        0x85, 0x6f, 0x4a, 0x83, // magic bytes
        0xb2, 0x98, 0x9e, 0xa9, // checksum
        1, 61, 0, 2, 0x12, 0x34, // chunkType: change, length, deps, actor '1234'
        1, 1, 252, 250, 220, 255, 5, // seq, startOp, time
        14, 73, 110, 105, 116, 105, 97, 108, 105, 122, 97, 116, 105, 111,
        110, // message: 'Initialization'
        0, 6, // actor list, column count
        0x15, 3, 0x34, 1, 0x42, 2, // keyStr, insert, action
        0x56, 2, 0x57, 1, 0x70, 2, // valLen, valRaw, predNum
        0x7f, 1, 0x78, // keyStr: 'x'
        1,    // insert: false
        0x7f, 1, // action: set
        0x7f, 19, // valLen: 1 byte of type uint
        1,  // valRaw: 1
        0x7f, 0, // predNum: 0
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, // 10 trailing bytes
    ];
    let change = automerge::Change::try_from(&change_bytes[..]).unwrap();
    assert_eq!(change.raw_bytes(), change_bytes);
    let expanded = automerge::ExpandedChange::from(&change);
    let unexpanded: automerge::Change = expanded.try_into().unwrap();
    assert_eq!(unexpanded.raw_bytes(), change_bytes);
}

#[test]
fn save_and_load_incremented_counter() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
    doc.commit();
    doc.increment(ROOT, "counter", 1).unwrap();
    doc.commit();
    let changes1: Vec<Change> = doc.get_changes(&[]).into_iter().cloned().collect();
    let json: Vec<_> = changes1
        .iter()
        .map(|c| serde_json::to_string(&c.decode()).unwrap())
        .collect();
    let changes2: Vec<Change> = json
        .iter()
        .map(|j| serde_json::from_str::<ExpandedChange>(j).unwrap().into())
        .collect();

    assert_eq!(changes1, changes2);
}

#[test]
fn load_incremental_with_corrupted_tail() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "key", ScalarValue::Str("value".into()))
        .unwrap();
    doc.commit();
    let mut bytes = doc.save();
    bytes.extend_from_slice(&[1, 2, 3, 4]);
    let mut loaded = Automerge::new();
    let loaded_len = loaded.load_incremental(&bytes).unwrap();
    assert_eq!(loaded_len, 1);
    assert_doc!(
        &loaded,
        map! {
            "key" => { "value" },
        }
    );
}

#[test]
fn load_doc_with_deleted_objects() {
    // Reproduces an issue where a document with deleted objects failed to load
    let mut doc = AutoCommit::new();
    doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.put_object(ROOT, "map", ObjType::Map).unwrap();
    doc.put_object(ROOT, "table", ObjType::Table).unwrap();
    doc.delete(&ROOT, "list").unwrap();
    doc.delete(&ROOT, "text").unwrap();
    doc.delete(&ROOT, "map").unwrap();
    doc.delete(&ROOT, "table").unwrap();
    let saved = doc.save();
    Automerge::load(&saved).unwrap();
}

#[test]
fn insert_after_many_deletes() {
    let mut doc = AutoCommit::new();
    let obj = doc.put_object(&ROOT, "object", ObjType::Map).unwrap();
    for i in 0..100 {
        doc.put(&obj, i.to_string(), i).unwrap();
        doc.delete(&obj, i.to_string()).unwrap();
    }
}

#[test]
fn simple_bad_saveload() {
    let mut doc = Automerge::new();
    doc.transact::<_, _, AutomergeError>(|d| {
        d.put(ROOT, "count", 0)?;
        Ok(())
    })
    .unwrap();

    doc.transact::<_, _, AutomergeError>(|_d| Ok(())).unwrap();

    doc.transact::<_, _, AutomergeError>(|d| {
        d.put(ROOT, "count", 0)?;
        Ok(())
    })
    .unwrap();

    let bytes = doc.save();
    Automerge::load(&bytes).unwrap();
}

#[test]
fn ops_on_wrong_objets() -> Result<(), AutomergeError> {
    let mut doc = AutoCommit::new();
    let list = doc.put_object(&automerge::ROOT, "list", ObjType::List)?;
    doc.insert(&list, 0, "a")?;
    doc.insert(&list, 1, "b")?;
    let e1 = doc.put(&list, "a", "AAA");
    assert_eq!(e1, Err(AutomergeError::InvalidOp(ObjType::List)));
    let e2 = doc.splice_text(&list, 0, 0, "hello world");
    assert_eq!(e2, Err(AutomergeError::InvalidOp(ObjType::List)));
    let map = doc.put_object(&automerge::ROOT, "map", ObjType::Map)?;
    doc.put(&map, "a", "AAA")?;
    doc.put(&map, "b", "BBB")?;
    let e3 = doc.insert(&map, 0, "b");
    assert_eq!(e3, Err(AutomergeError::InvalidOp(ObjType::Map)));
    let e4 = doc.splice_text(&map, 0, 0, "hello world");
    assert_eq!(e4, Err(AutomergeError::InvalidOp(ObjType::Map)));
    let text = doc.put_object(&automerge::ROOT, "text", ObjType::Text)?;
    doc.splice_text(&text, 0, 0, "hello world")?;
    let e5 = doc.put(&text, "a", "AAA");
    assert_eq!(e5, Err(AutomergeError::InvalidOp(ObjType::Text)));
    //let e6 = doc.insert(&text, 0, "b");
    //assert_eq!(e6, Err(AutomergeError::InvalidOp(ObjType::Text)));
    Ok(())
}

#[test]
fn fuzz_crashers() {
    let paths = fs::read_dir("./tests/fuzz-crashers").unwrap();

    for path in paths {
        // uncomment this line to figure out which fixture is crashing:
        // println!("{:?}", path.as_ref().unwrap().path().display());
        let bytes = fs::read(path.as_ref().unwrap().path());
        let res = Automerge::load(&bytes.unwrap());
        assert!(res.is_err());
    }
}

fn fixture(name: &str) -> Vec<u8> {
    fs::read("./tests/fixtures/".to_owned() + name).unwrap()
}

#[test]
fn overlong_leb() {
    // the value metadata says "2", but the LEB is only 1-byte long and there's an extra 0
    assert!(Automerge::load(&fixture("counter_value_has_incorrect_meta.automerge")).is_err());
    // the LEB is overlong (using 2 bytes where one would have sufficed)
    assert!(Automerge::load(&fixture("counter_value_is_overlong.automerge")).is_err());
    // the LEB is correct
    assert!(Automerge::load(&fixture("counter_value_is_ok.automerge")).is_ok());
}

#[test]
fn load() {
    fn check_fixture(name: &str) {
        let doc = Automerge::load(&fixture(name)).unwrap();
        let map_id = doc.get(ROOT, "a").unwrap().unwrap().1;
        assert_eq!(doc.get(map_id, "a").unwrap().unwrap().0, "b".into());
    }

    check_fixture("two_change_chunks.automerge");
    check_fixture("two_change_chunks_compressed.automerge");
    check_fixture("two_change_chunks_out_of_order.automerge");
}

#[test]
fn negative_64() {
    let mut doc = Automerge::new();
    assert!(doc.transact(|d| { d.put(ROOT, "a", -64_i64) }).is_ok())
}

#[test]
fn obj_id_64bits() {
    // this change has an opId of 2**42, which when cast to a 32-bit int gives 0.
    // The file should either fail to load (a limit of ~4 billion ops per doc seems reasonable), or be handled correctly.
    if let Ok(doc) = Automerge::load(&fixture("64bit_obj_id_change.automerge")) {
        let map_id = doc.get(ROOT, "a").unwrap().unwrap().1;
        assert!(map_id != ROOT)
    }

    // this fixture is the same as the above, but as a document chunk.
    if let Ok(doc) = Automerge::load(&fixture("64bit_obj_id_doc.automerge")) {
        let map_id = doc.get(ROOT, "a").unwrap().unwrap().1;
        assert!(map_id != ROOT)
    }
}

#[test]
fn bad_change_on_optree_node_boundary() {
    let mut doc = Automerge::new();
    doc.transact::<_, _, AutomergeError>(|d| {
        d.put(ROOT, "a", "z")?;
        d.put(ROOT, "b", 0)?;
        d.put(ROOT, "c", 0)?;
        Ok(())
    })
    .unwrap();
    let iterations = 15_u64;
    for i in 0_u64..iterations {
        doc.transact::<_, _, AutomergeError>(|d| {
            let s = "a".repeat(i as usize);
            d.put(ROOT, "a", s)?;
            d.put(ROOT, "b", i + 1)?;
            d.put(ROOT, "c", i + 1)?;
            Ok(())
        })
        .unwrap();
    }
    let mut doc2 = Automerge::load(doc.save().as_slice()).unwrap();
    doc.transact::<_, _, AutomergeError>(|d| {
        let i = iterations + 2;
        let s = "a".repeat(i as usize);
        d.put(ROOT, "a", s)?;
        d.put(ROOT, "b", i)?;
        d.put(ROOT, "c", i)?;
        Ok(())
    })
    .unwrap();
    let change = doc.get_changes(&doc2.get_heads());
    doc2.apply_changes(change.into_iter().cloned().collect::<Vec<_>>())
        .unwrap();
    Automerge::load(doc2.save().as_slice()).unwrap();
}

#[test]
fn regression_nth_miscount() {
    let mut doc = Automerge::new();
    doc.transact::<_, _, AutomergeError>(|d| {
        let list_id = d.put_object(ROOT, "listval", ObjType::List).unwrap();
        for i in 0..30 {
            d.insert(&list_id, i, ScalarValue::Null).unwrap();
            let map = d.put_object(&list_id, i, ObjType::Map).unwrap();
            d.put(map, "test", ScalarValue::Int(i.try_into().unwrap()))
                .unwrap();
        }
        Ok(())
    })
    .unwrap();
    for i in 0..30 {
        let (obj_type, list_id) = doc.get(ROOT, "listval").unwrap().unwrap();
        assert_eq!(obj_type, Value::Object(ObjType::List));
        let (obj_type, map_id) = doc.get(&list_id, i).unwrap().unwrap();
        assert_eq!(obj_type, Value::Object(ObjType::Map));
        let (obj_type, _) = doc.get(map_id, "test").unwrap().unwrap();
        assert_eq!(
            obj_type,
            Value::Scalar(std::borrow::Cow::Borrowed(&ScalarValue::Int(
                i.try_into().unwrap()
            )))
        )
    }
}

#[test]
fn regression_nth_miscount_smaller() {
    let mut doc = Automerge::new();
    doc.transact::<_, _, AutomergeError>(|d| {
        let list_id = d.put_object(ROOT, "listval", ObjType::List).unwrap();
        for i in 0..B * 4 {
            d.insert(&list_id, i, ScalarValue::Null).unwrap();
            d.put(&list_id, i, ScalarValue::Int(i.try_into().unwrap()))
                .unwrap();
        }
        Ok(())
    })
    .unwrap();
    for i in 0..B * 4 {
        let (obj_type, list_id) = doc.get(ROOT, "listval").unwrap().unwrap();
        assert_eq!(obj_type, Value::Object(ObjType::List));
        let (obj_type, _) = doc.get(list_id, i).unwrap().unwrap();
        assert_eq!(
            obj_type,
            Value::Scalar(std::borrow::Cow::Borrowed(&ScalarValue::Int(
                i.try_into().unwrap()
            )))
        )
    }
}

#[test]
fn regression_insert_opid() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let list_id = tx
        .put_object(&automerge::ROOT, "list", ObjType::List)
        .unwrap();
    tx.commit();

    let change1 = doc.get_last_local_change().unwrap().clone();
    let mut tx = doc.transaction();

    const N: usize = 30;
    for i in 0..=N {
        tx.insert(&list_id, i, ScalarValue::Null).unwrap();
        tx.put(&list_id, i, ScalarValue::Int(i as i64)).unwrap();
    }
    tx.commit();

    let change2 = doc.get_last_local_change().unwrap().clone();
    let mut new_doc = Automerge::new();
    let mut patch_log = PatchLog::active(TextRepresentation::String);
    new_doc
        .apply_changes_log_patches(vec![change1], &mut patch_log)
        .unwrap();
    new_doc
        .apply_changes_log_patches(vec![change2], &mut patch_log)
        .unwrap();

    for i in 0..=N {
        let (doc_val, _) = doc.get(&list_id, i).unwrap().unwrap();
        let (new_doc_val, _) = new_doc.get(&list_id, i).unwrap().unwrap();

        assert_eq!(
            doc_val,
            Value::Scalar(std::borrow::Cow::Owned(ScalarValue::Int(i as i64)))
        );
        assert_eq!(
            new_doc_val,
            Value::Scalar(std::borrow::Cow::Owned(ScalarValue::Int(i as i64)))
        );
    }

    let patches = new_doc.make_patches(&mut patch_log);

    let mut expected_patches = Vec::new();
    expected_patches.push(Patch {
        obj: ROOT,
        path: vec![],
        action: PatchAction::PutMap {
            key: "list".to_string(),
            value: (
                Value::Object(ObjType::List),
                ObjId::Id(1, doc.get_actor().clone(), 0),
            ),
            conflict: false,
        },
    });
    for i in 0..=N {
        let mut seq_tree = SequenceTree::new();
        seq_tree.push((
            Value::Scalar(std::borrow::Cow::Owned(ScalarValue::Null)),
            ObjId::Id(2 * (i + 1) as u64, doc.get_actor().clone(), 0),
            false,
        ));
        expected_patches.push(Patch {
            obj: ObjId::Id(1, doc.get_actor().clone(), 0),
            path: vec![(ROOT, Prop::Map("list".into()))],
            action: PatchAction::Insert {
                index: i,
                values: seq_tree,
                marks: None,
            },
        });
        expected_patches.push(Patch {
            obj: ObjId::Id(1, doc.get_actor().clone(), 0),
            path: vec![(ROOT, Prop::Map("list".into()))],
            action: PatchAction::PutSeq {
                index: i,
                value: (
                    Value::Scalar(std::borrow::Cow::Owned(ScalarValue::Int(i as i64))),
                    ObjId::Id((2 * (i + 1) + 1) as u64, doc.get_actor().clone(), 0),
                ),
                conflict: false,
            },
        });
    }
    assert_eq!(patches, expected_patches);
}

#[test]
fn big_list() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let list_id = tx.put_object(&ROOT, "list", ObjType::List).unwrap();
    tx.commit();

    let change1 = doc.get_last_local_change().unwrap().clone();
    let mut tx = doc.transaction();

    const N: usize = B;
    for i in 0..=N {
        tx.insert(&list_id, i, ScalarValue::Null).unwrap();
    }
    for i in 0..=N {
        tx.put_object(&list_id, i, ObjType::Map).unwrap();
    }
    tx.commit();

    let change2 = doc.get_last_local_change().unwrap().clone();
    let mut new_doc = Automerge::new();
    let mut patch_log = PatchLog::active(TextRepresentation::String);
    new_doc
        .apply_changes_log_patches(vec![change1], &mut patch_log)
        .unwrap();
    new_doc
        .apply_changes_log_patches(vec![change2], &mut patch_log)
        .unwrap();

    let patches = new_doc.make_patches(&mut patch_log);
    let matches = matches!(
        patches.last().unwrap(),
        Patch {
            action: PatchAction::PutSeq { index: N, .. },
            ..
        }
    );
    assert!(matches);
}

#[test]
fn marks() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();

    let text_id = tx.put_object(&ROOT, "text", ObjType::Text).unwrap();

    tx.splice_text(&text_id, 0, 0, "hello world").unwrap();

    let mark = Mark::new("bold".to_string(), true, 0, "hello".len());
    tx.mark(&text_id, mark, ExpandMark::Both).unwrap();

    // add " cool" (it will be bold because ExpandMark::Both)
    tx.splice_text(&text_id, "hello".len(), 0, " cool").unwrap();

    // unbold "hello"
    tx.unmark(&text_id, "bold", 0, "hello".len(), ExpandMark::Before)
        .unwrap();

    // insert "why " before hello.
    tx.splice_text(&text_id, 0, 0, "why ").unwrap();

    let marks = tx.marks(&text_id).unwrap();

    assert_eq!(marks[0].start, 9);
    assert_eq!(marks[0].end, 14);
    assert_eq!(marks[0].name(), "bold");
    assert_eq!(marks[0].value(), &ScalarValue::from(true));
}

#[test]
fn can_transaction_at() -> Result<(), AutomergeError> {
    let mut doc1 = Automerge::new();
    let mut tx = doc1.transaction();
    let txt = tx.put_object(&ROOT, "text", ObjType::Text).unwrap();
    tx.put(&ROOT, "size", 100).unwrap();
    tx.splice_text(&txt, 0, 0, "aaabbbccc")?;
    tx.commit();
    let heads1 = doc1.get_heads();
    let mut tx = doc1.transaction();
    assert_eq!(tx.text(&txt).unwrap(), "aaabbbccc");
    assert_eq!(tx.get(&ROOT, "size").unwrap().unwrap().0, Value::int(100));
    tx.splice_text(&txt, 3, 3, "QQQ")?;
    tx.put(&ROOT, "size", 200)?;
    assert_eq!(tx.text(&txt).unwrap(), "aaaQQQccc");
    assert_eq!(tx.get(&ROOT, "size").unwrap().unwrap().0, Value::int(200));
    tx.commit();

    let mut tx = doc1.transaction_at(PatchLog::null(), &heads1);
    assert_eq!(tx.text(&txt).unwrap(), "aaabbbccc");
    assert_eq!(tx.get(&ROOT, "size").unwrap().unwrap().0, Value::int(100));
    tx.splice_text(&txt, 3, 3, "ZZZ")?;
    tx.put(&ROOT, "size", 300)?;
    assert_eq!(tx.text(&txt).unwrap(), "aaaZZZccc");
    assert_eq!(tx.get(&ROOT, "size").unwrap().unwrap().0, Value::int(300));
    tx.commit();
    assert_eq!(doc1.text(&txt).unwrap(), "aaaZZZQQQccc");
    assert_eq!(doc1.get(&ROOT, "size").unwrap().unwrap().0, Value::int(300));

    let mut tx = doc1.transaction_at(PatchLog::null(), &heads1);
    assert_eq!(tx.text(&txt).unwrap(), "aaabbbccc");
    assert_eq!(tx.get(&ROOT, "size").unwrap().unwrap().0, Value::int(100));
    tx.splice_text(&txt, 3, 3, "TTT")?;
    tx.put(&ROOT, "size", 400)?;
    assert_eq!(tx.text(&txt).unwrap(), "aaaTTTccc");
    assert_eq!(tx.get(&ROOT, "size").unwrap().unwrap().0, Value::int(400));
    tx.commit();
    assert_eq!(doc1.text(&txt).unwrap(), "aaaTTTZZZQQQccc");
    assert_eq!(doc1.get(&ROOT, "size").unwrap().unwrap().0, Value::int(400));
    Ok(())
}

#[test]
fn can_isolate() -> Result<(), AutomergeError> {
    let mut doc1 = AutoCommit::new();
    let txt = doc1.put_object(&ROOT, "text", ObjType::Text).unwrap();
    doc1.put(&ROOT, "size", 100).unwrap();
    doc1.splice_text(&txt, 0, 0, "aaabbbccc")?;
    let heads1 = doc1.get_heads();
    doc1.put(&ROOT, "size", 150)?;

    doc1.isolate(&heads1);

    let mut doc2 = doc1.fork();
    doc2.put(&ROOT, "other", 999)?;
    doc2.splice_text(&txt, 9, 0, "111")?;

    assert_eq!(doc1.text(&txt).unwrap(), "aaabbbccc");
    assert_eq!(doc1.get(&ROOT, "size").unwrap().unwrap().0, Value::int(100));
    doc1.splice_text(&txt, 3, 3, "QQQ")?;
    doc1.put(&ROOT, "size", 200)?;
    assert_eq!(doc1.text(&txt).unwrap(), "aaaQQQccc");
    assert_eq!(doc1.get(&ROOT, "size").unwrap().unwrap().0, Value::int(200));

    let heads2 = doc1.get_heads();
    assert_eq!(doc1.text(&txt).unwrap(), "aaaQQQccc");

    doc1.merge(&mut doc2)?;
    assert_eq!(doc1.get(&ROOT, "size").unwrap().unwrap().0, Value::int(200));
    assert_eq!(doc1.get(&ROOT, "other").unwrap(), None);

    doc1.isolate(&heads1);

    assert_ne!(heads1, heads2);

    assert_eq!(doc1.text(&txt).unwrap(), "aaabbbccc");
    assert_eq!(doc1.get(&ROOT, "size").unwrap().unwrap().0, Value::int(100));
    doc1.splice_text(&txt, 3, 3, "ZZZ")?;
    doc1.put(&ROOT, "size", 300)?;
    assert_eq!(doc1.text(&txt).unwrap(), "aaaZZZccc");
    assert_eq!(doc1.get(&ROOT, "size").unwrap().unwrap().0, Value::int(300));

    let _heads3 = doc1.get_heads(); // commit
    assert_eq!(doc1.text(&txt).unwrap(), "aaaZZZccc");

    doc1.integrate();
    assert_eq!(doc1.text(&txt).unwrap(), "aaaZZZQQQccc111");
    assert_eq!(
        doc1.get(&ROOT, "other").unwrap().unwrap().0,
        Value::int(999)
    );

    doc1.isolate(&heads1);

    assert_eq!(doc1.text(&txt).unwrap(), "aaabbbccc");
    assert_eq!(doc1.get(&ROOT, "size").unwrap().unwrap().0, Value::int(100));
    doc1.splice_text(&txt, 3, 3, "TTT")?;
    doc1.put(&ROOT, "size", 400)?;
    assert_eq!(doc1.text(&txt).unwrap(), "aaaTTTccc");
    assert_eq!(doc1.get(&ROOT, "size").unwrap().unwrap().0, Value::int(400));

    let _heads4 = doc1.get_heads(); // commit
    assert_eq!(doc1.text(&txt).unwrap(), "aaaTTTccc");
    doc1.integrate();

    assert_eq!(doc1.text(&txt).unwrap(), "aaaTTTZZZQQQccc111");
    assert_eq!(doc1.get(&ROOT, "size").unwrap().unwrap().0, Value::int(400));
    Ok(())
}

#[test]
fn inserting_text_near_deleted_marks() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text_id = tx.put_object(&ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&text_id, 0, 0, "hello world").unwrap();
    let mark = Mark::new("bold".to_string(), true, 2, 8);
    tx.mark(&text_id, mark, ExpandMark::After).unwrap();
    let mark = Mark::new("link".to_string(), true, 3, 6);
    tx.mark(&text_id, mark, ExpandMark::None).unwrap();

    tx.splice_text(&text_id, 1, 10, "").unwrap(); // 'h'
    dbg!(tx.text(&text_id).unwrap(), tx.marks(&text_id).unwrap());
    tx.splice_text(&text_id, 0, 0, "a").unwrap(); // 'ah'
    dbg!(tx.text(&text_id).unwrap(), tx.marks(&text_id).unwrap());
    tx.splice_text(&text_id, 2, 0, "a").unwrap(); // 'ah<bold>a</bold>'
    dbg!(tx.text(&text_id).unwrap(), tx.marks(&text_id).unwrap());
}

/*
#[test]
fn conflicting_unicode_text_with_different_widths() -> Result<(), AutomergeError> {
    let mut doc1 = AutoCommit::new();
    let txt = doc1.put_object(&ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&txt, 0, 0, "abc")?;

    let mut doc2 = doc1.fork();

    doc1.put(&txt, 1, "B")?;
    doc2.put(&txt, 1, "🐻")?;

    assert_eq!(doc1.length(&txt), 3);
    assert_eq!(doc2.length(&txt), 4);

    doc1.merge(&mut doc2)?;
    doc2.merge(&mut doc1)?;

    let length = doc1.length(&txt);
    let last_value = doc1.get(&txt, length - 1)?;
    for n in 0..length {
        assert_eq!(doc1.get(&txt, n), doc2.get(&txt, n));
    }
    assert_eq!(last_value.unwrap().0, Value::from("c"));

    println!("list.len() == {:?}", length);
    assert_eq!(doc1.length(&txt), doc2.length(&txt));
    Ok(())
}
*/
