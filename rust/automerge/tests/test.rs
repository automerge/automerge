use automerge::marks::{ExpandMark, Mark};
//use automerge::op_tree::B;
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    sync::SyncDoc, ActorId, Author, AutoCommit, Automerge, AutomergeError, Change, ExpandedChange,
    ObjId, ObjType, Patch, PatchAction, PatchLog, Prop, ReadDoc, ScalarValue, SequenceTree, Value,
    ROOT,
};

const B: usize = 16;

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
fn save_restore_complex1() {
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
    let changes = doc1.get_changes(&[]).into_iter().collect::<Vec<_>>();
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
    let mut v = [ActorId::random(), ActorId::random(), ActorId::random()];
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

    doc1.dump();
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
    let changes = doc.get_changes(&[]).into_iter();

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
    let mut v = [ActorId::random(), ActorId::random(), ActorId::random()];
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
    let c = &changes[2];
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
    let unexpanded: automerge::Change = expanded.into();
    assert_eq!(unexpanded.raw_bytes(), change_bytes);
}

#[test]
fn save_and_load_incremented_counter() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
    doc.commit();
    doc.increment(ROOT, "counter", 1).unwrap();
    doc.commit();
    let changes1: Vec<Change> = doc.get_changes(&[]).into_iter().collect();
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
        println!("{:?}", path.as_ref().unwrap().path().display());
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
    doc2.apply_changes(change.into_iter().collect::<Vec<_>>())
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
    let mut patch_log = PatchLog::active();
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
    let mut values = SequenceTree::new();
    for i in 0..=N {
        values.push((
            Value::Scalar(std::borrow::Cow::Owned(ScalarValue::Int(i as i64))),
            ObjId::Id((2 * (i + 1) + 1) as u64, doc.get_actor().clone(), 0),
            false,
        ));
    }
    let expected_patches = vec![
        Patch {
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
        },
        Patch {
            obj: ObjId::Id(1, doc.get_actor().clone(), 0),
            path: vec![(ROOT, Prop::Map("list".into()))],
            action: PatchAction::Insert { index: 0, values },
        },
    ];
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
    let mut patch_log = PatchLog::active();
    new_doc
        .apply_changes_log_patches(vec![change1], &mut patch_log)
        .unwrap();
    new_doc
        .apply_changes_log_patches(vec![change2], &mut patch_log)
        .unwrap();

    let patches = new_doc.make_patches(&mut patch_log);
    println!("PATCH = {:?}", patches.last());
    let matches = match &patches.last().unwrap().action {
        PatchAction::PutSeq { index: N, .. } => true,
        PatchAction::Insert { index: 0, values } if values.len() == N + 1 => true,
        _ => false,
    };
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
    println!("---- A ");
    doc1.put(&ROOT, "size", 200)?;
    println!("---- N ");
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

#[test]
fn test_load_incremental_partial_load() {
    let mut doc = Automerge::new();

    let mut tx = doc.transaction();
    tx.put(&ROOT, "a", 1).unwrap();
    tx.commit();

    let start_heads = doc.get_heads();
    let mut tx = doc.transaction();
    tx.put(&ROOT, "b", 2).unwrap();
    tx.commit();

    let changes = doc.get_changes(&start_heads);

    let encoded = changes.into_iter().fold(Vec::new(), |mut acc, mut change| {
        acc.extend_from_slice(change.bytes().as_ref());
        acc
    });

    let mut doc2 = Automerge::new();
    doc2.load_incremental(&encoded).unwrap();
}

#[test]
fn test_get_change_meta() {
    let mut doc = Automerge::new();

    let mut tx = doc.transaction();
    tx.put(&ROOT, "a", 1).unwrap();
    tx.commit();

    let start_heads = doc.get_heads();
    let mut tx = doc.transaction();
    tx.put(&ROOT, "b", 2).unwrap();
    tx.commit();

    let changes = doc.get_changes_meta(&start_heads);

    assert_eq!(changes.len(), 1);
    assert_eq!(*changes[0].actor, *doc.get_actor());
    assert_eq!(changes[0].seq, 2);
}

#[test]
fn get_marks_at_heads() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text_id = tx.put_object(&ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&text_id, 0, 0, "hello world").unwrap();
    let mark = Mark::new("bold".to_string(), true, 0, 10);
    tx.mark(&text_id, mark, ExpandMark::After).unwrap();
    tx.commit();

    let heads = doc.get_heads();

    let mut tx = doc.transaction();
    tx.mark(
        &text_id,
        Mark::new("bold".to_string(), ScalarValue::Null, 0, 10),
        ExpandMark::None,
    )
    .unwrap();
    let mark_map = tx.get_marks(&text_id, 1, Some(&heads)).unwrap();
    assert_eq!(mark_map.len(), 1);
    let (mark_name, mark_value) = mark_map.iter().next().unwrap();
    assert_eq!(mark_name, "bold");
    assert_eq!(mark_value, &ScalarValue::Boolean(true));

    tx.commit();

    let mark_map = doc.get_marks(&text_id, 1, Some(&heads)).unwrap();
    assert_eq!(mark_map.len(), 1);
    let (mark_name, mark_value) = mark_map.iter().next().unwrap();
    assert_eq!(mark_name, "bold");
    assert_eq!(mark_value, &ScalarValue::Boolean(true));
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

#[test]
fn rollback_with_no_ops() {
    let mut doc = Automerge::new();

    doc.transact::<_, _, AutomergeError>(|tx| {
        tx.put(ROOT, "a", 1)?;
        Ok::<_, AutomergeError>(())
    })
    .unwrap();

    let mut doc2 = doc.fork();

    let tx = doc2.transaction();
    tx.commit();

    let mut doc3 = doc.fork();
    doc3.transact::<_, _, AutomergeError>(|tx| {
        tx.put(ROOT, "b", 2)?;
        Ok::<_, AutomergeError>(())
    })
    .unwrap();

    doc2.merge(&mut doc3).unwrap();

    let tx = doc2.transaction();
    tx.rollback();
}

#[test]
fn rollback_with_several_actors() {
    let mut doc1 = AutoCommit::new().with_actor("aaaaaa".try_into().unwrap());
    let text = doc1.put_object(&ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&text, 0, 0, "the sly fox jumped over the lazy dog")
        .unwrap();
    let map_a = doc1.put_object(&ROOT, "map_a", ObjType::Map).unwrap();
    doc1.put(&map_a, "key1", "value1a").unwrap();
    doc1.put(&map_a, "key2", "value2a").unwrap();

    let mut doc2 = doc1.fork().with_actor("cccccc".try_into().unwrap());
    doc2.splice_text(&text, 8, 3, "monkey").unwrap();
    doc2.splice_text(&text, 36, 3, "pig").unwrap();
    let map_c = doc2.put_object(&ROOT, "map_c", ObjType::Map).unwrap();
    doc2.put(&map_a, "key2", "value2c").unwrap();
    doc2.put(&map_a, "key3", "value3c").unwrap();
    doc2.put(&map_c, "key1", "value").unwrap();

    let mut doc3 = doc2.fork().with_actor("bbbbbb".try_into().unwrap());
    doc3.splice_text(&text, 8, 5, "zebra").unwrap();
    let map_b = doc3.put_object(&ROOT, "map_b", ObjType::Map).unwrap();
    doc3.put(&map_a, "key1", "value3b").unwrap();
    doc3.put(&map_a, "key3", "value3b").unwrap();
    doc3.put(&map_b, "key1", "value").unwrap();
    doc3.rollback();

    assert_eq!(doc3.save(), doc2.save());
}

#[test]
fn save_with_ops_which_reference_actors_only_via_delete() {
    let mut doc = Automerge::new();

    doc.transact::<_, _, AutomergeError>(|tx| {
        tx.put(ROOT, "a", 1)?;
        Ok::<_, AutomergeError>(())
    })
    .unwrap();

    let mut forked = doc.fork();
    forked
        .transact::<_, _, AutomergeError>(|tx| {
            tx.delete(ROOT, "a")?;
            Ok::<_, AutomergeError>(())
        })
        .unwrap();

    doc.merge(&mut forked).unwrap();

    // `doc` now contains a delete op which uses the actor of the fork. Delete
    // ops don't exist explicitly in the document ops, they are referenced in
    // the "successors" of the encoded ops. This means that when we're encoding
    // actor IDs into the document we need to check that any actor IDs which
    // are referenced in the `successors` of an op are encoded as well.

    let saved = doc.save();
    // This will panic if we failed to encode the referenced actor ID
    let _ = Automerge::load(&saved).unwrap();
}

#[test]
fn save_with_empty_commits() {
    let mut doc = Automerge::new();

    doc.transact::<_, _, AutomergeError>(|tx| {
        tx.put(ROOT, "a", 1)?;
        Ok::<_, AutomergeError>(())
    })
    .unwrap();

    let mut forked = doc.fork();
    forked.empty_commit(CommitOptions::default());

    doc.merge(&mut forked).unwrap();

    let saved = doc.save();
    // This will panic if we failed to encode the referenced actor ID
    let _ = Automerge::load(&saved).unwrap();
}

#[test]
fn large_patches_in_lists_are_correct() {
    // Reproduces a bug caused by an incorrect use of ListEncoding in Automerge::live_obj_paths.
    // This is a function which precalculates the path of every visible object in the document.
    // The problem was that when calculating the index into a sequence it was using
    // ListEncoding::List to determine the index, which meant that when a string was inserted into
    // a list then the index of elements following the list was based on the number of elements in
    // the string, when it should just increase the index by one for the whole string.
    //
    // This bug was a little tricky to track down because it was only triggered by an optimization
    // which kicks in when there are > 100 patches to render.

    let mut doc = Automerge::new();
    let heads_before = doc.get_heads();
    let list = doc
        .transact::<_, _, AutomergeError>(|tx| {
            let list = tx.put_object(ROOT, "list", ObjType::List)?;
            // This should just count as one
            tx.insert(&list, 0, "123456")?;
            for i in 1..501 {
                let inner = tx.insert_object(&list, i, ObjType::Map)?;
                tx.put(&inner, "a", i as i64)?;
            }
            Ok(list)
        })
        .unwrap()
        .result;
    let heads_after = doc.get_heads();
    let patches = doc.diff(&heads_before, &heads_after);
    let final_patch = patches.last().unwrap();
    assert_eq!(
        final_patch.path,
        vec![
            (ROOT, Prop::Map("list".into())),
            (list, Prop::Seq(500)) // In the buggy code this was incorrectly coming out as 505 due to
                                   // the counting of "123456" as 6 elements rather than 1
        ]
    );
    let PatchAction::PutMap { .. } = &final_patch.action else {
        panic!("Expected PutMap, got {:?}", final_patch.action);
    };
}

#[test]
fn diff_should_reverse_deletion_of_object_in_list_correctly() {
    let mut doc = AutoCommit::new();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, "a").unwrap();
    let text = doc
        .insert_object(&list, 1, automerge::ObjType::Text)
        .unwrap();
    doc.splice_text(&text, 0, 0, "b").unwrap();
    doc.insert(&list, 2, "c").unwrap();

    let heads_before = doc.get_heads();
    doc.delete(&list, 1).unwrap();
    let heads_after = doc.get_heads();

    doc.update_diff_cursor();
    let patches = doc.diff(&heads_after, &heads_before);

    assert_eq!(patches.len(), 2);
    let patch = patches[0].clone();
    let PatchAction::Insert { index, values } = &patch.action else {
        panic!("Expected Insert, got {:?}", patch.action);
    };
    assert_eq!(*index, 1);
    assert_eq!(values.len(), 1);
    let (value, _, _) = values.into_iter().next().unwrap();
    assert_eq!(value, &Value::Object(ObjType::Text));

    let patch = patches[1].clone();
    let PatchAction::SpliceText { index, value, .. } = patch.action else {
        panic!("Expected SpliceText, got {:?}", patch.action);
    };
    assert_eq!(index, 0);
    assert_eq!(value.make_string(), "b");
}

#[test]
fn diff_should_reverse_deletion_of_object_in_map_correctly() {
    let mut doc = AutoCommit::new();

    let map = doc.put_object(ROOT, "map", ObjType::Map).unwrap();
    doc.put_object(&map, "text", ObjType::Text).unwrap();

    doc.put(&map, "a", "a").unwrap();
    let text = doc.put_object(&map, "b", automerge::ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "b").unwrap();
    doc.put(&map, "c", "c").unwrap();

    let heads_before = doc.get_heads();
    doc.delete(&map, "b").unwrap();
    let heads_after = doc.get_heads();

    doc.update_diff_cursor();
    let patches = doc.diff(&heads_after, &heads_before);

    assert_eq!(patches.len(), 2);
    let patch = patches[0].clone();
    let PatchAction::PutMap { key, value, .. } = &patch.action else {
        panic!("Expected putmap, got {:?}", patch.action);
    };
    assert_eq!(key, "b");
    assert_eq!(value.0, Value::Object(ObjType::Text));

    let patch = patches[1].clone();
    let PatchAction::SpliceText { index, value, .. } = patch.action else {
        panic!("Expected SpliceText, got {:?}", patch.action);
    };
    assert_eq!(index, 0);
    assert_eq!(value.make_string(), "b");
}

#[test]
fn diff_should_reverse_deletion_of_block_in_text_correctly() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "a").unwrap();
    let block = doc.split_block(&text, 1).unwrap();
    doc.splice_text(&text, 2, 0, "b").unwrap();
    doc.put(&block, "key", "value").unwrap();

    let heads_before = doc.get_heads();
    doc.delete(&text, 1).unwrap();
    let heads_after = doc.get_heads();

    doc.update_diff_cursor();
    let patches = doc.diff(&heads_after, &heads_before);

    assert_eq!(patches.len(), 2);
    let patch = patches[0].clone();
    let PatchAction::Insert { index, values } = &patch.action else {
        panic!("Expected Insert, got {:?}", patch.action);
    };
    assert_eq!(*index, 1);
    assert_eq!(values.len(), 1);
    let (value, _, _) = values.into_iter().next().unwrap();
    assert_eq!(value, &Value::Object(ObjType::Map));

    let patch = patches[1].clone();
    let PatchAction::PutMap { key, value, .. } = patch.action else {
        panic!("Expected PutMap, got {:?}", patch.action);
    };
    assert_eq!(key, "key");
    let Value::Scalar(s) = value.0 else {
        panic!("Expected Scalar, got {:?}", value.0);
    };
    assert_eq!(s.as_ref(), &ScalarValue::Str("value".into()));
}

#[test]
fn missing_actors_when_docs_are_forked() {
    // Reproduces https://github.com/automerge/automerge/issues/897
    //
    // The problem was a result of these things interacting:
    //
    // 1. When we create a transaction we add the actor ID of the document
    //    creating the transaction to the IndexedCache of actor IDs that
    //    document stores
    // 2. When we fork a document we copy the IndexedCache from the source
    //    document to the forked document
    // 3. When we save a document we must encode all the actor IDs in the saved
    //    document in lexicographic order. To do this we first enumerate all
    //    the actor IDs in the change graph and then encode this in the
    //    document
    // 4. We assume that the IndexedCache of actor IDs on the document only
    //    contains actor IDs which are in the change graph
    //
    // What can happen is that we create a new actor ID somehow (by forking or
    // loading). Then we create a transaction with the new actor ID but never
    // actually make any changes. Then, we create another actor ID in the
    // same document - by forking it typically. This means that this last
    // document has an IndexedCache with an actor ID in it which will never
    // be saved to the document, but which is followed by an actor ID which
    // will be saved. This in turn means that the indexes we save to the
    // document are off by one and so we get load errors.
    //
    // The solution was to create the lookup table from actor index to actor
    // ID directly from the actor IDs in the change graph rather than from the
    // IndexedCache.
    let actor0 = ActorId::from(&[0]);
    let actor1 = ActorId::from(&[1]);
    let actor2 = ActorId::from(&[2]);

    let mut doc0 = AutoCommit::new().with_actor(actor0);
    doc0.put(ROOT, "a", 1).unwrap();

    // swap these actors and no error occurs
    let mut doc1 = doc0.fork().with_actor(actor2);
    let mut doc2 = doc0.fork().with_actor(actor1);

    doc1.put(ROOT, "b", 2).unwrap();
    doc2.merge(&mut doc1).unwrap();

    let s1 = doc2.save();

    // This call creates a transaction which doesn't do anything (because the
    // "c" key doesn't exist) and so the actor ID (actor1) gets added to the
    // IndexedCache of doc2
    doc2.delete(ROOT, "c").unwrap();

    // error occurs here
    let s2 = doc2.save_and_verify().unwrap();

    assert_eq!(s1, s2);
}

#[test]
fn allows_empty_keys_in_mappings() {
    let mut doc = AutoCommit::new();
    doc.put(&automerge::ROOT, "", 1).unwrap();
    assert_doc!(
        &doc,
        map! {
            "" => { 1 },
        }
    );
}

#[test]
fn has_our_changes() {
    let mut left = AutoCommit::new();
    left.put(&automerge::ROOT, "a", 1).unwrap();

    let mut right = AutoCommit::new();
    right.put(&automerge::ROOT, "b", 2).unwrap();

    let mut left_to_right = automerge::sync::State::new();
    let mut right_to_left = automerge::sync::State::new();

    assert!(!left.has_our_changes(&left_to_right));
    assert!(!right.has_our_changes(&right_to_left));

    while !left.has_our_changes(&left_to_right) || !right.has_our_changes(&right_to_left) {
        let mut quiet = true;
        if let Some(msg) = left.sync().generate_sync_message(&mut left_to_right) {
            quiet = false;
            right
                .sync()
                .receive_sync_message(&mut right_to_left, msg)
                .unwrap();
        }
        if let Some(msg) = right.sync().generate_sync_message(&mut right_to_left) {
            quiet = false;
            left.sync()
                .receive_sync_message(&mut left_to_right, msg)
                .unwrap();
        }
        if quiet {
            panic!("no messages sent but the sync state says we're not in sync");
        }
    }
    assert!(right.has_our_changes(&right_to_left));
}

#[test]
fn stats_smoke_test() {
    let mut doc = AutoCommit::new();
    doc.put(&automerge::ROOT, "a", 1).unwrap();
    doc.commit();
    doc.put(&automerge::ROOT, "b", 2).unwrap();
    doc.commit();
    let stats = doc.stats();
    assert_eq!(stats.num_changes, 2);
    assert_eq!(stats.num_ops, 2);
}

#[test]
fn invalid_index() {
    let mut doc = AutoCommit::new();
    let obj = doc
        .put_object(&automerge::ROOT, "a", ObjType::List)
        .unwrap();
    doc.insert(&obj, 0, 1).unwrap();
    doc.put(&obj, 0, 2).unwrap();
    assert_eq!(doc.get(&obj, 0).unwrap().unwrap().0, 2.into());
    assert_eq!(doc.insert(&obj, 2, 1), Err(AutomergeError::InvalidIndex(2)));
    assert_eq!(doc.put(&obj, 2, 2), Err(AutomergeError::InvalidIndex(2)));
    assert_eq!(
        doc.insert(&obj, 100, 1),
        Err(AutomergeError::InvalidIndex(100))
    );
    assert_eq!(
        doc.put(&obj, 100, 2),
        Err(AutomergeError::InvalidIndex(100))
    );
}

#[test]
fn zero_length_data() {
    let mut doc = AutoCommit::new();
    doc.put(&ROOT, "string", "").unwrap();
    doc.put(&ROOT, "bytes", vec![]).unwrap();
    doc.commit();
    assert_eq!(
        doc.get(&ROOT, "string").unwrap().unwrap().0,
        Value::from("")
    );
    assert_eq!(
        doc.get(&ROOT, "bytes").unwrap().unwrap().0,
        Value::from(vec![])
    );
}

#[test]
fn make_sure_load_incremental_doesnt_skip_a_load_with_a_common_head() {
    let mut doc1 = AutoCommit::new();
    doc1.put(&ROOT, "string", "hello").unwrap();
    let mut doc2 = doc1.fork();
    let mut doc3 = doc1.fork();

    assert!(doc1.get_heads().len() == 1);

    doc1.put(&ROOT, "concurrent1", "123").unwrap();
    assert!(doc1.get_heads().len() == 1);
    let hash_b = doc1.get_heads()[0];

    doc3.load_incremental(&doc1.save()).unwrap();
    assert!(doc3.get_heads().len() == 1);
    let hash_c = doc3.get_heads()[0];

    assert_eq!(hash_b, hash_c);

    doc2.put(&ROOT, "concurrent2", "abc").unwrap();
    assert!(doc2.get_heads().len() == 1);
    let hash_d = doc2.get_heads()[0];

    doc2.merge(&mut doc1).unwrap();
    let heads = doc2.get_heads();

    assert!(heads.len() == 2);
    assert!(heads.contains(&hash_d));
    assert!(heads.contains(&hash_b));

    doc3.load_incremental(&doc2.save()).unwrap();

    assert!(doc3.get_heads() == doc2.get_heads());
}

#[test]
fn test_get_last_local_change_generation() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello world").unwrap();
    confirm_last_change(&mut doc);
    doc.splice_text(&text, 5, 1, "X").unwrap();
    confirm_last_change(&mut doc);
    doc.splice_text(&text, 6, 1, "").unwrap();
    confirm_last_change(&mut doc);
    doc.splice_text(&text, 0, 0, "ten thousand and five hundred")
        .unwrap();
    confirm_last_change(&mut doc);
}

fn confirm_last_change(doc: &mut AutoCommit) {
    let heads = doc.get_heads();
    let change = doc.get_last_local_change().unwrap();
    assert_eq!(vec![change.hash()], heads);
}

#[test]
fn test_overwriting_a_conflict() {
    let mut doc1 = AutoCommit::new();
    let mut doc2 = doc1.fork();

    // put the same values
    doc1.put(&ROOT, "key", "value").unwrap();
    doc2.put(&ROOT, "key", "value").unwrap();
    doc1.merge(&mut doc2).unwrap();
    doc2.merge(&mut doc1).unwrap();

    assert_eq!(doc1.get_all(&ROOT, "key").unwrap().len(), 2);
    assert_eq!(doc2.get_all(&ROOT, "key").unwrap().len(), 2);

    doc1.put(&ROOT, "key", "value").unwrap();
    doc2.put(&ROOT, "key", "value").unwrap();
    doc1.merge(&mut doc2).unwrap();
    doc2.merge(&mut doc1).unwrap();

    assert_eq!(doc1.get_all(&ROOT, "key").unwrap().len(), 1);
    assert_eq!(doc2.get_all(&ROOT, "key").unwrap().len(), 1);
}

#[test]
fn get_changes_with_hash_of_empty_change_produces_correct_result() {
    // This test reproduces an issue where if you create an empty change, then
    // call Automerge::get_changes(&[hash_of_empty_change]) the result would
    // include the hash of the change just created but it should be empty. The
    // reason this happend was that `get_changes` works by walking the change
    // graph and combining all the (actor, seq) pairs to form a vector clock
    // which represents the ancestors which should be filtered out. The logic
    // which performed this walking combined clocks by comparing the `max_op` of
    // two changes, but in the case of an empty change the `max_op` didn't
    // change and this meant that the clock was not updated with the sequence
    // number of the empty change. This meant that the clock did not include the
    // empty change seq and so the empty change was not filtered out.
    let mut doc = AutoCommit::new();
    let head = doc.empty_change(CommitOptions::default());
    let changes = doc.get_changes(&[head]);
    assert!(changes.is_empty());
}

#[test]
fn reproduce_clock_cache_bug() {
    // This test exercises an issue with clock caching. The problem manifested
    // as two documents which have common history returning different results
    // for `Automerge::get_changes(&common_heads)` where `common_heads` is a
    // set of change hashes which are in both documents.
    // `Automerge::get_changes(&heads)` returns everything in a document which
    // is _not_ an ancestor of the heads specified. In two documents which
    // contain `heads`, `get_changes(&heads)` should therefore return the same
    // thing.
    //
    // In order to compute the ancestors of the common heads, automerge converts
    // the heads to a vector clock. Every commit in an Automerge document has an
    // (actor ID, sequence number) pair (a lamport timestamp) which identifies
    // the commit. We can walk the commit graph accumulating these pairs to form
    // a vector clock. This vector clock can then be used to determine if any
    // given commit is an ancestor of the given heads.
    //
    // E.g. imagine this graph where I denote the lamport timestamp as (actor
    // ID, sequence number):
    //
    //                       * (a, 1)
    //                       |
    //                       * (a, 2)
    //                     /   \
    //                    /      \
    //                   * (b, 1) * (c, 1)
    //                    \      /
    //                     \   /
    //                       * (d, 1)
    //
    // Then say we have the heads for (b, 1), we can walk from (b,1) to the root
    // and we will end up with a vector clock of [(a, 2), (b, 1)]. We can then
    // use this vector clock to determine that (b,1) is an ancestor of (a,1)
    // because the clock for (b,1) contains (a,2), which is greater than (a,1)
    //
    // Creating these clocks for large documents is expensive, so we cache
    // clocks for every 16 commits in the document. The problem this test
    // exposes is that the logic for the clock caching was incorrect. When
    // walking the graph, the cache logic could accidentally omit some
    // commits from the cached clock.
    //
    // The caching logic was expressed by starting with the end node and then
    // walking backwards in a depth first search until the cache limit is
    // reached, it looked something like this:
    //
    // let limit = CACHE_STEP * 2
    // let clock = new_clock()
    // let to_visit = [start node]
    // let visited = []
    // while let Some(node) = to_visit.pop() {
    //     // Process node
    //     if let Some(cached) = get_cache(node) {
    //         merge(cached, clock)
    //     } else if visited.len() <= limit {
    //         to_visit.extend(parents_of(node))
    //     } else {
    //         break;
    //     }
    // }
    //
    // The logic would then restart from the remaining nodes in the to_visit queue.
    //
    // The bug is that in some scenarios we would not add the parents of the
    // node which causes us to reach the cache limit to the to_visit queue.
    //
    // The easiest way to observe this bug is to create a document where there
    // is a commit which has a large number of parents (i.e. a merge commit
    // from many branches). E.g imagine a document like this:
    //
    //                     A
    //                     |
    //         .--.--.--.--.--.--.--.--.
    //         B  C  D  E  F  G  H  I  J
    //         |  |  |  |  |  |  |  |  |
    //         K  L  M  N  O  P  Q  R  S
    //         '--'--'--'--'--'--'--'--'
    //                    |
    //                    T
    //                    |
    //                    U
    //
    // Now, let's say our CACHE_STEP is 3. Then every third change which is
    // applied is cached. U is the 21st change and so it will be cached. The
    // cache logic will now step backwards through the graph until 6 (CACHE_STEP
    // * 2) nodes have been visited, and then it will stop. In this case that
    // would mean that we add T to the list of nodes to visit, then pop it and
    // process it, adding all of T's parents to the list of nodes to visit.
    // Then we continue processing until we reach O so the final order is
    // T, K, L, M, N, O. Here's where the bug is, the cache logic now returns
    // and says "now keep going with the rest of the to_visit list", but it
    // didn't add the parents of O to the list and so F never gets processed.
    //
    // This is only a problem if F is a commit with a different actor ID then
    // O, otherwise the (actor_id, seq) if O covers F.
    //
    // So, to reproduce this error, we create a lot of branches, and on each
    // branch make a lot of commits - each commit with a different actor.
    // Then we merge the branches together and observe that `get_changes(&heads)`
    // returns empty. If the caching logic is incorrect the heads will be
    // converted into a clock which doesn't cover commits like F and then
    // `get_changes(&heads)` will be non empty.

    let mut base = AutoCommit::new();

    // Add some number of initial commits
    for i in 0..100 {
        base.put(ROOT, format!("initial_commit_{}", i), true)
            .unwrap();
        base.commit();
    }

    const NUM_BRANCHES: usize = 20;
    const COMMITS_PER_BRANCH: usize = 2;

    let mut branches = (0..NUM_BRANCHES - 1)
        .map(|_| base.fork())
        .collect::<Vec<_>>();
    branches.push(base);

    for (branch_no, branch) in branches.iter_mut().enumerate() {
        for commit_no in 0..COMMITS_PER_BRANCH {
            branch
                .put(ROOT, format!("branch_{}-{}", branch_no, commit_no), true)
                .unwrap();
            branch.commit();
            // Make a new actor for the next commit
            *branch = branch.fork();
        }
    }

    let mut base = branches.pop().unwrap();

    for branch in &mut branches {
        base.merge(branch).unwrap();
    }

    // Create a bunch of commits after the merge to ensure a clock is cached
    // between the document heads and the branches
    for i in 0..100 {
        base.put(ROOT, format!("after-merge-{}", i), true).unwrap();
        base.commit();
    }
    let heads = base.get_heads();

    assert!(base.get_changes(&heads).is_empty());
}

#[test]
fn authorship() {
    let author1 = Author::from(vec![1, 1, 1]);
    let author2 = Author::from(vec![2, 2, 2]);
    let mut doc1 = AutoCommit::new();
    doc1.put(ROOT, "key", "value1").unwrap();
    let change = doc1.get_last_local_change().unwrap();
    assert_eq!(change.seq(), 1);
    assert_eq!(change.author(), None);
    assert!(change.extra_bytes().is_empty());

    let mut doc2 = doc1.fork();

    doc1.set_author(Some(author1.clone()));
    doc2.set_author(Some(author2.clone()));

    doc1.put(ROOT, "key", "value2").unwrap();
    let change = doc1.get_last_local_change().unwrap();
    assert_eq!(change.seq(), 1);
    assert_eq!(change.author(), Some(author1.as_bytes()));
    assert_eq!(change.extra_bytes(), &[1, 3, 1, 1, 1]);

    doc1.put(ROOT, "key", "value3").unwrap();
    let change = doc1.get_last_local_change().unwrap();
    assert_eq!(change.seq(), 2);
    assert_eq!(change.author(), None);
    assert!(change.extra_bytes().is_empty());

    doc2.put(ROOT, "key", "value4").unwrap();
    let change = doc2.get_last_local_change().unwrap();
    assert_eq!(change.seq(), 1);
    assert_eq!(change.author(), Some(author2.as_bytes()));
    assert_eq!(change.extra_bytes(), &[1, 3, 2, 2, 2]);

    doc1.merge(&mut doc2).unwrap();

    let authors = doc1.get_authors();
    assert_eq!(authors, vec![author1.clone(), author2.clone()]);
    let actors1 = doc1.get_actors_for_author(&author1);
    let actors2 = doc1.get_actors_for_author(&author2);
    assert_eq!(actors1.len(), 1);
    assert_eq!(actors2.len(), 1);
    assert_eq!(doc1.get_author_for_actor(&actors1[0]), Some(&author1));
    assert_eq!(doc1.get_author_for_actor(&actors2[0]), Some(&author2));

    let doc3 = AutoCommit::load(&doc1.save()).unwrap();

    let authors = doc3.get_authors();
    assert_eq!(authors, vec![author1.clone(), author2.clone()]);
    let actors1 = doc3.get_actors_for_author(&author1);
    let actors2 = doc3.get_actors_for_author(&author2);
    assert_eq!(actors1.len(), 1);
    assert_eq!(actors2.len(), 1);
    assert_eq!(doc3.get_author_for_actor(&actors1[0]), Some(&author1));
    assert_eq!(doc3.get_author_for_actor(&actors2[0]), Some(&author2));
}
