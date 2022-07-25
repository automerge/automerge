use automerge::transaction::Transactable;
use automerge::{
    ActorId, ApplyOptions, AutoCommit, Automerge, AutomergeError, Change, ExpandedChange, ObjType,
    ScalarValue, VecOpObserver, ROOT,
};

// set up logging for all the tests
use test_log::test;

mod helpers;
#[allow(unused_imports)]
use helpers::{
    mk_counter, new_doc, new_doc_with_actor, pretty_print, realize, realize_obj, sorted_actors,
    RealizedObject,
};
use pretty_assertions::assert_eq;

#[test]
fn no_conflict_on_repeated_assignment() {
    let mut doc = AutoCommit::new();
    doc.put(&automerge::ROOT, "foo", 1).unwrap();
    doc.put(&automerge::ROOT, "foo", 2).unwrap();
    assert_doc!(
        doc.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc.document(),
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
        doc1.document(),
        map! {
            "foo" => {  "bar" },
            "hello" => { "world" },
        }
    );
    doc2.merge(&mut doc1).unwrap();
    assert_doc!(
        doc2.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc2.document(),
        map! {
            "birds" => {list![
                {"blackbird"},
                {"goldfinch"},
            ]}
        }
    );

    assert_doc!(
        doc1.document(),
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
        doc1.document(),
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
        doc1.document(),
        map! {
            "birds" => {list![
                { "blackbird" },
                { "starling" }
            ]}
        }
    );

    doc2.merge(&mut doc1).unwrap();
    assert_doc!(
        doc2.document(),
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
        doc1.document(),
        map! {
            "birds" => {list![
                { "albatross" },
                { "cormorant" }
            ]}
        }
    );

    doc2.merge(&mut doc1).unwrap();
    assert_doc!(
        doc2.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc1.document(),
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
        doc2.document(),
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
        doc2.document(),
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
        doc2.document(),
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
        .unwrap()
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
    let changes = doc.get_changes(&[]).unwrap().into_iter().cloned();

    let mut doc = AutoCommit::new();
    let mut observer = VecOpObserver::default();
    doc.apply_changes_with(
        changes,
        ApplyOptions::default().with_op_observer(&mut observer),
    )
    .unwrap();
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
    doc1.splice(&text, 0, 0, "hello".chars().map(|c| c.to_string().into()))
        .unwrap();

    let mut doc2 = AutoCommit::load(&doc1.save()).unwrap();
    doc2.set_actor(actor2);

    assert_doc! {doc2.document(), map!{
        "text" => { list![{"h"}, {"e"}, {"l"}, {"l"}, {"o"}]},
    }};

    doc2.splice(&text, 4, 1, Vec::new()).unwrap();
    doc2.splice(&text, 4, 0, vec!["!".into()]).unwrap();
    doc2.splice(&text, 5, 0, vec![" ".into()]).unwrap();
    doc2.splice(&text, 6, 0, "world".chars().map(|c| c.into()))
        .unwrap();

    assert_doc!(
        doc2.document(),
        map! {
            "text" => { list![{"h"}, {"e"}, {"l"}, {"l"}, {"!"}, {" "}, {"w"} , {"o"}, {"r"}, {"l"}, {"d"}]}
        }
    );

    let mut doc3 = AutoCommit::load(&doc2.save()).unwrap();

    assert_doc!(
        doc3.document(),
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

    let changes = doc4.get_changes(&[]).unwrap();
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
    #[cfg(not(feature = "storage-v2"))]
    change.compress();
    let compressed = change.compressed_bytes().to_vec();
    assert!(compressed.len() < uncompressed.len());

    let reloaded = automerge::Change::try_from(&compressed[..]).unwrap();
    assert_eq!(change.raw_bytes(), reloaded.raw_bytes());
}

#[cfg(feature = "storage-v2")]
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

#[cfg(feature = "storage-v2")]
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
    let changes1: Vec<Change> = doc.get_changes(&[]).unwrap().into_iter().cloned().collect();
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
