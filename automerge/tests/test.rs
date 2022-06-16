use automerge::transaction::Transactable;
use automerge::{
    ActorId, ApplyOptions, AutoCommit, Automerge, AutomergeError, Change, ExpandedChange, ObjType,
    ScalarValue, Value, VecOpObserver, ROOT,
};

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

    let values = doc1.get_all(&list, 1)?;
    assert_eq!(values.len(), 3);
    assert_eq!(&values[0].0, &Value::counter(1));
    assert_eq!(&values[1].0, &Value::counter(10));
    assert_eq!(&values[2].0, &Value::counter(100));

    let values = doc1.get_all(&list, 2)?;
    assert_eq!(values.len(), 3);
    assert_eq!(&values[0].0, &Value::counter(1));
    assert_eq!(&values[1].0, &Value::counter(10));
    assert_eq!(&values[2].0, &Value::int(100));

    doc1.increment(&list, 1, 1)?;
    doc1.increment(&list, 2, 1)?;

    let values = doc1.get_all(&list, 1)?;
    assert_eq!(values.len(), 3);
    assert_eq!(&values[0].0, &Value::counter(2));
    assert_eq!(&values[1].0, &Value::counter(11));
    assert_eq!(&values[2].0, &Value::counter(101));

    let values = doc1.get_all(&list, 2)?;
    assert_eq!(values.len(), 2);
    assert_eq!(&values[0].0, &Value::counter(2));
    assert_eq!(&values[1].0, &Value::counter(11));

    assert_eq!(doc1.length(&list), 3);

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
    assert_eq!(
        doc.increment(ROOT, "nothing", 2),
        Err(AutomergeError::MissingCounter)
    );

    // can't increment a non-counter
    doc.put(ROOT, "non-counter", "mystring").unwrap();
    assert_eq!(
        doc.increment(ROOT, "non-counter", 2),
        Err(AutomergeError::MissingCounter)
    );

    // can increment a counter still
    doc.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
    assert_eq!(doc.increment(ROOT, "counter", 2), Ok(()));

    // can increment a counter that is part of a conflict
    let mut doc1 = AutoCommit::new();
    doc1.set_actor(ActorId::from([1]));
    let mut doc2 = AutoCommit::new();
    doc2.set_actor(ActorId::from([2]));

    doc1.put(ROOT, "key", ScalarValue::counter(1)).unwrap();
    doc2.put(ROOT, "key", "mystring").unwrap();
    doc1.merge(&mut doc2).unwrap();

    assert_eq!(doc1.increment(ROOT, "key", 2), Ok(()));
}

#[test]
fn increment_non_counter_list() {
    let mut doc = AutoCommit::new();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();

    // can't increment a non-counter
    doc.insert(&list, 0, "mystring").unwrap();
    assert_eq!(
        doc.increment(&list, 0, 2),
        Err(AutomergeError::MissingCounter)
    );

    // can increment a counter
    doc.insert(&list, 0, ScalarValue::counter(1)).unwrap();
    assert_eq!(doc.increment(&list, 0, 2), Ok(()));

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

    assert_eq!(doc1.increment(&list, 0, 2), Ok(()));
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
