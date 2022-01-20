use automerge::{ActorId, Automerge, Value, ROOT};

mod helpers;
#[allow(unused_imports)]
use helpers::{
    mk_counter, new_doc, new_doc_with_actor, pretty_print, realize, realize_obj, sorted_actors,
    RealizedObject,
};
#[test]
fn no_conflict_on_repeated_assignment() {
    let mut doc = Automerge::new();
    doc.set(&automerge::ROOT, "foo", 1).unwrap();
    doc.set(&automerge::ROOT, "foo", 2).unwrap();
    assert_doc!(
        &doc,
        map! {
            "foo" => { 2 },
        }
    );
}

#[test]
fn no_change_on_repeated_map_set() {
    let mut doc = new_doc();
    doc.set(&automerge::ROOT, "foo", 1).unwrap();
    assert!(doc.set(&automerge::ROOT, "foo", 1).unwrap().is_none());
}

#[test]
fn no_change_on_repeated_list_set() {
    let mut doc = new_doc();
    let list_id = doc
        .set(&automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc.insert(&list_id, 0, 1).unwrap();
    doc.set(&list_id, 0, 1).unwrap();
    assert!(doc.set(&list_id, 0, 1).unwrap().is_none());
}

#[test]
fn no_change_on_list_insert_followed_by_set_of_same_value() {
    let mut doc = new_doc();
    let list_id = doc
        .set(&automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc.insert(&list_id, 0, 1).unwrap();
    assert!(doc.set(&list_id, 0, 1).unwrap().is_none());
}

#[test]
fn repeated_map_assignment_which_resolves_conflict_not_ignored() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    doc1.set(&automerge::ROOT, "field", 123).unwrap();
    doc2.merge(&mut doc1);
    doc2.set(&automerge::ROOT, "field", 456).unwrap();
    doc1.set(&automerge::ROOT, "field", 789).unwrap();
    doc1.merge(&mut doc2);
    assert_eq!(doc1.values(&automerge::ROOT, "field").unwrap().len(), 2);

    doc1.set(&automerge::ROOT, "field", 123).unwrap();
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
        .set(&automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(&list_id, 0, 123).unwrap();
    doc2.merge(&mut doc1);
    doc2.set(&list_id, 0, 456).unwrap();
    doc1.merge(&mut doc2);
    doc1.set(&list_id, 0, 789).unwrap();

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
        .set(&automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc.insert(&list_id, 0, 123).unwrap();
    doc.insert(&list_id, 1, 456).unwrap();
    doc.insert(&list_id, 2, 789).unwrap();
    doc.del(&list_id, 1).unwrap();
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
    doc1.set(&automerge::ROOT, "foo", "bar").unwrap();
    doc2.set(&automerge::ROOT, "hello", "world").unwrap();
    doc1.merge(&mut doc2);
    assert_eq!(
        doc1.value(&automerge::ROOT, "foo").unwrap().unwrap().0,
        "bar".into()
    );
    assert_doc!(
        &doc1,
        map! {
            "foo" => {  "bar" },
            "hello" => { "world" },
        }
    );
    doc2.merge(&mut doc1);
    assert_doc!(
        &doc2,
        map! {
            "foo" => { "bar" },
            "hello" => { "world" },
        }
    );
    assert_eq!(realize(&doc1), realize(&doc2));
}

#[test]
fn add_concurrent_increments_of_same_property() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    doc1.set(&automerge::ROOT, "counter", mk_counter(0))
        .unwrap();
    doc2.merge(&mut doc1);
    doc1.inc(&automerge::ROOT, "counter", 1).unwrap();
    doc2.inc(&automerge::ROOT, "counter", 2).unwrap();
    doc1.merge(&mut doc2);
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

    doc1.set(&automerge::ROOT, "counter", mk_counter(0))
        .unwrap();
    doc1.inc(&automerge::ROOT, "counter", 1).unwrap();

    // create a counter in doc2
    doc2.set(&automerge::ROOT, "counter", mk_counter(0))
        .unwrap();
    doc2.inc(&automerge::ROOT, "counter", 3).unwrap();

    // The two values should be conflicting rather than added
    doc1.merge(&mut doc2);

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
    doc1.set(&automerge::ROOT, "field", "one").unwrap();
    doc2.set(&automerge::ROOT, "field", "two").unwrap();

    doc1.merge(&mut doc2);

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
        .set(&automerge::ROOT, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(&list_id, 0, "finch").unwrap();
    doc2.merge(&mut doc1);
    doc1.set(&list_id, 0, "greenfinch").unwrap();
    doc2.set(&list_id, 0, "goldfinch").unwrap();

    doc1.merge(&mut doc2);

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
    doc1.set(&automerge::ROOT, "field", "string").unwrap();
    doc2.set(&automerge::ROOT, "field", automerge::Value::list())
        .unwrap();
    doc3.set(&automerge::ROOT, "field", automerge::Value::map())
        .unwrap();
    doc1.merge(&mut doc2);
    doc1.merge(&mut doc3);

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
    doc1.set(&automerge::ROOT, "field", "string").unwrap();
    let map_id = doc2
        .set(&automerge::ROOT, "field", automerge::Value::map())
        .unwrap()
        .unwrap();
    doc2.set(&map_id, "innerKey", 42).unwrap();
    doc1.merge(&mut doc2);

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
        .set(&automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(&list_id, 0, "hello").unwrap();
    doc2.merge(&mut doc1);

    let map_in_doc1 = doc1
        .set(&list_id, 0, automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.set(&map_in_doc1, "map1", true).unwrap();
    doc1.set(&map_in_doc1, "key", 1).unwrap();

    let map_in_doc2 = doc2
        .set(&list_id, 0, automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.merge(&mut doc2);
    doc2.set(&map_in_doc2, "map2", true).unwrap();
    doc2.set(&map_in_doc2, "key", 2).unwrap();

    doc1.merge(&mut doc2);

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
        .set(&automerge::ROOT, "config", automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.set(&doc1_map_id, "background", "blue").unwrap();

    let doc2_map_id = doc2
        .set(&automerge::ROOT, "config", automerge::Value::map())
        .unwrap()
        .unwrap();
    doc2.set(&doc2_map_id, "logo_url", "logo.png").unwrap();

    doc1.merge(&mut doc2);

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
    assert!(doc1.maybe_get_actor().unwrap() < doc2.maybe_get_actor().unwrap());

    let list_id = doc1
        .set(&automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();

    doc1.insert(&list_id, 0, "one").unwrap();
    doc1.insert(&list_id, 1, "three").unwrap();
    doc2.merge(&mut doc1);
    doc1.splice(&list_id, 1, 0, vec!["two".into()]).unwrap();
    doc2.insert(&list_id, 2, "four").unwrap();

    doc1.merge(&mut doc2);

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
    assert!(doc1.maybe_get_actor().unwrap() < doc2.maybe_get_actor().unwrap());

    let list_id = doc1
        .set(&automerge::ROOT, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(&list_id, 0, "parakeet").unwrap();

    doc2.merge(&mut doc1);
    doc1.insert(&list_id, 1, "starling").unwrap();
    doc2.insert(&list_id, 1, "chaffinch").unwrap();
    doc1.merge(&mut doc2);

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
    doc1.set(&automerge::ROOT, "bestBird", "robin").unwrap();
    doc2.merge(&mut doc1);
    doc1.del(&automerge::ROOT, "bestBird").unwrap();
    doc2.set(&automerge::ROOT, "bestBird", "magpie").unwrap();

    doc1.merge(&mut doc2);

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
        .set(&automerge::ROOT, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(&list_id, 0, "blackbird").unwrap();
    doc1.insert(&list_id, 1, "thrush").unwrap();
    doc1.insert(&list_id, 2, "goldfinch").unwrap();
    doc2.merge(&mut doc1);
    doc1.set(&list_id, 1, "starling").unwrap();
    doc2.del(&list_id, 1).unwrap();

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

    doc1.merge(&mut doc2);

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
        .set(&automerge::ROOT, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();

    doc1.insert(&list_id, 0, "blackbird").unwrap();
    doc1.insert(&list_id, 1, "thrush").unwrap();
    doc1.insert(&list_id, 2, "goldfinch").unwrap();

    doc2.merge(&mut doc1);

    doc1.splice(&list_id, 1, 2, Vec::new()).unwrap();

    doc2.splice(&list_id, 2, 0, vec!["starling".into()])
        .unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list![
                { "blackbird" },
                { "starling" }
            ]}
        }
    );

    doc2.merge(&mut doc1);
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
        .set(&automerge::ROOT, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();

    doc1.insert(&list_id, 0, "albatross").unwrap();
    doc1.insert(&list_id, 1, "buzzard").unwrap();
    doc1.insert(&list_id, 2, "cormorant").unwrap();

    doc2.merge(&mut doc1);

    doc1.del(&list_id, 1).unwrap();

    doc2.del(&list_id, 1).unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list![
                { "albatross" },
                { "cormorant" }
            ]}
        }
    );

    doc2.merge(&mut doc1);
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
        .set(&automerge::ROOT, "animals", automerge::Value::map())
        .unwrap()
        .unwrap();
    let birds = doc1
        .set(&animals, "birds", automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.set(&birds, "pink", "flamingo").unwrap();
    doc1.set(&birds, "black", "starling").unwrap();

    let mammals = doc1
        .set(&animals, "mammals", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(&mammals, 0, "badger").unwrap();

    doc2.merge(&mut doc1);

    doc1.set(&birds, "brown", "sparrow").unwrap();

    doc2.del(&animals, "birds").unwrap();
    doc1.merge(&mut doc2);

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
        &doc2,
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
        .set(&automerge::ROOT, "birds", automerge::Value::map())
        .unwrap()
        .unwrap();
    let blackbird = doc1
        .set(&birds, "blackbird", automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.set(&blackbird, "feathers", "black").unwrap();

    doc2.merge(&mut doc1);

    doc1.del(&birds, "blackbird").unwrap();

    doc2.set(&blackbird, "beak", "orange").unwrap();

    doc1.merge(&mut doc2);

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
        .set(&automerge::ROOT, "wisdom", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc2.merge(&mut doc1);

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

    doc1.merge(&mut doc2);

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
        .set(&automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(&list, 0, "two").unwrap();
    doc2.merge(&mut doc1);

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
        .set(&automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(&list, 0, "two").unwrap();
    doc2.merge(&mut doc1);

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
        .set(&automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(&list, 0, "four").unwrap();
    doc2.merge(&mut doc1);
    doc2.insert(&list, 0, "three").unwrap();
    doc1.merge(&mut doc2);
    doc1.insert(&list, 0, "two").unwrap();
    doc2.merge(&mut doc1);
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
    let loaded = Automerge::load(&doc.save().unwrap()).unwrap();

    assert_doc!(&loaded, map! {});
}

#[test]
fn save_restore_complex() {
    let mut doc1 = new_doc();
    let todos = doc1
        .set(&automerge::ROOT, "todos", automerge::Value::list())
        .unwrap()
        .unwrap();

    let first_todo = doc1
        .insert(&todos, 0, automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.set(&first_todo, "title", "water plants").unwrap();
    doc1.set(&first_todo, "done", false).unwrap();

    let mut doc2 = new_doc();
    doc2.merge(&mut doc1);
    doc2.set(&first_todo, "title", "weed plants").unwrap();

    doc1.set(&first_todo, "title", "kill plants").unwrap();
    doc1.merge(&mut doc2);

    let reloaded = Automerge::load(&doc1.save().unwrap()).unwrap();

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
    println!("{:?}", v);
    let actor1 = v[0].clone();
    let actor2 = v[1].clone();
    let actor3 = v[2].clone();

    let mut doc1 = new_doc_with_actor(actor1);

    let list = doc1.set(&ROOT, "list", Value::list())?.unwrap();
    doc1.insert(&list, 0, "a")?;
    doc1.insert(&list, 1, "b")?;
    doc1.insert(&list, 2, "c")?;

    let mut doc2 = Automerge::load(&doc1.save()?)?;
    doc2.set_actor(actor2);

    let mut doc3 = Automerge::load(&doc1.save()?)?;
    doc3.set_actor(actor3);

    doc1.set(&list, 1, Value::counter(0))?;
    doc2.set(&list, 1, Value::counter(10))?;
    doc3.set(&list, 1, Value::counter(100))?;

    doc1.set(&list, 2, Value::counter(0))?;
    doc2.set(&list, 2, Value::counter(10))?;
    doc3.set(&list, 2, Value::int(100))?;

    doc1.inc(&list, 1, 1)?;
    doc1.inc(&list, 2, 1)?;

    doc1.merge(&mut doc2);
    doc1.merge(&mut doc3);

    let values = doc1.values(&list, 1)?;
    assert_eq!(values.len(), 3);
    assert_eq!(&values[0].0, &Value::counter(1));
    assert_eq!(&values[1].0, &Value::counter(10));
    assert_eq!(&values[2].0, &Value::counter(100));

    let values = doc1.values(&list, 2)?;
    assert_eq!(values.len(), 3);
    assert_eq!(&values[0].0, &Value::counter(1));
    assert_eq!(&values[1].0, &Value::counter(10));
    assert_eq!(&values[2].0, &Value::int(100));

    doc1.inc(&list, 1, 1)?;
    doc1.inc(&list, 2, 1)?;

    let values = doc1.values(&list, 1)?;
    assert_eq!(values.len(), 3);
    assert_eq!(&values[0].0, &Value::counter(2));
    assert_eq!(&values[1].0, &Value::counter(11));
    assert_eq!(&values[2].0, &Value::counter(101));

    let values = doc1.values(&list, 2)?;
    assert_eq!(values.len(), 2);
    assert_eq!(&values[0].0, &Value::counter(2));
    assert_eq!(&values[1].0, &Value::counter(11));

    assert_eq!(doc1.length(&list), 3);

    doc1.del(&list, 2)?;

    assert_eq!(doc1.length(&list), 2);

    let doc4 = Automerge::load(&doc1.save()?)?;

    assert_eq!(doc4.length(&list), 2);

    doc1.del(&list, 1)?;

    assert_eq!(doc1.length(&list), 1);

    let doc5 = Automerge::load(&doc1.save()?)?;

    assert_eq!(doc5.length(&list), 1);

    Ok(())
}
