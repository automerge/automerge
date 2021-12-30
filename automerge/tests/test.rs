use automerge::Automerge;

mod helpers;
#[allow(unused_imports)]
use helpers::{
    mk_counter, new_doc, new_doc_with_actor, pretty_print, realize, realize_obj, sorted_actors,
    translate_obj_id, OpIdExt, RealizedObject,
};
#[test]
fn no_conflict_on_repeated_assignment() {
    let mut doc = Automerge::new();
    doc.set(automerge::ROOT, "foo", 1).unwrap();
    let op = doc.set(automerge::ROOT, "foo", 2).unwrap().unwrap();
    assert_doc!(
        &doc,
        map! {
            "foo" => { op => 2},
        }
    );
}

#[test]
fn no_change_on_repeated_map_set() {
    let mut doc = new_doc();
    doc.set(automerge::ROOT, "foo", 1).unwrap();
    assert!(doc.set(automerge::ROOT, "foo", 1).unwrap().is_none());
}

#[test]
fn no_change_on_repeated_list_set() {
    let mut doc = new_doc();
    let list_id = doc
        .set(automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc.insert(list_id, 0, 1).unwrap();
    doc.set(list_id, 0, 1).unwrap();
    assert!(doc.set(list_id, 0, 1).unwrap().is_none());
}

#[test]
fn no_change_on_list_insert_followed_by_set_of_same_value() {
    let mut doc = new_doc();
    let list_id = doc
        .set(automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc.insert(list_id, 0, 1).unwrap();
    assert!(doc.set(list_id, 0, 1).unwrap().is_none());
}

#[test]
fn repeated_map_assignment_which_resolves_conflict_not_ignored() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    doc1.set(automerge::ROOT, "field", 123).unwrap();
    doc2.merge(&mut doc1);
    doc2.set(automerge::ROOT, "field", 456).unwrap();
    doc1.set(automerge::ROOT, "field", 789).unwrap();
    doc1.merge(&mut doc2);
    assert_eq!(doc1.values(automerge::ROOT, "field").unwrap().len(), 2);

    let op = doc1.set(automerge::ROOT, "field", 123).unwrap().unwrap();
    assert_doc!(
        &doc1,
        map! {
            "field" => {
                op => 123
            }
        }
    );
}

#[test]
fn repeated_list_assignment_which_resolves_conflict_not_ignored() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .set(automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(list_id, 0, 123).unwrap();
    doc2.merge(&mut doc1);
    let list_id_in_doc2 = translate_obj_id(&doc1, &doc2, list_id);
    doc2.set(list_id_in_doc2, 0, 456).unwrap().unwrap();
    doc1.merge(&mut doc2);
    let doc1_op = doc1.set(list_id, 0, 789).unwrap().unwrap();

    assert_doc!(
        &doc1,
        map! {
            "list" => {
                list_id => list![
                    { doc1_op => 789 },
                ]
            }
        }
    );
}

#[test]
fn list_deletion() {
    let mut doc = new_doc();
    let list_id = doc
        .set(automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    let op1 = doc.insert(list_id, 0, 123).unwrap();
    doc.insert(list_id, 1, 456).unwrap();
    let op3 = doc.insert(list_id, 2, 789).unwrap();
    doc.del(list_id, 1).unwrap();
    assert_doc!(
        &doc,
        map! {
            "list" => {list_id => list![
                { op1 => 123 },
                { op3 => 789 },
            ]}
        }
    )
}

#[test]
fn merge_concurrent_map_prop_updates() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let op1 = doc1.set(automerge::ROOT, "foo", "bar").unwrap().unwrap();
    let hello = doc2
        .set(automerge::ROOT, "hello", "world")
        .unwrap()
        .unwrap();
    doc1.merge(&mut doc2);
    assert_eq!(
        doc1.value(automerge::ROOT, "foo").unwrap().unwrap().0,
        "bar".into()
    );
    assert_doc!(
        &doc1,
        map! {
            "foo" => { op1 => "bar" },
            "hello" => { hello.translate(&doc2) => "world" },
        }
    );
    doc2.merge(&mut doc1);
    assert_doc!(
        &doc2,
        map! {
            "foo" => { op1.translate(&doc1) => "bar" },
            "hello" => { hello => "world" },
        }
    );
    assert_eq!(realize(&doc1), realize(&doc2));
}

#[test]
fn add_concurrent_increments_of_same_property() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let counter_id = doc1
        .set(automerge::ROOT, "counter", mk_counter(0))
        .unwrap()
        .unwrap();
    doc2.merge(&mut doc1);
    doc1.inc(automerge::ROOT, "counter", 1).unwrap();
    doc2.inc(automerge::ROOT, "counter", 2).unwrap();
    doc1.merge(&mut doc2);
    assert_doc!(
        &doc1,
        map! {
            "counter" => {
                counter_id => mk_counter(3)
            }
        }
    );
}

#[test]
fn add_increments_only_to_preceeded_values() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    // create a counter in doc1
    let doc1_counter_id = doc1
        .set(automerge::ROOT, "counter", mk_counter(0))
        .unwrap()
        .unwrap();
    doc1.inc(automerge::ROOT, "counter", 1).unwrap();

    // create a counter in doc2
    let doc2_counter_id = doc2
        .set(automerge::ROOT, "counter", mk_counter(0))
        .unwrap()
        .unwrap();
    doc2.inc(automerge::ROOT, "counter", 3).unwrap();

    // The two values should be conflicting rather than added
    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "counter" => {
                doc1_counter_id.native() => mk_counter(1),
                doc2_counter_id.translate(&doc2) => mk_counter(3),
            }
        }
    );
}

#[test]
fn concurrent_updates_of_same_field() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let set_one_opid = doc1.set(automerge::ROOT, "field", "one").unwrap().unwrap();
    let set_two_opid = doc2.set(automerge::ROOT, "field", "two").unwrap().unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "field" => {
                set_one_opid.native() => "one",
                set_two_opid.translate(&doc2) => "two",
            }
        }
    );
}

#[test]
fn concurrent_updates_of_same_list_element() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .set(automerge::ROOT, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(list_id, 0, "finch").unwrap();
    doc2.merge(&mut doc1);
    let set_one_op = doc1.set(list_id, 0, "greenfinch").unwrap().unwrap();
    let list_id_in_doc2 = translate_obj_id(&doc1, &doc2, list_id);
    let set_op_two = doc2.set(list_id_in_doc2, 0, "goldfinch").unwrap().unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {
                list_id => list![{
                    set_one_op.native() => "greenfinch",
                    set_op_two.translate(&doc2) => "goldfinch",
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
    let op_one = doc1
        .set(automerge::ROOT, "field", "string")
        .unwrap()
        .unwrap();
    let op_two = doc2
        .set(automerge::ROOT, "field", automerge::Value::list())
        .unwrap()
        .unwrap();
    let op_three = doc3
        .set(automerge::ROOT, "field", automerge::Value::map())
        .unwrap()
        .unwrap();

    doc1.merge(&mut doc2);
    doc1.merge(&mut doc3);

    assert_doc!(
        &doc1,
        map! {
            "field" => {
                op_one.native() => "string",
                op_two.translate(&doc2) => list!{},
                op_three.translate(&doc3) => map!{},
            }
        }
    );
}

#[test]
fn changes_within_conflicting_map_field() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let op_one = doc1
        .set(automerge::ROOT, "field", "string")
        .unwrap()
        .unwrap();
    let map_id = doc2
        .set(automerge::ROOT, "field", automerge::Value::map())
        .unwrap()
        .unwrap();
    let set_in_doc2 = doc2.set(map_id, "innerKey", 42).unwrap().unwrap();
    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "field" => {
                op_one.native() => "string",
                map_id.translate(&doc2) => map!{
                    "innerKey" => {
                        set_in_doc2.translate(&doc2) => 42,
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
        .set(automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(list_id, 0, "hello").unwrap();
    doc2.merge(&mut doc1);

    let map_in_doc1 = doc1
        .set(list_id, 0, automerge::Value::map())
        .unwrap()
        .unwrap();
    let set_map1 = doc1.set(map_in_doc1, "map1", true).unwrap().unwrap();
    let set_key1 = doc1.set(map_in_doc1, "key", 1).unwrap().unwrap();

    let list_id_in_doc2 = translate_obj_id(&doc1, &doc2, list_id);
    let map_in_doc2 = doc2
        .set(list_id_in_doc2, 0, automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.merge(&mut doc2);
    let set_map2 = doc2.set(map_in_doc2, "map2", true).unwrap().unwrap();
    let set_key2 = doc2.set(map_in_doc2, "key", 2).unwrap().unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "list" => {
                list_id => list![
                    {
                        map_in_doc2.translate(&doc2) => map!{
                            "map2" => { set_map2.translate(&doc2) => true },
                            "key" => { set_key2.translate(&doc2) => 2 },
                        },
                        map_in_doc1.native() => map!{
                            "key" => { set_key1.native() => 1 },
                            "map1" => { set_map1.native() => true },
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
        .set(automerge::ROOT, "config", automerge::Value::map())
        .unwrap()
        .unwrap();
    let doc1_field = doc1
        .set(doc1_map_id, "background", "blue")
        .unwrap()
        .unwrap();

    let doc2_map_id = doc2
        .set(automerge::ROOT, "config", automerge::Value::map())
        .unwrap()
        .unwrap();
    let doc2_field = doc2
        .set(doc2_map_id, "logo_url", "logo.png")
        .unwrap()
        .unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "config" => {
                doc1_map_id.native() => map!{
                    "background" => {doc1_field.native() => "blue"}
                },
                doc2_map_id.translate(&doc2) => map!{
                    "logo_url" => {doc2_field.translate(&doc2) => "logo.png"}
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
        .set(automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();

    let one = doc1.insert(list_id, 0, "one").unwrap();
    let three = doc1.insert(list_id, 1, "three").unwrap();
    doc2.merge(&mut doc1);
    let two = doc1.splice(list_id, 1, 0, vec!["two".into()]).unwrap()[0];
    let list_id_in_doc2 = translate_obj_id(&doc1, &doc2, list_id);
    let four = doc2.insert(list_id_in_doc2, 2, "four").unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "list" => {
                list_id => list![
                    {one.native() => "one"},
                    {two.native() => "two"},
                    {three.native() => "three"},
                    {four.translate(&doc2) => "four"},
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
        .set(automerge::ROOT, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();
    let parakeet = doc1.insert(list_id, 0, "parakeet").unwrap();

    doc2.merge(&mut doc1);
    let list_id_in_doc2 = translate_obj_id(&doc1, &doc2, list_id);
    let starling = doc1.insert(list_id, 1, "starling").unwrap();
    let chaffinch = doc2.insert(list_id_in_doc2, 1, "chaffinch").unwrap();
    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {
                list_id => list![
                    {
                        parakeet.native() => "parakeet",
                    },
                    {
                        starling.native() => "starling",
                    },
                    {
                        chaffinch.translate(&doc2) => "chaffinch",
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
    doc1.set(automerge::ROOT, "bestBird", "robin").unwrap();
    doc2.merge(&mut doc1);
    doc1.del(automerge::ROOT, "bestBird").unwrap();
    let set_two = doc2
        .set(automerge::ROOT, "bestBird", "magpie")
        .unwrap()
        .unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "bestBird" => {
                set_two.translate(&doc2) => "magpie",
            }
        }
    );
}

#[test]
fn concurrent_assignment_and_deletion_of_list_entry() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .set(automerge::ROOT, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();
    let blackbird = doc1.insert(list_id, 0, "blackbird").unwrap();
    doc1.insert(list_id, 1, "thrush").unwrap();
    let goldfinch = doc1.insert(list_id, 2, "goldfinch").unwrap();
    doc2.merge(&mut doc1);

    let starling = doc1.set(list_id, 1, "starling").unwrap().unwrap();

    let list_id_in_doc2 = translate_obj_id(&doc1, &doc2, list_id);
    doc2.del(list_id_in_doc2, 1).unwrap();

    assert_doc!(
        &doc2,
        map! {
            "birds" => {list_id.translate(&doc1) => list![
                { blackbird.translate(&doc1) => "blackbird"},
                { goldfinch.translate(&doc1) => "goldfinch"},
            ]}
        }
    );

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list_id => list![
                { blackbird => "blackbird" },
                { starling => "starling" },
                { goldfinch => "goldfinch" },
            ]}
        }
    );

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list_id => list![
                { blackbird => "blackbird" },
                { starling => "starling" },
                { goldfinch => "goldfinch" },
            ]}
        }
    );
}

#[test]
fn insertion_after_a_deleted_list_element() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .set(automerge::ROOT, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();

    let blackbird = doc1.insert(list_id, 0, "blackbird").unwrap();
    doc1.insert(list_id, 1, "thrush").unwrap();
    doc1.insert(list_id, 2, "goldfinch").unwrap();

    doc2.merge(&mut doc1);

    doc1.splice(list_id, 1, 2, Vec::new()).unwrap();

    let list_id_in_doc2 = translate_obj_id(&doc1, &doc2, list_id);
    let starling = doc2
        .splice(list_id_in_doc2, 2, 0, vec!["starling".into()])
        .unwrap()[0];

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list_id => list![
                { blackbird.native() => "blackbird" },
                { starling.translate(&doc2) => "starling" }
            ]}
        }
    );

    doc2.merge(&mut doc1);
    assert_doc!(
        &doc2,
        map! {
            "birds" => {list_id.translate(&doc1) => list![
                { blackbird.translate(&doc1) => "blackbird" },
                { starling.native() => "starling" }
            ]}
        }
    );
}

#[test]
fn concurrent_deletion_of_same_list_element() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .set(automerge::ROOT, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();

    let albatross = doc1.insert(list_id, 0, "albatross").unwrap();
    doc1.insert(list_id, 1, "buzzard").unwrap();
    let cormorant = doc1.insert(list_id, 2, "cormorant").unwrap();

    doc2.merge(&mut doc1);

    doc1.del(list_id, 1).unwrap();

    let list_id_in_doc2 = translate_obj_id(&doc1, &doc2, list_id);
    doc2.del(list_id_in_doc2, 1).unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list_id => list![
                { albatross => "albatross" },
                { cormorant  => "cormorant" }
            ]}
        }
    );

    doc2.merge(&mut doc1);
    assert_doc!(
        &doc2,
        map! {
            "birds" => {list_id.translate(&doc1) => list![
                { albatross.translate(&doc1) => "albatross" },
                { cormorant.translate(&doc1) => "cormorant" }
            ]}
        }
    );
}

#[test]
fn concurrent_updates_at_different_levels() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    let animals = doc1
        .set(automerge::ROOT, "animals", automerge::Value::map())
        .unwrap()
        .unwrap();
    let birds = doc1
        .set(animals, "birds", automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.set(birds, "pink", "flamingo").unwrap().unwrap();
    doc1.set(birds, "black", "starling").unwrap().unwrap();

    let mammals = doc1
        .set(animals, "mammals", automerge::Value::list())
        .unwrap()
        .unwrap();
    let badger = doc1.insert(mammals, 0, "badger").unwrap();

    doc2.merge(&mut doc1);

    doc1.set(birds, "brown", "sparrow").unwrap().unwrap();

    let animals_in_doc2 = translate_obj_id(&doc1, &doc2, animals);
    doc2.del(animals_in_doc2, "birds").unwrap();
    doc1.merge(&mut doc2);

    assert_obj!(
        &doc1,
        automerge::ROOT,
        "animals",
        map! {
            "mammals" => {
                mammals => list![{ badger => "badger" }],
            }
        }
    );

    assert_obj!(
        &doc2,
        automerge::ROOT,
        "animals",
        map! {
            "mammals" => {
                mammals.translate(&doc1) => list![{ badger.translate(&doc1) => "badger" }],
            }
        }
    );
}

#[test]
fn concurrent_updates_of_concurrently_deleted_objects() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    let birds = doc1
        .set(automerge::ROOT, "birds", automerge::Value::map())
        .unwrap()
        .unwrap();
    let blackbird = doc1
        .set(birds, "blackbird", automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.set(blackbird, "feathers", "black").unwrap().unwrap();

    doc2.merge(&mut doc1);

    doc1.del(birds, "blackbird").unwrap();

    translate_obj_id(&doc1, &doc2, blackbird);
    doc2.set(blackbird, "beak", "orange").unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {
                birds => map!{},
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
        .set(automerge::ROOT, "wisdom", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc2.merge(&mut doc1);

    let doc1elems = doc1
        .splice(
            wisdom,
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

    let wisdom_in_doc2 = translate_obj_id(&doc1, &doc2, wisdom);
    let doc2elems = doc2
        .splice(
            wisdom_in_doc2,
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
            "wisdom" => {wisdom => list![
                {doc1elems[0].native() => "to"},
                {doc1elems[1].native() => "be"},
                {doc1elems[2].native() => "is"},
                {doc1elems[3].native() => "to"},
                {doc1elems[4].native() => "do"},
                {doc2elems[0].translate(&doc2) => "to"},
                {doc2elems[1].translate(&doc2) => "do"},
                {doc2elems[2].translate(&doc2) => "is"},
                {doc2elems[3].translate(&doc2) => "to"},
                {doc2elems[4].translate(&doc2) => "be"},
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
        .set(automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    let two = doc1.insert(list, 0, "two").unwrap();
    doc2.merge(&mut doc1);

    let list_in_doc2 = translate_obj_id(&doc1, &doc2, list);
    let one = doc2.insert(list_in_doc2, 0, "one").unwrap();
    assert_doc!(
        &doc2,
        map! {
            "list" => { list.translate(&doc1) => list![
                { one.native() => "one" },
                { two.translate(&doc1) => "two" },
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
        .set(automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    let two = doc1.insert(list, 0, "two").unwrap();
    doc2.merge(&mut doc1);

    let list_in_doc2 = translate_obj_id(&doc1, &doc2, list);
    let one = doc2.insert(list_in_doc2, 0, "one").unwrap();
    assert_doc!(
        &doc2,
        map! {
            "list" => { list.translate(&doc1) => list![
                { one.native() => "one" },
                { two.translate(&doc1) => "two" },
            ]}
        }
    );
}

#[test]
fn insertion_consistent_with_causality() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    let list = doc1
        .set(automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    let four = doc1.insert(list, 0, "four").unwrap();
    doc2.merge(&mut doc1);
    let list_in_doc2 = translate_obj_id(&doc1, &doc2, list);
    let three = doc2.insert(list_in_doc2, 0, "three").unwrap();
    doc1.merge(&mut doc2);
    let two = doc1.insert(list, 0, "two").unwrap();
    doc2.merge(&mut doc1);
    let one = doc2.insert(list_in_doc2, 0, "one").unwrap();

    assert_doc!(
        &doc2,
        map! {
            "list" => {list.translate(&doc1) => list![
                {one.native() => "one"},
                {two.translate(&doc1) => "two"},
                {three.native() => "three" },
                {four.translate(&doc1) => "four"},
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
        .set(automerge::ROOT, "todos", automerge::Value::list())
        .unwrap()
        .unwrap();

    let first_todo = doc1.insert(todos, 0, automerge::Value::map()).unwrap();
    doc1.set(first_todo, "title", "water plants")
        .unwrap()
        .unwrap();
    let first_done = doc1.set(first_todo, "done", false).unwrap().unwrap();

    let mut doc2 = new_doc();
    doc2.merge(&mut doc1);
    let first_todo_in_doc2 = translate_obj_id(&doc1, &doc2, first_todo);
    let weed_title = doc2
        .set(first_todo_in_doc2, "title", "weed plants")
        .unwrap()
        .unwrap();

    let kill_title = doc1
        .set(first_todo, "title", "kill plants")
        .unwrap()
        .unwrap();
    doc1.merge(&mut doc2);

    let reloaded = Automerge::load(&doc1.save().unwrap()).unwrap();

    assert_doc!(
        &reloaded,
        map! {
            "todos" => {todos.translate(&doc1) => list![
                {first_todo.translate(&doc1) => map!{
                    "title" => {
                        weed_title.translate(&doc2) => "weed plants",
                        kill_title.translate(&doc1) => "kill plants",
                    },
                    "done" => {first_done.translate(&doc1) => false},
                }}
            ]}
        }
    );
}
