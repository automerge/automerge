use automerge::{Automerge, ObjId};

mod helpers;
#[allow(unused_imports)]
use helpers::{
    mk_counter, new_doc, new_doc_with_actor, pretty_print, realize, realize_obj, sorted_actors,
    RealizedObject,
};
#[test]
fn no_conflict_on_repeated_assignment() {
    let mut doc = Automerge::new();
    doc.set(ObjId::Root, "foo", 1).unwrap();
    let op = doc.set(ObjId::Root, "foo", 2).unwrap().unwrap();
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
    doc.set(ObjId::Root, "foo", 1).unwrap();
    assert!(doc.set(ObjId::Root, "foo", 1).unwrap().is_none());
}

#[test]
fn no_change_on_repeated_list_set() {
    let mut doc = new_doc();
    let list_id = doc
        .set(ObjId::Root, "list", automerge::Value::list())
        .unwrap()
        .unwrap().into();
    doc.insert(&list_id, 0, 1).unwrap();
    doc.set(&list_id, 0, 1).unwrap();
    assert!(doc.set(list_id, 0, 1).unwrap().is_none());
}

#[test]
fn no_change_on_list_insert_followed_by_set_of_same_value() {
    let mut doc = new_doc();
    let list_id = doc
        .set(ObjId::Root, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc.insert(&list_id, 0, 1).unwrap();
    assert!(doc.set(&list_id, 0, 1).unwrap().is_none());
}

#[test]
fn repeated_map_assignment_which_resolves_conflict_not_ignored() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    doc1.set(ObjId::Root, "field", 123).unwrap();
    doc2.merge(&mut doc1);
    doc2.set(ObjId::Root, "field", 456).unwrap();
    doc1.set(ObjId::Root, "field", 789).unwrap();
    doc1.merge(&mut doc2);
    assert_eq!(doc1.values(ObjId::Root, "field").unwrap().len(), 2);

    let op = doc1.set(ObjId::Root, "field", 123).unwrap().unwrap();
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
        .set(ObjId::Root, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(&list_id, 0, 123).unwrap();
    doc2.merge(&mut doc1);
    doc2.set(&list_id, 0, 456).unwrap().unwrap();
    doc1.merge(&mut doc2);
    let doc1_op = doc1.set(&list_id, 0, 789).unwrap().unwrap();

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
        .set(ObjId::Root, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    let op1 = doc.insert(&list_id, 0, 123).unwrap();
    doc.insert(&list_id, 1, 456).unwrap();
    let op3 = doc.insert(&list_id.clone(), 2, 789).unwrap();
    doc.del(&list_id, 1).unwrap();
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
    let op1 = doc1.set(ObjId::Root, "foo", "bar").unwrap().unwrap();
    let hello = doc2
        .set(ObjId::Root, "hello", "world")
        .unwrap()
        .unwrap();
    doc1.merge(&mut doc2);
    assert_eq!(
        doc1.value(ObjId::Root, "foo").unwrap().unwrap().0,
        "bar".into()
    );
    assert_doc!(
        &doc1,
        map! {
            "foo" => { op1 => "bar" },
            "hello" => { hello => "world" },
        }
    );
    doc2.merge(&mut doc1);
    assert_doc!(
        &doc2,
        map! {
            "foo" => { op1 => "bar" },
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
        .set(ObjId::Root, "counter", mk_counter(0))
        .unwrap()
        .unwrap();
    doc2.merge(&mut doc1);
    doc1.inc(ObjId::Root, "counter", 1).unwrap();
    doc2.inc(ObjId::Root, "counter", 2).unwrap();
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
        .set(ObjId::Root, "counter", mk_counter(0))
        .unwrap()
        .unwrap();
    doc1.inc(ObjId::Root, "counter", 1).unwrap();

    // create a counter in doc2
    let doc2_counter_id = doc2
        .set(ObjId::Root, "counter", mk_counter(0))
        .unwrap()
        .unwrap();
    doc2.inc(ObjId::Root, "counter", 3).unwrap();

    // The two values should be conflicting rather than added
    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "counter" => {
                doc1_counter_id => mk_counter(1),
                doc2_counter_id => mk_counter(3),
            }
        }
    );
}

#[test]
fn concurrent_updates_of_same_field() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let set_one_opid = doc1.set(ObjId::Root, "field", "one").unwrap().unwrap();
    let set_two_opid = doc2.set(ObjId::Root, "field", "two").unwrap().unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "field" => {
                set_one_opid => "one",
                set_two_opid => "two",
            }
        }
    );
}

#[test]
fn concurrent_updates_of_same_list_element() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .set(ObjId::Root, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(list_id.clone(), 0, "finch").unwrap();
    doc2.merge(&mut doc1);
    let set_one_op = doc1.set(&list_id, 0, "greenfinch").unwrap().unwrap();
    let set_op_two = doc2.set(&list_id, 0, "goldfinch").unwrap().unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {
                list_id => list![{
                    set_one_op => "greenfinch",
                    set_op_two => "goldfinch",
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
        .set(ObjId::Root, "field", "string")
        .unwrap()
        .unwrap();
    let op_two = doc2
        .set(ObjId::Root, "field", automerge::Value::list())
        .unwrap()
        .unwrap();
    let op_three = doc3
        .set(ObjId::Root, "field", automerge::Value::map())
        .unwrap()
        .unwrap();

    doc1.merge(&mut doc2);
    doc1.merge(&mut doc3);

    assert_doc!(
        &doc1,
        map! {
            "field" => {
                op_one => "string",
                op_two => list!{},
                op_three => map!{},
            }
        }
    );
}

#[test]
fn changes_within_conflicting_map_field() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let op_one = doc1
        .set(ObjId::Root, "field", "string")
        .unwrap()
        .unwrap();
    let map_id = doc2
        .set(ObjId::Root, "field", automerge::Value::map())
        .unwrap()
        .unwrap();
    let set_in_doc2 = doc2.set(&map_id, "innerKey", 42).unwrap().unwrap();
    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "field" => {
                op_one => "string",
                map_id => map!{
                    "innerKey" => {
                        set_in_doc2 => 42,
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
        .set(ObjId::Root, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc1.insert(&list_id, 0, "hello").unwrap();
    doc2.merge(&mut doc1);

    let map_in_doc1 = doc1
        .set(&list_id, 0, automerge::Value::map())
        .unwrap()
        .unwrap();
    let set_map1 = doc1.set(&map_in_doc1, "map1", true).unwrap().unwrap();
    let set_key1 = doc1.set(&map_in_doc1, "key", 1).unwrap().unwrap();

    let map_in_doc2 = doc2
        .set(&list_id, 0, automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.merge(&mut doc2);
    let set_map2 = doc2.set(&map_in_doc2, "map2", true).unwrap().unwrap();
    let set_key2 = doc2.set(&map_in_doc2, "key", 2).unwrap().unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "list" => {
                list_id => list![
                    {
                        map_in_doc2 => map!{
                            "map2" => { set_map2 => true },
                            "key" => { set_key2 => 2 },
                        },
                        map_in_doc1 => map!{
                            "key" => { set_key1 => 1 },
                            "map1" => { set_map1 => true },
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
        .set(ObjId::Root, "config", automerge::Value::map())
        .unwrap()
        .unwrap();
    let doc1_field = doc1
        .set(doc1_map_id.clone(), "background", "blue")
        .unwrap()
        .unwrap();

    let doc2_map_id = doc2
        .set(ObjId::Root, "config", automerge::Value::map())
        .unwrap()
        .unwrap();
    let doc2_field = doc2
        .set(doc2_map_id.clone(), "logo_url", "logo.png")
        .unwrap()
        .unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "config" => {
                doc1_map_id => map!{
                    "background" => {doc1_field => "blue"}
                },
                doc2_map_id => map!{
                    "logo_url" => {doc2_field => "logo.png"}
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
        .set(ObjId::Root, "list", automerge::Value::list())
        .unwrap()
        .unwrap();

    let one = doc1.insert(&list_id, 0, "one").unwrap();
    let three = doc1.insert(&list_id, 1, "three").unwrap();
    doc2.merge(&mut doc1);
    let two = doc1.splice(&list_id, 1, 0, vec!["two".into()]).unwrap()[0].clone();
    let four = doc2.insert(&list_id, 2, "four").unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "list" => {
                list_id => list![
                    {one => "one"},
                    {two => "two"},
                    {three => "three"},
                    {four => "four"},
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
        .set(ObjId::Root, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();
    let parakeet = doc1.insert(&list_id, 0, "parakeet").unwrap();

    doc2.merge(&mut doc1);
    let starling = doc1.insert(&list_id, 1, "starling").unwrap();
    let chaffinch = doc2.insert(&list_id, 1, "chaffinch").unwrap();
    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {
                list_id => list![
                    {
                        parakeet => "parakeet",
                    },
                    {
                        starling => "starling",
                    },
                    {
                        chaffinch => "chaffinch",
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
    doc1.set(ObjId::Root, "bestBird", "robin").unwrap();
    doc2.merge(&mut doc1);
    doc1.del(ObjId::Root, "bestBird").unwrap();
    let set_two = doc2
        .set(ObjId::Root, "bestBird", "magpie")
        .unwrap()
        .unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "bestBird" => {
                set_two => "magpie",
            }
        }
    );
}

#[test]
fn concurrent_assignment_and_deletion_of_list_entry() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .set(ObjId::Root, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();
    let blackbird = doc1.insert(&list_id, 0, "blackbird").unwrap();
    doc1.insert(&list_id, 1, "thrush").unwrap();
    let goldfinch = doc1.insert(&list_id, 2, "goldfinch").unwrap();
    doc2.merge(&mut doc1);

    let starling = doc1.set(&list_id, 1, "starling").unwrap().unwrap();

    doc2.del(&list_id, 1).unwrap();

    assert_doc!(
        &doc2,
        map! {
            "birds" => {list_id => list![
                { blackbird => "blackbird"},
                { goldfinch => "goldfinch"},
            ]}
        }
    );

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list_id.clone() => list![
                { blackbird => "blackbird" },
                { starling.clone() => "starling" },
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
        .set(ObjId::Root, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();

    let blackbird = doc1.insert(list_id.clone(), 0, "blackbird").unwrap();
    doc1.insert(&list_id, 1, "thrush").unwrap();
    doc1.insert(&list_id, 2, "goldfinch").unwrap();

    doc2.merge(&mut doc1);

    doc1.splice(&list_id, 1, 2, Vec::new()).unwrap();

    let starling = doc2
        .splice(&list_id, 2, 0, vec!["starling".into()])
        .unwrap()[0].clone();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list_id => list![
                { blackbird => "blackbird" },
                { starling => "starling" }
            ]}
        }
    );

    doc2.merge(&mut doc1);
    assert_doc!(
        &doc2,
        map! {
            "birds" => {list_id => list![
                { blackbird => "blackbird" },
                { starling => "starling" }
            ]}
        }
    );
}

#[test]
fn concurrent_deletion_of_same_list_element() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();
    let list_id = doc1
        .set(ObjId::Root, "birds", automerge::Value::list())
        .unwrap()
        .unwrap();

    let albatross = doc1.insert(list_id.clone(), 0, "albatross").unwrap();
    doc1.insert(&list_id, 1, "buzzard").unwrap();
    let cormorant = doc1.insert(&list_id, 2, "cormorant").unwrap();

    doc2.merge(&mut doc1);

    doc1.del(&list_id, 1).unwrap();

    doc2.del(&list_id, 1).unwrap();

    doc1.merge(&mut doc2);

    assert_doc!(
        &doc1,
        map! {
            "birds" => {list_id.clone() => list![
                { albatross.clone() => "albatross" },
                { cormorant.clone()  => "cormorant" }
            ]}
        }
    );

    doc2.merge(&mut doc1);
    assert_doc!(
        &doc2,
        map! {
            "birds" => {list_id => list![
                { albatross => "albatross" },
                { cormorant => "cormorant" }
            ]}
        }
    );
}

#[test]
fn concurrent_updates_at_different_levels() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    let animals = doc1
        .set(ObjId::Root, "animals", automerge::Value::map())
        .unwrap()
        .unwrap();
    let birds = doc1
        .set(&animals, "birds", automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.set(&birds, "pink", "flamingo").unwrap().unwrap();
    doc1.set(&birds, "black", "starling").unwrap().unwrap();

    let mammals = doc1
        .set(&animals, "mammals", automerge::Value::list())
        .unwrap()
        .unwrap();
    let badger = doc1.insert(&mammals, 0, "badger").unwrap();

    doc2.merge(&mut doc1);

    doc1.set(&birds, "brown", "sparrow").unwrap().unwrap();

    doc2.del(&animals, "birds").unwrap();
    doc1.merge(&mut doc2);

    assert_obj!(
        &doc1,
        ObjId::Root,
        "animals",
        map! {
            "mammals" => {
                mammals => list![{ badger => "badger" }],
            }
        }
    );

    assert_obj!(
        &doc2,
        ObjId::Root,
        "animals",
        map! {
            "mammals" => {
                mammals => list![{ badger => "badger" }],
            }
        }
    );
}

#[test]
fn concurrent_updates_of_concurrently_deleted_objects() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    let birds = doc1
        .set(ObjId::Root, "birds", automerge::Value::map())
        .unwrap()
        .unwrap();
    let blackbird = doc1
        .set(&birds, "blackbird", automerge::Value::map())
        .unwrap()
        .unwrap();
    doc1.set(&blackbird, "feathers", "black").unwrap().unwrap();

    doc2.merge(&mut doc1);

    doc1.del(&birds, "blackbird").unwrap();

    doc2.set(&blackbird, "beak", "orange").unwrap();

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
        .set(ObjId::Root, "wisdom", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc2.merge(&mut doc1);

    let doc1elems = doc1
        .splice(
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

    let doc2elems = doc2
        .splice(
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
            "wisdom" => {wisdom => list![
                {doc1elems[0] => "to"},
                {doc1elems[1] => "be"},
                {doc1elems[2] => "is"},
                {doc1elems[3] => "to"},
                {doc1elems[4] => "do"},
                {doc2elems[0] => "to"},
                {doc2elems[1] => "do"},
                {doc2elems[2] => "is"},
                {doc2elems[3] => "to"},
                {doc2elems[4] => "be"},
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
        .set(ObjId::Root, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    let two = doc1.insert(&list, 0, "two").unwrap();
    doc2.merge(&mut doc1);

    let one = doc2.insert(&list, 0, "one").unwrap();
    assert_doc!(
        &doc2,
        map! {
            "list" => { list => list![
                { one => "one" },
                { two => "two" },
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
        .set(ObjId::Root, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    let two = doc1.insert(&list, 0, "two").unwrap();
    doc2.merge(&mut doc1);

    let one = doc2.insert(&list, 0, "one").unwrap();
    assert_doc!(
        &doc2,
        map! {
            "list" => { list => list![
                { one => "one" },
                { two => "two" },
            ]}
        }
    );
}

#[test]
fn insertion_consistent_with_causality() {
    let mut doc1 = new_doc();
    let mut doc2 = new_doc();

    let list = doc1
        .set(ObjId::Root, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    let four = doc1.insert(&list, 0, "four").unwrap();
    doc2.merge(&mut doc1);
    let three = doc2.insert(&list, 0, "three").unwrap();
    doc1.merge(&mut doc2);
    let two = doc1.insert(&list, 0, "two").unwrap();
    doc2.merge(&mut doc1);
    let one = doc2.insert(&list, 0, "one").unwrap();

    assert_doc!(
        &doc2,
        map! {
            "list" => {list => list![
                {one => "one"},
                {two => "two"},
                {three => "three" },
                {four => "four"},
            ]}
        }
    );
}

#[test]
fn should_handle_arbitrary_depth_nesting() {
    let mut doc1 = new_doc();
    let a = doc1.set(ObjId::Root, "a", automerge::Value::map()).unwrap().unwrap(); 
    let b = doc1.set(&a, "b", automerge::Value::map()).unwrap().unwrap();
    let c = doc1.set(&b, "c", automerge::Value::map()).unwrap().unwrap();
    let d = doc1.set(&c, "d", automerge::Value::map()).unwrap().unwrap();
    let e = doc1.set(&d, "e", automerge::Value::map()).unwrap().unwrap();
    let f = doc1.set(&e, "f", automerge::Value::map()).unwrap().unwrap();
    let g = doc1.set(&f, "g", automerge::Value::map()).unwrap().unwrap();
    let h = doc1.set(&g, "h", "h").unwrap().unwrap();
    let j = doc1.set(&f, "i", "j").unwrap().unwrap();

    assert_doc!(
        &doc1,
        map!{
            "a" => {a => map!{
                "b" => {b => map!{
                    "c" => {c => map!{
                        "d" => {d => map!{
                            "e" => {e => map!{
                                "f" => {f => map!{
                                    "g" => {g => map!{
                                        "h" => {h => "h"}
                                    }},
                                    "i" => {j => "j"},
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }
    );

    Automerge::load(&doc1.save().unwrap()).unwrap();
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
        .set(ObjId::Root, "todos", automerge::Value::list())
        .unwrap()
        .unwrap();

    let first_todo = doc1.insert(todos.clone(), 0, automerge::Value::map()).unwrap();
    doc1.set(&first_todo, "title", "water plants")
        .unwrap()
        .unwrap();
    let first_done = doc1.set(first_todo.clone(), "done", false).unwrap().unwrap();

    let mut doc2 = new_doc();
    doc2.merge(&mut doc1);
    let weed_title = doc2
        .set(first_todo.clone(), "title", "weed plants")
        .unwrap()
        .unwrap();

    let kill_title = doc1
        .set(&first_todo, "title", "kill plants")
        .unwrap()
        .unwrap();
    doc1.merge(&mut doc2);

    let reloaded = Automerge::load(&doc1.save().unwrap()).unwrap();

    assert_doc!(
        &reloaded,
        map! {
            "todos" => {todos => list![
                {first_todo => map!{
                    "title" => {
                        weed_title => "weed plants",
                        kill_title => "kill plants",
                    },
                    "done" => {first_done => false},
                }}
            ]}
        }
    );
}
