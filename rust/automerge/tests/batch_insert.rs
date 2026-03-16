use automerge::transaction::Transactable;
use automerge::{AutoCommit, ObjType, PatchAction, ReadDoc, ScalarValue, Value, ROOT};
use std::borrow::Cow;

use automerge::{hydrate, hydrate_list, hydrate_map, hydrate_text, TextEncoding};

fn vbool(v: bool) -> Value<'static> {
    Value::Scalar(Cow::Owned(ScalarValue::Boolean(v)))
}

fn vnull() -> Value<'static> {
    Value::Scalar(Cow::Owned(ScalarValue::Null))
}

#[test]
fn batch_insert_flat_map() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::Map(hydrate_map! {
        "a" => "hello",
        "b" => 42_i64,
        "c" => ScalarValue::Boolean(true),
    });
    let obj_id = doc
        .batch_create_object(ROOT, "data", &value, false)
        .unwrap();

    assert_eq!(
        doc.get(ROOT, "data").unwrap().unwrap().0,
        Value::Object(ObjType::Map)
    );
    assert_eq!(
        doc.get(&obj_id, "a").unwrap().unwrap().0,
        Value::str("hello")
    );
    assert_eq!(doc.get(&obj_id, "b").unwrap().unwrap().0, Value::int(42));
    assert_eq!(doc.get(&obj_id, "c").unwrap().unwrap().0, vbool(true));
}

#[test]
fn batch_insert_nested_maps() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::Map(hydrate_map! {
        "outer" => hydrate_map! {
            "inner_a" => "deep",
            "inner_b" => 99_i64,
        },
        "top_level" => "flat",
    });
    let root_obj = doc
        .batch_create_object(ROOT, "nested", &value, false)
        .unwrap();

    let (outer_val, outer_id) = doc.get(&root_obj, "outer").unwrap().unwrap();
    assert_eq!(outer_val, Value::Object(ObjType::Map));
    assert_eq!(
        doc.get(&outer_id, "inner_a").unwrap().unwrap().0,
        Value::str("deep")
    );
    assert_eq!(
        doc.get(&outer_id, "inner_b").unwrap().unwrap().0,
        Value::int(99)
    );
    assert_eq!(
        doc.get(&root_obj, "top_level").unwrap().unwrap().0,
        Value::str("flat")
    );
}

#[test]
fn batch_insert_map_overwrites_existing_key() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "key", "old_value").unwrap();
    assert_eq!(
        doc.get(ROOT, "key").unwrap().unwrap().0,
        Value::str("old_value")
    );

    let value = hydrate::Value::Map(hydrate_map! { "child" => "new" });
    doc.batch_create_object(ROOT, "key", &value, false).unwrap();

    assert_eq!(
        doc.get(ROOT, "key").unwrap().unwrap().0,
        Value::Object(ObjType::Map)
    );
}

#[test]
fn batch_insert_flat_list() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::List(hydrate_list![1_i64, 2_i64, 3_i64]);
    let obj_id = doc
        .batch_create_object(ROOT, "nums", &value, false)
        .unwrap();

    assert_eq!(doc.length(&obj_id), 3);
    assert_eq!(doc.get(&obj_id, 0).unwrap().unwrap().0, Value::int(1));
    assert_eq!(doc.get(&obj_id, 1).unwrap().unwrap().0, Value::int(2));
    assert_eq!(doc.get(&obj_id, 2).unwrap().unwrap().0, Value::int(3));
}

#[test]
fn batch_insert_list_with_nested_objects() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::List(hydrate_list![
        hydrate_map! { "name" => "alice" },
        hydrate_map! { "name" => "bob" },
    ]);
    let list_id = doc
        .batch_create_object(ROOT, "users", &value, false)
        .unwrap();

    assert_eq!(doc.length(&list_id), 2);
    let (alice_val, alice_id) = doc.get(&list_id, 0).unwrap().unwrap();
    assert_eq!(alice_val, Value::Object(ObjType::Map));
    assert_eq!(
        doc.get(&alice_id, "name").unwrap().unwrap().0,
        Value::str("alice")
    );

    let (bob_val, bob_id) = doc.get(&list_id, 1).unwrap().unwrap();
    assert_eq!(bob_val, Value::Object(ObjType::Map));
    assert_eq!(
        doc.get(&bob_id, "name").unwrap().unwrap().0,
        Value::str("bob")
    );
}

#[test]
fn batch_insert_scalar_fails() {
    let mut doc = AutoCommit::new();
    let Err(_) = doc.batch_create_object(ROOT, "foo", &hydrate::Value::Scalar(1.into()), false)
    else {
        panic!("batch creating a scalar should throw an error");
    };
}

#[test]
fn batch_insert_into_list_at_end() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "items", ObjType::List).unwrap();
    doc.insert(&list_id, 0, "first").unwrap();
    doc.insert(&list_id, 1, "second").unwrap();

    let value = hydrate::Value::Map(hydrate_map! { "key" => "third" });
    doc.batch_create_object(&list_id, 2_usize, &value, true)
        .unwrap();

    assert_eq!(doc.length(&list_id), 3);
    assert_eq!(
        doc.get(&list_id, 0).unwrap().unwrap().0,
        Value::str("first")
    );
    assert_eq!(
        doc.get(&list_id, 1).unwrap().unwrap().0,
        Value::str("second")
    );
    let (third_val, third_id) = doc.get(&list_id, 2).unwrap().unwrap();
    assert_eq!(third_val, Value::Object(ObjType::Map));
    assert_eq!(
        doc.get(&third_id, "key").unwrap().unwrap().0,
        Value::str("third")
    );
}

#[test]
fn batch_insert_into_list_at_middle() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "items", ObjType::List).unwrap();
    doc.insert(&list_id, 0, "a").unwrap();
    doc.insert(&list_id, 1, "c").unwrap();

    // Insert at index 1 with insert=true, shifting "c" to index 2
    let value = hydrate::Value::Map(hydrate_map! { "val" => "b" });
    doc.batch_create_object(&list_id, 1_usize, &value, true)
        .unwrap();

    assert_eq!(doc.length(&list_id), 3);
    assert_eq!(doc.get(&list_id, 0).unwrap().unwrap().0, Value::str("a"));
    assert_eq!(
        doc.get(&list_id, 1).unwrap().unwrap().0,
        Value::Object(ObjType::Map)
    );
    assert_eq!(doc.get(&list_id, 2).unwrap().unwrap().0, Value::str("c"));
}

#[test]
fn batch_put_overwrites_existing_list_element() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "items", ObjType::List).unwrap();
    doc.insert(&list_id, 0, "old_a").unwrap();
    doc.insert(&list_id, 1, "old_b").unwrap();
    doc.insert(&list_id, 2, "old_c").unwrap();

    // Overwrite element at index 1 with a map using insert=false
    let value = hydrate::Value::Map(hydrate_map! { "replaced" => ScalarValue::Boolean(true) });
    doc.batch_create_object(&list_id, 1_usize, &value, false)
        .unwrap();

    // Length should stay the same (overwrite, not insert)
    assert_eq!(doc.length(&list_id), 3);
    assert_eq!(
        doc.get(&list_id, 0).unwrap().unwrap().0,
        Value::str("old_a")
    );
    let (replaced_val, replaced_id) = doc.get(&list_id, 1).unwrap().unwrap();
    assert_eq!(replaced_val, Value::Object(ObjType::Map));
    assert_eq!(
        doc.get(&replaced_id, "replaced").unwrap().unwrap().0,
        vbool(true)
    );
    assert_eq!(
        doc.get(&list_id, 2).unwrap().unwrap().0,
        Value::str("old_c")
    );
}

#[test]
fn batch_insert_with_text() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::Map(hydrate_map! {
        "greeting" => hydrate_text!{"hello world"},
    });
    let obj_id = doc
        .batch_create_object(ROOT, "data", &value, false)
        .unwrap();

    let (text_val, text_id) = doc.get(&obj_id, "greeting").unwrap().unwrap();
    assert_eq!(text_val, Value::Object(ObjType::Text));
    assert_eq!(doc.text(&text_id).unwrap(), "hello world");
}

#[test]
fn batch_insert_text_in_list() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::List(hydrate_list![hydrate_text! {"one"}, hydrate_text! {"two"},]);
    let list_id = doc
        .batch_create_object(ROOT, "texts", &value, false)
        .unwrap();

    assert_eq!(doc.length(&list_id), 2);
    let (_, text0) = doc.get(&list_id, 0).unwrap().unwrap();
    assert_eq!(doc.text(&text0).unwrap(), "one");
    let (_, text1) = doc.get(&list_id, 1).unwrap().unwrap();
    assert_eq!(doc.text(&text1).unwrap(), "two");
}

#[test]
fn batch_insert_deeply_nested() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::Map(hydrate_map! {
        "level1" => hydrate_map! {
            "level2" => hydrate_map! {
                "level3" => hydrate_map! {
                    "level4" => "deep_value",
                },
            },
        },
    });
    let obj_id = doc
        .batch_create_object(ROOT, "deep", &value, false)
        .unwrap();

    let (_, l1) = doc.get(&obj_id, "level1").unwrap().unwrap();
    let (_, l2) = doc.get(&l1, "level2").unwrap().unwrap();
    let (_, l3) = doc.get(&l2, "level3").unwrap().unwrap();
    assert_eq!(
        doc.get(&l3, "level4").unwrap().unwrap().0,
        Value::str("deep_value")
    );
}

#[test]
fn batch_insert_mixed_nesting() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::Map(hydrate_map! {
        "users" => hydrate_list![
            hydrate_map! {
                "name" => "alice",
                "scores" => hydrate_list![10_i64, 20_i64, 30_i64],
            },
            hydrate_map! {
                "name" => "bob",
                "scores" => hydrate_list![40_i64, 50_i64],
            },
        ],
        "count" => 2_i64,
    });
    let obj_id = doc
        .batch_create_object(ROOT, "data", &value, false)
        .unwrap();

    assert_eq!(doc.get(&obj_id, "count").unwrap().unwrap().0, Value::int(2));

    let (_, users_id) = doc.get(&obj_id, "users").unwrap().unwrap();
    assert_eq!(doc.length(&users_id), 2);

    let (_, alice_id) = doc.get(&users_id, 0).unwrap().unwrap();
    assert_eq!(
        doc.get(&alice_id, "name").unwrap().unwrap().0,
        Value::str("alice")
    );
    let (_, alice_scores) = doc.get(&alice_id, "scores").unwrap().unwrap();
    assert_eq!(doc.length(&alice_scores), 3);
    assert_eq!(
        doc.get(&alice_scores, 0).unwrap().unwrap().0,
        Value::int(10)
    );
    assert_eq!(
        doc.get(&alice_scores, 2).unwrap().unwrap().0,
        Value::int(30)
    );

    let (_, bob_id) = doc.get(&users_id, 1).unwrap().unwrap();
    assert_eq!(
        doc.get(&bob_id, "name").unwrap().unwrap().0,
        Value::str("bob")
    );
    let (_, bob_scores) = doc.get(&bob_id, "scores").unwrap().unwrap();
    assert_eq!(doc.length(&bob_scores), 2);
}

#[test]
fn batch_insert_survives_save_load() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::Map(hydrate_map! {
        "name" => "test",
        "items" => hydrate_list![1_i64, 2_i64, 3_i64],
        "nested" => hydrate_map! { "deep" => ScalarValue::Boolean(true) },
    });
    doc.batch_create_object(ROOT, "data", &value, false)
        .unwrap();

    let saved = doc.save();
    let doc2 = AutoCommit::load(&saved).unwrap();

    let (_, data_id) = doc2.get(ROOT, "data").unwrap().unwrap();
    assert_eq!(
        doc2.get(&data_id, "name").unwrap().unwrap().0,
        Value::str("test")
    );

    let (_, items_id) = doc2.get(&data_id, "items").unwrap().unwrap();
    assert_eq!(doc2.length(&items_id), 3);

    let (_, nested_id) = doc2.get(&data_id, "nested").unwrap().unwrap();
    assert_eq!(
        doc2.get(&nested_id, "deep").unwrap().unwrap().0,
        vbool(true)
    );
}

#[test]
fn batch_insert_merges_correctly() {
    let mut doc1 = AutoCommit::new();
    let mut doc2 = doc1.fork();

    let v1 = hydrate::Value::Map(hydrate_map! { "from" => "doc1" });
    doc1.batch_create_object(ROOT, "obj1", &v1, false).unwrap();

    let v2 = hydrate::Value::Map(hydrate_map! { "from" => "doc2" });
    doc2.batch_create_object(ROOT, "obj2", &v2, false).unwrap();

    doc1.merge(&mut doc2).unwrap();

    let (_, obj1_id) = doc1.get(ROOT, "obj1").unwrap().unwrap();
    assert_eq!(
        doc1.get(&obj1_id, "from").unwrap().unwrap().0,
        Value::str("doc1")
    );
    let (_, obj2_id) = doc1.get(ROOT, "obj2").unwrap().unwrap();
    assert_eq!(
        doc1.get(&obj2_id, "from").unwrap().unwrap().0,
        Value::str("doc2")
    );
}

#[test]
fn multiple_batch_inserts() {
    let mut doc = AutoCommit::new();

    let v1 = hydrate::Value::Map(hydrate_map! { "a" => 1_i64 });
    doc.batch_create_object(ROOT, "first", &v1, false).unwrap();

    let v2 = hydrate::Value::Map(hydrate_map! { "b" => 2_i64 });
    doc.batch_create_object(ROOT, "second", &v2, false).unwrap();

    let v3 = hydrate::Value::Map(hydrate_map! { "c" => 3_i64 });
    doc.batch_create_object(ROOT, "third", &v3, false).unwrap();

    let (_, first_id) = doc.get(ROOT, "first").unwrap().unwrap();
    assert_eq!(doc.get(&first_id, "a").unwrap().unwrap().0, Value::int(1));
    let (_, second_id) = doc.get(ROOT, "second").unwrap().unwrap();
    assert_eq!(doc.get(&second_id, "b").unwrap().unwrap().0, Value::int(2));
    let (_, third_id) = doc.get(ROOT, "third").unwrap().unwrap();
    assert_eq!(doc.get(&third_id, "c").unwrap().unwrap().0, Value::int(3));
}

#[test]
fn batch_insert_into_existing_map() {
    let mut doc = AutoCommit::new();
    let parent = doc.put_object(ROOT, "parent", ObjType::Map).unwrap();
    doc.put(&parent, "existing", "value").unwrap();

    let value = hydrate::Value::Map(hydrate_map! { "x" => 1_i64, "y" => 2_i64 });
    doc.batch_create_object(&parent, "child", &value, false)
        .unwrap();

    assert_eq!(
        doc.get(&parent, "existing").unwrap().unwrap().0,
        Value::str("value")
    );
    let (_, child_id) = doc.get(&parent, "child").unwrap().unwrap();
    assert_eq!(doc.get(&child_id, "x").unwrap().unwrap().0, Value::int(1));
}

#[test]
fn batch_insert_into_existing_list() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list_id, 0, "existing").unwrap();

    let value = hydrate::Value::Map(hydrate_map! { "appended" => ScalarValue::Boolean(true) });
    doc.batch_create_object(&list_id, 1_usize, &value, true)
        .unwrap();

    assert_eq!(doc.length(&list_id), 2);
    assert_eq!(
        doc.get(&list_id, 0).unwrap().unwrap().0,
        Value::str("existing")
    );
    let (_, appended_id) = doc.get(&list_id, 1).unwrap().unwrap();
    assert_eq!(
        doc.get(&appended_id, "appended").unwrap().unwrap().0,
        vbool(true)
    );
}

#[test]
fn batch_insert_matches_hydrate_output() {
    let mut doc = AutoCommit::new();
    let input = hydrate::Value::Map(hydrate_map! {
        "name" => "test",
        "count" => 42_i64,
        "tags" => hydrate_list!["alpha", "beta"],
    });
    let obj_id = doc
        .batch_create_object(ROOT, "data", &input, false)
        .unwrap();

    let hydrated = doc.hydrate(&obj_id, None).unwrap();

    match &hydrated {
        hydrate::Value::Map(map) => {
            assert_eq!(map.get("name"), Some(&hydrate::Value::from("test")));
            assert_eq!(map.get("count"), Some(&hydrate::Value::from(42_i64)));
            match map.get("tags") {
                Some(hydrate::Value::List(list)) => assert_eq!(list.len(), 2),
                other => panic!("expected list for tags, got {:?}", other),
            }
        }
        other => panic!("expected map, got {:?}", other),
    }
}

#[test]
fn batch_insert_with_transaction() {
    let mut doc = automerge::Automerge::new();
    let mut tx = doc.transaction();

    let value = hydrate::Value::Map(hydrate_map! { "key" => "from_tx" });
    let obj_id = tx.batch_create_object(ROOT, "data", &value, false).unwrap();
    assert_eq!(
        tx.get(&obj_id, "key").unwrap().unwrap().0,
        Value::str("from_tx")
    );

    tx.commit();

    assert_eq!(
        doc.get(ROOT, "data").unwrap().unwrap().0,
        Value::Object(ObjType::Map)
    );
}

#[test]
fn batch_insert_transaction_rollback() {
    let mut doc = automerge::Automerge::new();
    {
        let mut tx = doc.transaction();
        let value = hydrate::Value::Map(hydrate_map! { "key" => "should_be_gone" });
        tx.batch_create_object(ROOT, "data", &value, false).unwrap();
        tx.rollback();
    }

    assert!(doc.get(ROOT, "data").unwrap().is_none());
}

#[test]
fn batch_insert_empty_map() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::Map(hydrate_map! {});
    let obj_id = doc
        .batch_create_object(ROOT, "empty", &value, false)
        .unwrap();

    assert_eq!(
        doc.get(ROOT, "empty").unwrap().unwrap().0,
        Value::Object(ObjType::Map)
    );
    assert_eq!(doc.keys(&obj_id).count(), 0);
}

#[test]
fn batch_insert_empty_list() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::List(hydrate_list![]);
    let obj_id = doc
        .batch_create_object(ROOT, "empty", &value, false)
        .unwrap();

    assert_eq!(
        doc.get(ROOT, "empty").unwrap().unwrap().0,
        Value::Object(ObjType::List)
    );
    assert_eq!(doc.length(&obj_id), 0);
}

#[test]
fn batch_insert_empty_text() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::Text(hydrate::Text::new(TextEncoding::UnicodeCodePoint, ""));
    let obj_id = doc
        .batch_create_object(ROOT, "empty", &value, false)
        .unwrap();

    assert_eq!(
        doc.get(ROOT, "empty").unwrap().unwrap().0,
        Value::Object(ObjType::Text)
    );
    assert_eq!(doc.text(&obj_id).unwrap(), "");
}

#[test]
fn batch_insert_various_scalar_types() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::Map(hydrate_map! {
        "str_val" => "hello",
        "int_val" => 42_i64,
        "uint_val" => 100_u64,
        "float_val" => 5.14_f64,
        "bool_true" => ScalarValue::Boolean(true),
        "bool_false" => ScalarValue::Boolean(false),
        "null_val" => ScalarValue::Null,
    });
    let obj_id = doc
        .batch_create_object(ROOT, "scalars", &value, false)
        .unwrap();

    assert_eq!(
        doc.get(&obj_id, "str_val").unwrap().unwrap().0,
        Value::str("hello")
    );
    assert_eq!(
        doc.get(&obj_id, "int_val").unwrap().unwrap().0,
        Value::int(42)
    );
    assert_eq!(
        doc.get(&obj_id, "uint_val").unwrap().unwrap().0,
        Value::uint(100)
    );
    assert_eq!(
        doc.get(&obj_id, "float_val").unwrap().unwrap().0,
        Value::f64(5.14)
    );
    assert_eq!(
        doc.get(&obj_id, "bool_true").unwrap().unwrap().0,
        vbool(true)
    );
    assert_eq!(
        doc.get(&obj_id, "bool_false").unwrap().unwrap().0,
        vbool(false)
    );
    assert_eq!(doc.get(&obj_id, "null_val").unwrap().unwrap().0, vnull());
}

#[test]
fn batch_insert_equivalent_to_individual_ops() {
    let mut doc_batch = AutoCommit::new();
    let value = hydrate::Value::Map(hydrate_map! {
        "name" => "test",
        "count" => 5_i64,
        "items" => hydrate_list!["a", "b", "c"],
    });
    doc_batch
        .batch_create_object(ROOT, "data", &value, false)
        .unwrap();

    let mut doc_individual = AutoCommit::new();
    let data = doc_individual
        .put_object(ROOT, "data", ObjType::Map)
        .unwrap();
    doc_individual.put(&data, "name", "test").unwrap();
    doc_individual.put(&data, "count", 5_i64).unwrap();
    let items = doc_individual
        .put_object(&data, "items", ObjType::List)
        .unwrap();
    doc_individual.insert(&items, 0, "a").unwrap();
    doc_individual.insert(&items, 1, "b").unwrap();
    doc_individual.insert(&items, 2, "c").unwrap();

    let hydrated_batch = doc_batch.hydrate(ROOT, None).unwrap();
    let hydrated_individual = doc_individual.hydrate(ROOT, None).unwrap();
    assert_eq!(hydrated_batch, hydrated_individual);
}

#[test]
fn batch_insert_generates_patches() {
    let mut doc = AutoCommit::new();
    doc.update_diff_cursor();

    let value = hydrate::Value::Map(hydrate_map! {
        "name" => "test",
        "items" => hydrate_list![1_i64, 2_i64],
    });
    doc.batch_create_object(ROOT, "data", &value, false)
        .unwrap();

    let patches = doc.diff_incremental();
    assert!(!patches.is_empty(), "expected patches from batch insert");

    let has_data_patch = patches
        .iter()
        .any(|p| matches!(&p.action, PatchAction::PutMap { key, .. } if key == "data"));
    assert!(has_data_patch, "should have a PutMap patch for 'data'");
}

#[test]
fn batch_insert_text_generates_splice_patch() {
    let mut doc = AutoCommit::new();
    doc.update_diff_cursor();

    let value = hydrate::Value::Map(hydrate_map! {
        "greeting" => hydrate_text!{"hi"},
    });
    doc.batch_create_object(ROOT, "data", &value, false)
        .unwrap();

    let patches = doc.diff_incremental();
    let has_splice = patches
        .iter()
        .any(|p| matches!(&p.action, PatchAction::SpliceText { .. }));
    assert!(
        has_splice,
        "should have a SpliceText patch, got: {:?}",
        patches.iter().map(|p| &p.action).collect::<Vec<_>>()
    );
}

#[test]
fn batch_insert_list_of_lists() {
    let mut doc = AutoCommit::new();
    let value = hydrate::Value::List(hydrate_list![
        hydrate_list![1_i64, 2_i64],
        hydrate_list![3_i64, 4_i64],
    ]);
    let outer = doc
        .batch_create_object(ROOT, "matrix", &value, false)
        .unwrap();

    assert_eq!(doc.length(&outer), 2);
    let (_, inner0) = doc.get(&outer, 0).unwrap().unwrap();
    assert_eq!(doc.length(&inner0), 2);
    assert_eq!(doc.get(&inner0, 0).unwrap().unwrap().0, Value::int(1));
    assert_eq!(doc.get(&inner0, 1).unwrap().unwrap().0, Value::int(2));
    let (_, inner1) = doc.get(&outer, 1).unwrap().unwrap();
    assert_eq!(doc.length(&inner1), 2);
    assert_eq!(doc.get(&inner1, 0).unwrap().unwrap().0, Value::int(3));
    assert_eq!(doc.get(&inner1, 1).unwrap().unwrap().0, Value::int(4));
}

#[test]
fn batch_put_overwrite_with_nested_structure() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "items", ObjType::List).unwrap();
    doc.insert(&list_id, 0, "placeholder").unwrap();
    doc.insert(&list_id, 1, "keep").unwrap();

    // Overwrite index 0 with a complex nested structure
    let value = hydrate::Value::Map(hydrate_map! {
        "name" => "complex",
        "children" => hydrate_list![
            hydrate_map! { "id" => 1_i64 },
            hydrate_map! { "id" => 2_i64 },
        ],
    });
    doc.batch_create_object(&list_id, 0_usize, &value, false)
        .unwrap();

    assert_eq!(doc.length(&list_id), 2);
    let (_, obj_id) = doc.get(&list_id, 0).unwrap().unwrap();
    assert_eq!(
        doc.get(&obj_id, "name").unwrap().unwrap().0,
        Value::str("complex")
    );
    let (_, children_id) = doc.get(&obj_id, "children").unwrap().unwrap();
    assert_eq!(doc.length(&children_id), 2);
    let (_, child0) = doc.get(&children_id, 0).unwrap().unwrap();
    assert_eq!(doc.get(&child0, "id").unwrap().unwrap().0, Value::int(1));
    assert_eq!(doc.get(&list_id, 1).unwrap().unwrap().0, Value::str("keep"));
}

#[test]
fn batch_init_map_flat() {
    let map = hydrate_map! {
        "name" => "test",
        "count" => 42_i64,
    };
    let mut doc = AutoCommit::new();
    doc.init_root_from_hydrate(&map).unwrap();

    assert_eq!(
        doc.get(ROOT, "name").unwrap().unwrap().0,
        Value::str("test")
    );
    assert_eq!(doc.get(ROOT, "count").unwrap().unwrap().0, Value::int(42));
}

#[test]
fn batch_init_map_nested() {
    let map = hydrate_map! {
        "users" => hydrate_list![
            hydrate_map! { "name" => "alice" },
            hydrate_map! { "name" => "bob" },
        ],
        "meta" => hydrate_map! {
            "version" => 1_i64,
        },
    };
    let mut doc = AutoCommit::new();
    doc.init_root_from_hydrate(&map).unwrap();

    let (_, users_id) = doc.get(ROOT, "users").unwrap().unwrap();
    assert_eq!(doc.length(&users_id), 2);

    let (_, alice_id) = doc.get(&users_id, 0).unwrap().unwrap();
    assert_eq!(
        doc.get(&alice_id, "name").unwrap().unwrap().0,
        Value::str("alice")
    );

    let (_, meta_id) = doc.get(ROOT, "meta").unwrap().unwrap();
    assert_eq!(
        doc.get(&meta_id, "version").unwrap().unwrap().0,
        Value::int(1)
    );
}

#[test]
fn batch_init_map_with_text() {
    let map = hydrate_map! {
        "greeting" => hydrate_text!{"hello world"},
    };
    let mut doc = AutoCommit::new();
    doc.init_root_from_hydrate(&map).unwrap();

    let (val, text_id) = doc.get(ROOT, "greeting").unwrap().unwrap();
    assert_eq!(val, Value::Object(ObjType::Text));
    assert_eq!(doc.text(&text_id).unwrap(), "hello world");
}

#[test]
fn batch_init_map_survives_save_load() {
    let map = hydrate_map! {
        "name" => "test",
        "items" => hydrate_list![1_i64, 2_i64, 3_i64],
    };
    let mut doc = AutoCommit::new();
    doc.init_root_from_hydrate(&map).unwrap();

    let saved = doc.save();
    let doc2 = AutoCommit::load(&saved).unwrap();

    assert_eq!(
        doc2.get(ROOT, "name").unwrap().unwrap().0,
        Value::str("test")
    );
    let (_, items_id) = doc2.get(ROOT, "items").unwrap().unwrap();
    assert_eq!(doc2.length(&items_id), 3);
}

#[test]
fn batch_init_map_equivalent_to_individual_ops() {
    let map = hydrate_map! {
        "name" => "test",
        "count" => 5_i64,
        "items" => hydrate_list!["a", "b", "c"],
    };
    let mut doc_batch = AutoCommit::new();
    doc_batch.init_root_from_hydrate(&map).unwrap();

    let mut doc_individual = AutoCommit::new();
    doc_individual.put(ROOT, "name", "test").unwrap();
    doc_individual.put(ROOT, "count", 5_i64).unwrap();
    let items = doc_individual
        .put_object(ROOT, "items", ObjType::List)
        .unwrap();
    doc_individual.insert(&items, 0, "a").unwrap();
    doc_individual.insert(&items, 1, "b").unwrap();
    doc_individual.insert(&items, 2, "c").unwrap();

    let hydrated_batch = doc_batch.hydrate(ROOT, None).unwrap();
    let hydrated_individual = doc_individual.hydrate(ROOT, None).unwrap();
    assert_eq!(hydrated_batch, hydrated_individual);
}

#[test]
fn batch_init_map_generates_patches() {
    let map = hydrate_map! {
        "name" => "test",
        "items" => hydrate_list![1_i64, 2_i64],
    };
    let mut doc = AutoCommit::new();
    doc.init_root_from_hydrate(&map).unwrap();

    let heads = doc.get_heads();
    let patches = doc.diff(&[], &heads);
    assert!(
        !patches.is_empty(),
        "expected patches from new_from_hydrate"
    );

    let has_name_patch = patches
        .iter()
        .any(|p| matches!(&p.action, PatchAction::PutMap { key, .. } if key == "name"));
    assert!(has_name_patch, "should have a PutMap patch for 'name'");
}

#[test]
fn splice_insert_scalars() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list_id, 0, "a").unwrap();
    doc.insert(&list_id, 1, "d").unwrap();

    let values = vec![
        hydrate::Value::Scalar(ScalarValue::from("b")),
        hydrate::Value::Scalar(ScalarValue::from("c")),
    ];
    doc.splice(&list_id, 1, 0, values).unwrap();

    assert_eq!(doc.length(&list_id), 4);
    assert_eq!(doc.get(&list_id, 0).unwrap().unwrap().0, Value::str("a"));
    assert_eq!(doc.get(&list_id, 1).unwrap().unwrap().0, Value::str("b"));
    assert_eq!(doc.get(&list_id, 2).unwrap().unwrap().0, Value::str("c"));
    assert_eq!(doc.get(&list_id, 3).unwrap().unwrap().0, Value::str("d"));
}

#[test]
fn splice_insert_objects() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "list", ObjType::List).unwrap();

    let values = vec![
        hydrate::Value::Map(hydrate_map! { "name" => "alice" }),
        hydrate::Value::Map(hydrate_map! { "name" => "bob" }),
    ];
    doc.splice(&list_id, 0, 0, values).unwrap();

    assert_eq!(doc.length(&list_id), 2);
    let (_, alice_id) = doc.get(&list_id, 0).unwrap().unwrap();
    assert_eq!(
        doc.get(&alice_id, "name").unwrap().unwrap().0,
        Value::str("alice")
    );
    let (_, bob_id) = doc.get(&list_id, 1).unwrap().unwrap();
    assert_eq!(
        doc.get(&bob_id, "name").unwrap().unwrap().0,
        Value::str("bob")
    );
}

#[test]
fn splice_insert_mixed() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "list", ObjType::List).unwrap();

    let values = vec![
        hydrate::Value::Scalar(ScalarValue::from("hello")),
        hydrate::Value::Map(hydrate_map! { "nested" => ScalarValue::Boolean(true) }),
        hydrate::Value::List(hydrate_list![1_i64, 2_i64]),
    ];
    doc.splice(&list_id, 0, 0, values).unwrap();

    assert_eq!(doc.length(&list_id), 3);
    assert_eq!(
        doc.get(&list_id, 0).unwrap().unwrap().0,
        Value::str("hello")
    );
    let (_, map_id) = doc.get(&list_id, 1).unwrap().unwrap();
    assert_eq!(doc.get(&map_id, "nested").unwrap().unwrap().0, vbool(true));
    let (_, inner_list) = doc.get(&list_id, 2).unwrap().unwrap();
    assert_eq!(doc.length(&inner_list), 2);
}

#[test]
fn splice_delete_and_insert() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list_id, 0, "a").unwrap();
    doc.insert(&list_id, 1, "b").unwrap();
    doc.insert(&list_id, 2, "c").unwrap();

    // Delete "b", insert "x" and "y" in its place
    let values = vec![
        hydrate::Value::Scalar(ScalarValue::from("x")),
        hydrate::Value::Scalar(ScalarValue::from("y")),
    ];
    doc.splice(&list_id, 1, 1, values).unwrap();

    assert_eq!(doc.length(&list_id), 4);
    assert_eq!(doc.get(&list_id, 0).unwrap().unwrap().0, Value::str("a"));
    assert_eq!(doc.get(&list_id, 1).unwrap().unwrap().0, Value::str("x"));
    assert_eq!(doc.get(&list_id, 2).unwrap().unwrap().0, Value::str("y"));
    assert_eq!(doc.get(&list_id, 3).unwrap().unwrap().0, Value::str("c"));
}

#[test]
fn splice_delete_only() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list_id, 0, "a").unwrap();
    doc.insert(&list_id, 1, "b").unwrap();
    doc.insert(&list_id, 2, "c").unwrap();

    doc.splice(&list_id, 1, 1, Vec::<hydrate::Value>::new())
        .unwrap();

    assert_eq!(doc.length(&list_id), 2);
    assert_eq!(doc.get(&list_id, 0).unwrap().unwrap().0, Value::str("a"));
    assert_eq!(doc.get(&list_id, 1).unwrap().unwrap().0, Value::str("c"));
}

#[test]
fn splice_with_text() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "list", ObjType::List).unwrap();

    let values = vec![
        hydrate::Value::Text(hydrate::Text::new(TextEncoding::UnicodeCodePoint, "hello")),
        hydrate::Value::Text(hydrate::Text::new(TextEncoding::UnicodeCodePoint, "world")),
    ];
    doc.splice(&list_id, 0, 0, values).unwrap();

    assert_eq!(doc.length(&list_id), 2);
    let (_, text0) = doc.get(&list_id, 0).unwrap().unwrap();
    assert_eq!(doc.text(&text0).unwrap(), "hello");
    let (_, text1) = doc.get(&list_id, 1).unwrap().unwrap();
    assert_eq!(doc.text(&text1).unwrap(), "world");
}

#[test]
fn splice_deeply_nested() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "list", ObjType::List).unwrap();

    let values = vec![hydrate::Value::Map(hydrate_map! {
        "users" => hydrate_list![
            hydrate_map! {
                "name" => "alice",
                "scores" => hydrate_list![10_i64, 20_i64],
            },
        ],
    })];
    doc.splice(&list_id, 0, 0, values).unwrap();

    let (_, map_id) = doc.get(&list_id, 0).unwrap().unwrap();
    let (_, users_id) = doc.get(&map_id, "users").unwrap().unwrap();
    let (_, alice_id) = doc.get(&users_id, 0).unwrap().unwrap();
    assert_eq!(
        doc.get(&alice_id, "name").unwrap().unwrap().0,
        Value::str("alice")
    );
    let (_, scores_id) = doc.get(&alice_id, "scores").unwrap().unwrap();
    assert_eq!(doc.length(&scores_id), 2);
    assert_eq!(doc.get(&scores_id, 0).unwrap().unwrap().0, Value::int(10));
}

#[test]
fn splice_survives_save_load() {
    let mut doc = AutoCommit::new();
    let list_id = doc.put_object(ROOT, "list", ObjType::List).unwrap();

    let values = vec![
        hydrate::Value::Map(hydrate_map! { "key" => "val" }),
        hydrate::Value::Scalar(ScalarValue::from(42_i64)),
    ];
    doc.splice(&list_id, 0, 0, values).unwrap();

    let saved = doc.save();
    let doc2 = AutoCommit::load(&saved).unwrap();

    let (_, list_id2) = doc2.get(ROOT, "list").unwrap().unwrap();
    assert_eq!(doc2.length(&list_id2), 2);
    let (_, map_id) = doc2.get(&list_id2, 0).unwrap().unwrap();
    assert_eq!(
        doc2.get(&map_id, "key").unwrap().unwrap().0,
        Value::str("val")
    );
    assert_eq!(doc2.get(&list_id2, 1).unwrap().unwrap().0, Value::int(42));
}

#[test]
fn splice_merges_correctly() {
    let mut doc1 = AutoCommit::new();
    let list_id = doc1.put_object(ROOT, "list", ObjType::List).unwrap();
    doc1.insert(&list_id, 0, "shared").unwrap();

    let mut doc2 = doc1.fork();

    let values1 = vec![hydrate::Value::Map(hydrate_map! { "from" => "doc1" })];
    doc1.splice(&list_id, 1, 0, values1).unwrap();

    let list_id2 = doc2.get(ROOT, "list").unwrap().unwrap().1;
    let values2 = vec![hydrate::Value::Map(hydrate_map! { "from" => "doc2" })];
    doc2.splice(&list_id2, 1, 0, values2).unwrap();

    doc1.merge(&mut doc2).unwrap();

    assert_eq!(doc1.length(&list_id), 3);
    assert_eq!(
        doc1.get(&list_id, 0).unwrap().unwrap().0,
        Value::str("shared")
    );
}
