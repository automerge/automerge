use automerge::{
    transaction::Transactable, AutoCommit, LoadOptions, ObjType, ReadDoc, StringMigration, Value,
    ROOT,
};
use test_log::test;

#[test]
fn test_strings_in_maps_are_converted_to_text() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "somestring", "hello").unwrap();
    let saved = doc.save();

    let loaded = AutoCommit::load_with_options(
        &saved,
        LoadOptions::new().migrate_strings(StringMigration::ConvertToText),
    )
    .unwrap();

    let val = loaded.get(ROOT, "somestring").unwrap();
    let Some((val, obj_id)) = val else {
        panic!("no value found for key 'somestring'");
    };
    let Value::Object(obj) = val else {
        panic!("expected an object, found {:?}", val);
    };
    let ObjType::Text = obj else {
        panic!("expected a text object, found {:?}", obj);
    };
    let text = loaded.text(obj_id).unwrap();
    assert_eq!(text, "hello");
}

#[test]
fn test_strings_in_lists_are_converted_to_text() {
    let mut doc = AutoCommit::new();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, "hello").unwrap();
    let saved = doc.save();

    let loaded = AutoCommit::load_with_options(
        &saved,
        LoadOptions::new().migrate_strings(StringMigration::ConvertToText),
    )
    .unwrap();

    let val = loaded.get(&list, 0).unwrap();
    let Some((val, obj_id)) = val else {
        panic!("no value found for key 'somestring'");
    };
    let Value::Object(obj) = val else {
        panic!("expected an object, found {:?}", val);
    };
    let ObjType::Text = obj else {
        panic!("expected a text object, found {:?}", obj);
    };
    let text = loaded.text(obj_id).unwrap();
    assert_eq!(text, "hello");
}

#[test]
fn test_does_not_add_size_when_strings_are_not_converted() {
    let empty_document = AutoCommit::new().save();
    let mut loaded = AutoCommit::load_with_options(
        &empty_document,
        LoadOptions::new().migrate_strings(StringMigration::ConvertToText),
    )
    .unwrap();

    let saved_again = loaded.save();

    assert_eq!(empty_document.len(), saved_again.len());
}
