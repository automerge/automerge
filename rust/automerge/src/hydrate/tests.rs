use text_value::ConcreteTextValue;

use crate::exid::ExId;
use crate::transaction::Transactable;
use crate::*;

#[test]
fn simple_hydrate() -> Result<(), AutomergeError> {
    let mut doc = AutoCommit::default();
    let list = doc.put_object(&ObjId::Root, "list", ObjType::List)?;
    doc.insert(&list, 0, 5)?;
    doc.insert(&list, 1, 6)?;
    doc.insert(&list, 2, 7)?;
    doc.insert(&list, 3, "hello")?;
    doc.insert(&list, 4, ScalarValue::counter(100))?;
    doc.insert_object(&list, 5, ObjType::Map)?;
    doc.insert_object(&list, 6, ObjType::List)?;
    let text = doc.put_object(&ObjId::Root, "text", ObjType::Text)?;
    doc.splice_text(&text, 0, 0, "hello world")?;
    let mut hydrated = doc.hydrate(ExId::Root, None)?;
    assert_eq!(
        hydrated,
        hydrate_map!(
            "list" => hydrate_list!(5,6,7,"hello", ScalarValue::counter(100), hydrate_map!(), hydrate_list![]),
            "text" => ConcreteTextValue::new("hello world", TextEncoding::platform_default()),
        ).into()
    );
    doc.splice_text(&text, 6, 0, "big bad ")?;
    assert_eq!(doc.text(&text)?, "hello big bad world".to_owned());
    let heads = doc.get_heads();
    let cursor = doc.diff_cursor().to_vec();
    let patches = doc.diff(&cursor, &heads);
    doc.update_diff_cursor();
    hydrated.apply_patches(TextEncoding::platform_default(), patches)?;
    let hydrate::Value::Text(val) = &hydrated.as_map().unwrap().get("text").unwrap() else {
        panic!("expected text");
    };
    assert_eq!(String::from(val), "hello big bad world".to_string(),);
    Ok(())
}

/// A document of `n` small maps under `children`, each of two keys.
fn many_small_maps(n: usize) -> (AutoCommit, Vec<ExId>) {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(b"small maps".as_slice()));
    let children = doc
        .put_object(&ObjId::Root, "children", ObjType::Map)
        .unwrap();
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let child = doc
            .put_object(&children, format!("c{i:05}"), ObjType::Map)
            .unwrap();
        doc.put(&child, "at", i as i64).unwrap();
        doc.put(&child, "kind", "leaf").unwrap();
        ids.push(child);
    }
    doc.commit();
    (doc, ids)
}

/// `hydrate` seeks every op column to each object it reads, so reading a
/// document of many small maps seeks once per map, whether the whole
/// document or each map in turn. The one-pass read seeks once.
#[test]
fn reading_many_small_maps_in_one_pass_seeks_once() {
    use crate::seek_counters::{reset, top_ops_seeks};

    let (doc, ids) = many_small_maps(1000);

    reset();
    let whole = doc.hydrate(&ObjId::Root, None).unwrap();
    // The root, `children` and each of its maps.
    assert_eq!(top_ops_seeks(), 1002);
    reset();
    assert_eq!(doc.hydrate_in_one_pass(), whole);
    let one_pass = top_ops_seeks();
    // Before the one-pass read: 1002 seeks (`hydrate`); after: 1. The gate
    // is a tenth of the way from after to before.
    assert!(
        one_pass <= 101,
        "{one_pass} seeks to read the whole document"
    );

    reset();
    let each: Vec<_> = ids
        .iter()
        .map(|id| doc.hydrate(id, None).unwrap())
        .collect();
    assert_eq!(top_ops_seeks(), 1000);
    reset();
    let mut gathered = doc.gather_in_one_pass();
    let built: Vec<_> = ids
        .iter()
        .map(|id| doc.hydrate_gathered(&mut gathered, id).unwrap())
        .collect();
    let one_pass = top_ops_seeks();
    assert_eq!(built, each);
    // Before: 1000 seeks (`hydrate` for each map); after: 1. The gate is a
    // tenth of the way from after to before.
    assert!(one_pass <= 100, "{one_pass} seeks to read each map");
}
