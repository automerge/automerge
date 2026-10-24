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
