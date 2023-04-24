use crate::hydrate;
use crate::op_observer::{HasPatches, TextRepresentation};
use crate::text_value::TextValue;
use crate::transaction::{Observed, Transactable};
use crate::*;

#[test]
fn simple_hydrate() -> Result<(), AutomergeError> {
    let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default()
        .with_observer(VecOpObserver::default().with_text_rep(TextRepresentation::String));
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
    let mut hydrated = doc.hydrate(None);
    assert_eq!(
        hydrated,
        hydrate_map!(
            "list" => hydrate_list!(5,6,7,"hello", ScalarValue::counter(100), hydrate_map!(), hydrate_list![]),
            "text" => TextValue::new("hello world"),
        )
    );
    doc.splice_text(&text, 6, 0, "big bad ")?;
    assert_eq!(doc.text(&text)?, "hello big bad world".to_owned());
    let patches = doc.observer().take_patches();
    hydrated.apply_patches(patches)?;
    assert_eq!(
        hydrated.as_map().unwrap().get("text"),
        Some(&TextValue::new("hello big bad world").into())
    );
    Ok(())
}
