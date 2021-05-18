use std::collections::HashMap;

use automerge::{Path, Primitive, Value};
use test_env_log::test;

#[test]
fn test_frontend_uses_correct_elem_ids() {
    let mut hm = HashMap::new();
    hm.insert(
        "a".to_owned(),
        automerge::Value::Sequence(vec![automerge::Value::Primitive(Primitive::Null)]),
    );
    let mut backend = automerge::Backend::new();

    let (mut frontend, change) =
        automerge::Frontend::new_with_initial_state(Value::Map(hm, automerge::MapType::Map))
            .unwrap();

    println!("change1 {:?}", change);

    let (patch, _) = backend.apply_local_change(change).unwrap();
    frontend.apply_patch(patch).unwrap();

    let ((), c) = frontend
        .change::<_, _, automerge::InvalidChangeRequest>(None, |d| {
            d.add_change(automerge::LocalChange::set(
                automerge::Path::root().key("a").index(0),
                automerge::Value::Primitive(automerge::Primitive::Int(0)),
            ))
            .unwrap();
            d.add_change(automerge::LocalChange::insert(
                automerge::Path::root().key("a").index(1),
                automerge::Value::Primitive(automerge::Primitive::Boolean(false)),
            ))
            .unwrap();
            Ok(())
        })
        .unwrap();

    let mut ehm = HashMap::new();
    ehm.insert(
        "a".to_owned(),
        automerge::Value::Sequence(vec![
            automerge::Value::Primitive(automerge::Primitive::Int(0)),
            automerge::Value::Primitive(automerge::Primitive::Boolean(false)),
        ]),
    );
    let expected = automerge::Value::Map(ehm.clone(), automerge::MapType::Map);

    assert_eq!(expected, frontend.get_value(&Path::root()).unwrap());

    if let Some(c) = c {
        println!("change2 {:?}", c);
        let (p, _) = backend.apply_local_change(c).unwrap();
        frontend.apply_patch(p).unwrap();
    }
    let v = frontend.get_value(&Path::root()).unwrap();

    let expected = automerge::Value::Map(ehm, automerge::MapType::Map);
    assert_eq!(expected, v);
}
