use automerge::Automerge;

#[test]
fn no_conflict_on_repeated_assignment() {
    let mut doc = Automerge::new();
    doc.set(automerge::ROOT, "foo", 1).unwrap();
    doc.set(automerge::ROOT, "foo", 2).unwrap();
    assert_eq!(
        doc.values(automerge::ROOT, "foo")
            .unwrap()
            .into_iter()
            .map(|e| e.0)
            .collect::<Vec<automerge::Value>>(),
        vec![2.into()]
    );
}

#[test]
fn no_change_on_repeated_map_set() {
    let mut doc = Automerge::new();
    doc.set(automerge::ROOT, "foo", 1).unwrap();
    assert!(doc.set(automerge::ROOT, "foo", 1).unwrap().is_none());
}

#[test]
fn no_change_on_repeated_list_set() {
    let mut doc = Automerge::new();
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
    let mut doc = Automerge::new();
    let list_id = doc
        .set(automerge::ROOT, "list", automerge::Value::list())
        .unwrap()
        .unwrap();
    doc.insert(list_id, 0, 1).unwrap();
    assert!(doc.set(list_id, 0, 1).unwrap().is_none());
}
