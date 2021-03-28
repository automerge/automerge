use wasm_bindgen_test::*;

#[wasm_bindgen_test]
fn test_simple_frontend_change_with_set_sequence() {
    let mut f = automerge_frontend::Frontend::new_with_timestamper(Box::new(|| None));
    f.change::<_, automerge_frontend::InvalidChangeRequest>(None, |doc| {
        doc.add_change(automerge_frontend::LocalChange::set(
            automerge_frontend::Path::root().key(""),
            automerge_frontend::Value::Sequence(vec![]),
        ))
        .unwrap();
        Ok(())
    })
    .unwrap();
}
