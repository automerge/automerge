use automerge_frontend::Options;
use wasm_bindgen_test::*;

#[wasm_bindgen_test]
fn test_simple_frontend_change_with_set_sequence() {
    let mut f = automerge_frontend::Frontend::new(Options::default().with_timestamper(|| None));
    f.change::<_, _, automerge_frontend::InvalidChangeRequest>(None, |doc| {
        doc.add_change(automerge_frontend::LocalChange::set(
            automerge_frontend::Path::root().key(""),
            automerge_frontend::Value::List(vec![]),
        ))
        .unwrap();
        Ok(())
    })
    .unwrap();
}
