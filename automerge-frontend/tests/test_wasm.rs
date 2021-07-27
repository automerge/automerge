use automerge_frontend::{Options, Schema};
use automerge_protocol::ActorId;
use wasm_bindgen_test::*;

#[wasm_bindgen_test]
fn test_simple_frontend_change_with_set_sequence() {
    let mut f = automerge_frontend::Frontend::new(Options {
        timestamper: || None,
        actor_id: ActorId::random(),
        schema: Schema::default(),
    });
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
