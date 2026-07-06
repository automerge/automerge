use automerge::hydrate;
use automerge::sync::{State, SyncDoc};
use automerge::transaction::Transactable;
use automerge::{ActorId, AutoCommit, ObjType, ScalarValue, ROOT};
use std::collections::HashMap;

fn nested_map(s: &str, a: i64, b: i64) -> hydrate::Value {
    let mut inner = HashMap::new();
    inner.insert("m4".to_string(), hydrate::Value::scalar(a));
    inner.insert("n4".to_string(), hydrate::Value::scalar(b));

    let mut outer = HashMap::new();
    outer.insert(
        "m3".to_string(),
        hydrate::Value::Scalar(ScalarValue::Str(s.into())),
    );
    outer.insert("n3".to_string(), hydrate::Value::Map(inner.into()));
    hydrate::Value::Map(outer.into())
}

fn sync_round(
    left: &mut AutoCommit,
    right: &mut AutoCommit,
    left_state: &mut State,
    right_state: &mut State,
) {
    if let Some(message) = left.sync().generate_sync_message(left_state) {
        right
            .sync()
            .receive_sync_message(right_state, message)
            .unwrap();
    }
    if let Some(message) = right.sync().generate_sync_message(right_state) {
        left.sync()
            .receive_sync_message(left_state, message)
            .unwrap();
    }
}

#[test]
#[ignore = "reproduces fuzz crash: op order invalid after syncing batch-created object in list"]
fn batch_create_map_prop_in_list_then_sync_keeps_op_order() {
    let mut left = AutoCommit::new();
    left.set_actor(ActorId::from(vec![0]));

    let list = left.put_object(ROOT, "list", ObjType::List).unwrap();
    left.batch_create_object(ROOT, "root_map", &nested_map("multi\nline", 60, 61), false)
        .unwrap();

    let mut right = AutoCommit::load(&left.save()).unwrap();

    left.batch_create_object(&list, "bad_map_prop", &nested_map("", -36, -35), false)
        .unwrap();

    let mut left_state = State::new();
    let mut right_state = State::new();
    for _ in 0..8 {
        sync_round(&mut left, &mut right, &mut left_state, &mut right_state);
    }
}
