extern crate automerge_protocol as amp;
use std::num::NonZeroU32;

use amp::SortedVec;
use proptest::prelude::*;

fn arb_objtype() -> impl Strategy<Value = amp::ObjType> {
    prop_oneof![
        Just(amp::ObjType::Map),
        Just(amp::ObjType::Table),
        Just(amp::ObjType::List),
        Just(amp::ObjType::Text),
    ]
}

fn arb_scalar_value() -> impl Strategy<Value = amp::ScalarValue> {
    prop_oneof![
        any::<String>().prop_map(|s| amp::ScalarValue::Str(s.into())),
        any::<i64>().prop_map(amp::ScalarValue::Int),
        //This is necessary because we don't support integers larger than i64 in the JSON protocol
        //any::<i64>().prop_map(|i| amp::ScalarValue::Uint(i as u64)),
        any::<u64>().prop_map(amp::ScalarValue::Uint),
        any::<f64>().prop_map(amp::ScalarValue::F64),
        any::<f32>().prop_map(amp::ScalarValue::F32),
        any::<i64>().prop_map(amp::ScalarValue::Counter),
        any::<i64>().prop_map(amp::ScalarValue::Timestamp),
        any::<bool>().prop_map(amp::ScalarValue::Boolean),
        Just(amp::ScalarValue::Null),
    ]
}

fn arb_optype() -> impl Strategy<Value = amp::OpType> {
    prop_oneof![
        arb_objtype().prop_map(amp::OpType::Make),
        any::<u32>().prop_map(|u| amp::OpType::Del(NonZeroU32::new(u + 1).unwrap())),
        any::<i64>().prop_map(amp::OpType::Inc),
        arb_scalar_value().prop_map(amp::OpType::Set),
    ]
}

fn arb_actorid() -> impl Strategy<Value = amp::ActorId> {
    proptest::collection::vec(any::<u8>(), 32).prop_map(|bytes| amp::ActorId::from_bytes(&bytes))
}

fn arb_opid() -> impl Strategy<Value = amp::OpId> {
    (any::<u64>(), arb_actorid()).prop_map(|(seq, actor)| amp::OpId::new(seq, &actor))
}

fn arb_objid() -> impl Strategy<Value = amp::ObjectId> {
    prop_oneof![
        Just(amp::ObjectId::Root),
        arb_opid().prop_map(amp::ObjectId::Id),
    ]
}

fn arb_elemid() -> impl Strategy<Value = amp::ElementId> {
    prop_oneof![
        Just(amp::ElementId::Head),
        arb_opid().prop_map(amp::ElementId::Id),
    ]
}

fn arb_key() -> impl Strategy<Value = amp::Key> {
    prop_oneof![
        any::<String>().prop_map(|s| amp::Key::Map(s.into())),
        arb_elemid().prop_map(amp::Key::Seq),
    ]
}

fn arb_changehash() -> impl Strategy<Value = amp::ChangeHash> {
    any::<[u8; 32]>().prop_map(amp::ChangeHash)
}

prop_compose! {
    fn arb_op()
        (insert in any::<bool>(),
         action in arb_optype(),
         obj in arb_objid(),
         key in arb_key(),
         pred in proptest::collection::vec(arb_opid(), 0..10)) -> amp::Op {
            amp::Op{
                action,
                obj,
                key,
                pred: SortedVec::from(pred),
                insert,
            }
    }
}

prop_compose! {
    fn arb_change()
            (seq in any::<u64>(),
             actor_id in arb_actorid(),
             start_op in any::<u64>(),
             time in any::<i64>(),
             message in proptest::option::of(any::<String>()),
             deps in proptest::collection::vec(arb_changehash(), 0..10),
             extra_bytes in proptest::collection::vec(any::<u8>(), 0..10),
             operations in proptest::collection::vec(arb_op(), 0..10)) -> amp::Change {
            amp::Change{
                seq,
                actor_id,
                start_op,
                time,
                hash: None,
                message,
                deps,
                operations,
                extra_bytes
            }
    }
}

enum Mode {
    Json,
    MessagePack,
}

/// We're roundtripping through json, which doesn't have a 32 bit float type or a uint type.
/// This means that inputs with f32 values will round trip into 64 bit floats, and any
/// positive i64's will round trip into u64's. This function performs that normalisation on an
/// existing change so  it can be compared with a round tripped change.
fn normalize_change(change: &amp::Change, mode: Mode) -> amp::Change {
    let mut result = change.clone();
    for op in result.operations.iter_mut() {
        let new_action = match &op.action {
            amp::OpType::Set(amp::ScalarValue::F32(f)) => {
                let deserialized: f64 = match mode {
                    Mode::Json => {
                        let serialized = serde_json::to_string(f).unwrap();
                        serde_json::from_str(&serialized).unwrap()
                    }
                    Mode::MessagePack => {
                        let serialized = rmp_serde::to_vec(&f).unwrap();
                        rmp_serde::from_slice(&serialized).unwrap()
                    }
                };
                amp::OpType::Set(amp::ScalarValue::F64(deserialized))
            }
            amp::OpType::Set(amp::ScalarValue::Int(i)) => {
                let val = if *i > 0 {
                    amp::ScalarValue::Uint((*i) as u64)
                } else {
                    amp::ScalarValue::Int(*i)
                };
                amp::OpType::Set(val)
            }
            //amp::OpType::Set(amp::ScalarValue::Uint(u)) => {
            //if *u > (i64::max_value() as u64) {
            //amp::OpType::Set(amp::ScalarValue::Uint(*u))
            //} else {
            //amp::OpType::Set(amp::ScalarValue::Int((*u).try_into().unwrap()))
            //}
            //}
            a => a.clone(),
        };
        op.action = new_action;
    }
    result
}

proptest! {
    #[test]
    fn test_round_trip_serialization_json(change in arb_change()) {
        let serialized = serde_json::to_string(&change)?;
        let deserialized: amp::Change = serde_json::from_str(&serialized)?;
        prop_assert_eq!(normalize_change(&change, Mode::Json), deserialized);
    }

    #[test]
    fn test_round_trip_serialization_msgpack(change in arb_change()) {
        let serialized = rmp_serde::to_vec_named(&change).unwrap();
        let deserialized: amp::Change = rmp_serde::from_slice(&serialized)?;
        prop_assert_eq!(normalize_change(&change, Mode::MessagePack), deserialized);
    }
}
