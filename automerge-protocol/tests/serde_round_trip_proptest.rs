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

proptest! {
    #[test]
    fn test_round_trip_serialization_json(change in arb_change()) {
        let serialized = serde_json::to_string(&change)?;
        let deserialized: amp::UncompressedChange = serde_json::from_str(&serialized)?;
        prop_assert_eq!(change, deserialized);
    }

    #[test]
    fn test_round_trip_serialization_msgpack(change in arb_change()) {
        let serialized = rmp_serde::to_vec_named(&change).unwrap();
        let deserialized: amp::UncompressedChange = rmp_serde::from_slice(&serialized)?;
        prop_assert_eq!(change, deserialized);
    }
}
