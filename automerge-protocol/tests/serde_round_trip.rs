extern crate automerge_protocol as amp;
use maplit::hashmap;

// This was not caught in the proptests
#[test]
fn test_msgpack_roundtrip_diff() {
    let actor = amp::ActorId::from_bytes("bd1850df21004038a8141a98473ff142".as_bytes());
    let diff = amp::RootDiff {
        props: hashmap! {
            "bird".into() => hashmap! {
                actor.op_id_at(1) => "magpie".into()
            }
        },
    };
    let serialized = rmp_serde::to_vec_named(&diff).unwrap();
    let deserialized: amp::RootDiff = rmp_serde::from_slice(&serialized).unwrap();
    assert_eq!(diff, deserialized);
}
