use crate::{transaction::Transactable, Automerge, Change, OpType, ReadDoc, ScalarValue, ROOT};

#[test]
fn prototype_gate_rejects_native_action_8_without_feature() {
    assert!(
        OpType::validate_action_and_value(8, &ScalarValue::Bytes(vec![1, 1, 0, b'a', 0, 0]))
            .is_err()
    );
    assert!(crate::op_set2::types::Action::try_from(8).is_err());
    // Actual bytes emitted by the feature-enabled control-only roundtrip fixture.
    let change=hex::decode("856f4a834cacdedd012a00011e0101000000061502340142025603570b70027f00017f087fb7010106006e6f626f647900007f00").unwrap();
    let document=hex::decode("856f4a83986816f9006901011e014cacdedde765488d595746d4775e68609111b6899b6575e98e31f56370e2bb5b0601020302130223024002560208150221022302340142025603570b8001027f007f017f017f007f007f077f007f007f01017f087fb7010106006e6f626f647900007f0000").unwrap();
    assert!(Change::from_bytes(change.clone()).is_err());
    assert!(Automerge::load(&change).is_err());
    assert!(Automerge::load(&document).is_err());
    // Legacy incremental receipt accepts a valid prefix before encountering action 8.
    // An envelope gate must run BEFORE invoking that API: universal fail-closed is false.
    let mut prefix = Automerge::new();
    let mut tx = prefix.transaction();
    tx.put(ROOT, "prefix", true).unwrap();
    tx.commit();
    let mut bytes = prefix.save();
    bytes.extend(change);
    let mut receiver = Automerge::new();
    let result = receiver.load_incremental(&bytes);
    assert!(result.is_ok());
    assert!(receiver.get(ROOT, "prefix").unwrap().is_some());
    assert_eq!(receiver.get_heads(), prefix.get_heads());
}
