use super::*;
use crate::signature::error::SigningError;
use crate::AutomergeError;

#[test]
fn arbitrary_bundles_reject_signing_mode_but_fragment_export_still_works() {
    let k = key(1);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 1);
    for hashes in [vec![], vec![a], vec![a, b]] {
        assert!(matches!(
            doc.bundle(hashes),
            Err(AutomergeError::Signing(SigningError::BundleNotSupported))
        ));
    }
    sign_doc(&mut doc, b, &k);
    assert!(matches!(
        doc.bundle([a, b]),
        Err(AutomergeError::Signing(SigningError::BundleNotSupported))
    ));
    let mut peer =
        Automerge::from_config(crate::Config::default().signing(author(&key(2))).unwrap());
    peer.load_incremental(&doc.bundle_fragments(doc.fragments(..)).concat())
        .unwrap();
    assert_eq!(peer.get_heads(), doc.get_heads());
    assert!(!peer.has_unpublished_changes());

    let mut unsigned = Automerge::new();
    unsigned.apply_changes(doc.get_changes(&[])).unwrap();
    let bundle = unsigned.bundle([a, b]).unwrap();
    assert_eq!(bundle.iter_changes().count(), 2);
    let mut target = Automerge::new();
    target.apply_changes([bundle.bytes()]).unwrap();
    assert_eq!(target.get_heads(), unsigned.get_heads());
}
