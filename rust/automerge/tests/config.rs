//! Construction intent is explicit; ordinary key-shaped authors stay unsigned.

use automerge::transaction::Transactable;
use automerge::{Author, AutoCommit, Automerge, Config, ReadDoc, TextEncoding, ROOT};
use ed25519_dalek::SigningKey;

fn author() -> Author<'static> {
    Author::from(
        SigningKey::from_bytes(&[7; 32])
            .verifying_key()
            .to_bytes()
            .to_vec(),
    )
}

#[test]
fn from_config_preserves_default_and_explicit_unsigned_choices() {
    let mut default = Automerge::from_config(Config::default());
    assert!(default.get_author().is_none());
    assert_eq!(default.text_encoding(), TextEncoding::platform_default());
    assert!(default.try_signing().is_err());
    for identity in [None, Some(Author::from(vec![1, 2, 3])), Some(author())] {
        let mut doc = Automerge::from_config(
            Config::default()
                .author(identity.clone())
                .text_encoding(TextEncoding::Utf16CodeUnit),
        );
        assert_eq!(doc.get_author(), identity.as_ref());
        assert_eq!(doc.text_encoding(), TextEncoding::Utf16CodeUnit);
        assert!(doc.try_signing().is_err());
        let mut tx = doc.transaction();
        tx.put(ROOT, "value", 1).unwrap();
        let hash = tx.commit().0.unwrap();
        assert_eq!(doc.get_change_by_hash(&hash).unwrap().author(), identity);
    }
}

#[test]
fn from_config_constructs_both_signing_document_types_infallibly() {
    let identity = author();
    let config = Config::default().signing(identity.clone()).unwrap();
    let mut doc: Automerge = Automerge::from_config(config);
    assert_eq!(doc.get_author(), Some(&identity));
    assert!(doc.try_signing().is_ok());
    assert!(doc.get_heads().is_empty());

    let mut auto = AutoCommit::from_config(
        Config::default()
            .text_encoding(TextEncoding::Utf8CodeUnit)
            .signing(identity.clone())
            .unwrap(),
    );
    assert_eq!(auto.text_encoding(), TextEncoding::Utf8CodeUnit);
    assert_eq!(auto.get_author(), Some(&identity));
    auto.put(ROOT, "value", 1).unwrap();
    assert_eq!(auto.try_signing().unwrap().required_signatures().len(), 1);
    assert_eq!(auto.get_changes(&[])[0].author(), Some(identity));
}
