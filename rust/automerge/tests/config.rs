//! Construction intent is explicit and survives configuration ownership.

use automerge::transaction::Transactable;
use automerge::{Author, AutoCommit, Automerge, Config, LoadOptions, ReadDoc, TextEncoding, ROOT};
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
fn defaults_are_unsigned_and_use_platform_encoding() {
    let mut doc = Automerge::from_config(Config::default());
    assert!(doc.get_author().is_none());
    assert_eq!(doc.text_encoding(), TextEncoding::platform_default());
    assert!(doc.signing_requests().is_err());
    assert!(!doc.has_unpublished_changes());
    assert!(Automerge::new_with_encoding(TextEncoding::Utf8CodeUnit)
        .get_author()
        .is_none());
    assert!(AutoCommit::new_with_encoding(TextEncoding::Utf8CodeUnit)
        .get_author()
        .is_none());
}

#[test]
fn unsigned_author_configuration_does_not_infer_signing_from_key_bytes() {
    for author in [None, Some(Author::from(vec![1, 2, 3])), Some(author())] {
        let mut doc = Automerge::from_config(
            Config::default()
                .author(author.clone())
                .text_encoding(TextEncoding::Utf16CodeUnit),
        );
        assert_eq!(doc.get_author(), author.as_ref());
        assert_eq!(doc.text_encoding(), TextEncoding::Utf16CodeUnit);
        assert!(doc.signing_requests().is_err());
        let mut tx = doc.transaction();
        tx.put(ROOT, "value", 1).unwrap();
        let hash = tx.commit().0.unwrap();
        assert_eq!(doc.get_change_by_hash(&hash).unwrap().author(), author);
    }
}

#[test]
fn signing_configuration_validates_once_and_constructs_without_an_error_channel() {
    let author = author();
    let config = Config::default().signing(author.clone()).unwrap();
    let mut doc: Automerge = Automerge::from_config(config);
    assert_eq!(doc.get_author(), Some(&author));
    assert!(doc.signing_requests().unwrap().is_empty());
    assert!(doc.get_heads().is_empty());

    let mut auto = AutoCommit::from_config(
        Config::default()
            .text_encoding(TextEncoding::Utf8CodeUnit)
            .signing(author.clone())
            .unwrap(),
    );
    assert_eq!(auto.get_author(), Some(&author));
    assert_eq!(auto.text_encoding(), TextEncoding::Utf8CodeUnit);
    auto.put(ROOT, "value", 1).unwrap();
    assert_eq!(auto.signing_requests().unwrap().len(), 1);
}

#[test]
fn load_options_establish_signing_without_a_separate_mode_flag() {
    let local = author();
    let mut doc =
        Automerge::load_with_options(&[], LoadOptions::default().signing(local.clone()).unwrap())
            .unwrap();
    assert_eq!(doc.get_author(), Some(&local));
    assert!(doc.signing_requests().unwrap().is_empty());
    assert!(LoadOptions::default()
        .signing(Author::from(vec![1, 2, 3]))
        .is_err());

    let mut unsigned = Automerge::load_with_options(
        &[],
        LoadOptions::default()
            .signing(local.clone())
            .unwrap()
            .author(local.clone()),
    )
    .unwrap();
    assert_eq!(unsigned.get_author(), Some(&local));
    assert!(unsigned.signing_requests().is_err());
}

#[test]
#[allow(deprecated)]
fn unsigned_load_migration_uses_the_configured_identity_before_creating_history() {
    let mut source = AutoCommit::new();
    source.put(ROOT, "legacy", "text").unwrap();
    let bytes = source.save();
    let source_actor = source.get_actor().clone();
    let original = source.get_changes(&[])[0].clone();
    let author = Author::from(vec![1, 2, 3]);
    let mut loaded = AutoCommit::load_with_options(
        &bytes,
        LoadOptions::default()
            .author(author.clone())
            .migrate_strings(automerge::StringMigration::ConvertToText),
    )
    .unwrap();
    assert_eq!(loaded.get_author_for_actor(&source_actor), None);
    assert_eq!(
        loaded
            .get_change_by_hash(&original.hash())
            .unwrap()
            .raw_bytes(),
        original.raw_bytes()
    );
    let migration = loaded.get_last_local_change().unwrap();
    assert_eq!(migration.author(), Some(author.clone()));
    let actor = migration.actor_id().clone();
    loaded.put(ROOT, "next", 2).unwrap();
    loaded.commit();
    assert_eq!(loaded.get_author_for_actor(&actor), Some(author));
    assert_eq!(source.save(), bytes);
}

#[test]
fn author_builders_replace_configuration_intent() {
    let mut unsigned =
        Automerge::from_config(Config::default().signing(author()).unwrap().author(None));
    assert!(unsigned.get_author().is_none());
    assert!(unsigned.signing_requests().is_err());

    let author = author();
    let mut signed = Automerge::from_config(
        Config::default()
            .author(Some(Author::from(vec![1])))
            .signing(author.clone())
            .unwrap(),
    );
    assert_eq!(signed.get_author(), Some(&author));
    assert!(signed.signing_requests().is_ok());
}
