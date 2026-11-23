use super::{Config, DocumentAuthor};
use crate::{Author, TextEncoding};
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
fn intent_is_explicit_and_builders_replace_it() {
    let defaults = Config::default();
    assert!(matches!(defaults.author, DocumentAuthor::Unset));
    assert_eq!(defaults.text_encoding, TextEncoding::platform_default());
    let ordinary = Config::default().author(Some(author()));
    assert!(matches!(ordinary.author, DocumentAuthor::Set(_)));
    let signed = ordinary.signing(author()).unwrap();
    let DocumentAuthor::Verifier(verifier) = &signed.author else {
        panic!("prepared key")
    };
    assert_eq!(verifier.author(), &author());
    let unset = signed
        .author(None)
        .text_encoding(TextEncoding::Utf8CodeUnit);
    assert!(matches!(unset.author, DocumentAuthor::Unset));
    assert_eq!(unset.text_encoding, TextEncoding::Utf8CodeUnit);
    assert!(matches!(
        Config::default()
            .author(Some(Author::from(vec![1, 2, 3])))
            .author,
        DocumentAuthor::Set(_)
    ));
}

#[test]
fn invalid_and_weak_keys_fail_during_configuration() {
    let mut weak = vec![0; 32];
    weak[0] = 1;
    for bytes in [vec![1, 2, 3], weak] {
        assert!(Config::default().signing(Author::from(bytes)).is_err());
    }
}
