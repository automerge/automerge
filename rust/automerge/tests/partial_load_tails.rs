//! Compatibility probes for malformed data after otherwise complete chunks.

use automerge::{
    transaction::Transactable, ActorId, Automerge, LoadOptions, OnPartialLoad, ReadDoc, ROOT,
};
use sha2::{Digest, Sha256};

fn document(actor: u8, key: &str) -> Automerge {
    let mut doc = Automerge::new().with_actor(ActorId::from(vec![actor]));
    let mut tx = doc.transaction();
    tx.put(ROOT, key, 1).unwrap();
    tx.commit();
    doc
}

fn incomplete_prefixes() -> Vec<(String, Vec<u8>)> {
    let doc = document(9, "unused");
    let change = doc.get_last_local_change().unwrap();
    // The type is at offset 8. None of these prefixes identifies the next
    // chunk, but all are genuine prefixes of a valid change chunk.
    (1..=8)
        .map(|len| {
            (
                format!("prefix length {len}"),
                change.raw_bytes()[..len].to_vec(),
            )
        })
        .collect()
}

fn framed(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut hashed = vec![tag];
    leb128::write::unsigned(&mut hashed, body.len() as u64).unwrap();
    hashed.extend_from_slice(body);
    let hash = Sha256::digest(&hashed);
    let mut bytes = vec![0x85, 0x6f, 0x4a, 0x83];
    bytes.extend_from_slice(&hash[..4]);
    bytes.extend_from_slice(&hashed);
    bytes
}

fn with_optional_signatures(tails: Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
    let signatures = framed(4, &[0]);
    tails
        .into_iter()
        .flat_map(|(label, tail)| {
            let signed_tail = [signatures.as_slice(), tail.as_slice()].concat();
            [
                (format!("without signatures, {label}"), tail),
                (format!("with signatures, {label}"), signed_tail),
            ]
        })
        .collect()
}

fn signature_chunk(hash: automerge::ChangeHash) -> Vec<u8> {
    let mut body = vec![1]; // One signature entry.
    body.extend_from_slice(&hash.0);
    body.extend_from_slice(&[1, 3, 7, 8, 9]); // Tag, length, and opaque payload.
    framed(4, &body)
}

fn identified_truncations(bytes: &[u8]) -> Vec<(&'static str, &[u8])> {
    vec![
        ("type present, length missing", &bytes[..9]),
        ("length present, body missing", &bytes[..10]),
        ("signature payload truncated", &bytes[..bytes.len() - 1]),
    ]
}

fn unknown_chunks() -> Vec<(String, Vec<u8>)> {
    [5u8, 255]
        .into_iter()
        .map(|tag| {
            // Complete framing with a correct checksum and an empty body.
            // This probes error recovery, not support for decoding unknown types.
            (format!("unknown tag {tag}"), framed(tag, &[]))
        })
        .collect()
}

fn assert_snapshot_recovery(tails: Vec<(String, Vec<u8>)>) {
    let doc = document(1, "snapshot");
    let snapshot = doc.save();
    let mut failures = Vec::new();
    for (label, tail) in with_optional_signatures(tails) {
        let stream = [snapshot.as_slice(), tail.as_slice()].concat();
        assert!(
            Automerge::load_with_options(
                &stream,
                LoadOptions::new().on_partial_load(OnPartialLoad::Error),
            )
            .is_err(),
            "strict loading must reject {label}",
        );
        match Automerge::load_with_options(
            &stream,
            LoadOptions::new().on_partial_load(OnPartialLoad::Ignore),
        ) {
            Ok(recovered) => {
                assert_eq!(recovered.get_heads(), doc.get_heads(), "{label}");
                assert_eq!(
                    recovered.get(ROOT, "snapshot").unwrap().unwrap().0.as_i64(),
                    Some(1),
                    "{label}",
                );
            }
            Err(error) => failures.push(format!("{label}: {error}")),
        }
    }
    assert!(
        failures.is_empty(),
        "snapshot recovery failed:\n{}",
        failures.join("\n")
    );
}

fn assert_incremental_recovery(tails: Vec<(String, Vec<u8>)>) {
    let source = document(2, "incoming");
    let incoming = source.get_last_local_change().unwrap();
    let mut failures = Vec::new();
    for (label, tail) in with_optional_signatures(tails) {
        // A nonempty target exercises the incremental adapter, not initial load.
        let mut target = document(1, "existing");
        let stream = [incoming.raw_bytes(), tail.as_slice()].concat();
        match target.load_incremental(&stream) {
            Ok(added) => {
                if added != 1 || target.get_change_by_hash(&incoming.hash()).is_none() {
                    failures.push(format!(
                        "{label}: added {added} operations, incoming change retained: {}",
                        target.get_change_by_hash(&incoming.hash()).is_some()
                    ));
                } else {
                    assert_eq!(
                        target.get(ROOT, "incoming").unwrap().unwrap().0.as_i64(),
                        Some(1)
                    );
                }
                assert_eq!(
                    target.get(ROOT, "existing").unwrap().unwrap().0.as_i64(),
                    Some(1)
                );
            }
            Err(error) => failures.push(format!("{label}: {error}")),
        }
    }
    assert!(
        failures.is_empty(),
        "incremental recovery failed:\n{}",
        failures.join("\n")
    );
}

#[test]
fn identified_truncated_signatures_invalidate_snapshot_under_both_policies() {
    let doc = document(1, "snapshot");
    let snapshot = doc.save();
    let signatures = signature_chunk(doc.get_heads()[0]);
    // Pin that the complete fixture is structurally valid before truncating it.
    let complete = [snapshot.as_slice(), signatures.as_slice()].concat();
    assert_eq!(
        Automerge::load(&complete).unwrap().get_heads(),
        doc.get_heads()
    );

    for (label, truncated) in identified_truncations(&signatures) {
        let stream = [snapshot.as_slice(), truncated].concat();
        for policy in [OnPartialLoad::Error, OnPartialLoad::Ignore] {
            assert!(
                Automerge::load_with_options(&stream, LoadOptions::new().on_partial_load(policy))
                    .is_err(),
                "identified signatures must invalidate snapshot: {label}, {policy:?}",
            );
        }
    }
}

#[test]
fn identified_truncated_signatures_withhold_incremental_change() {
    let source = document(2, "incoming");
    let incoming = source.get_last_local_change().unwrap();
    let signatures = signature_chunk(incoming.hash());
    let mut control = document(1, "existing");
    let complete = [incoming.raw_bytes(), signatures.as_slice()].concat();
    assert_eq!(control.load_incremental(&complete).unwrap(), 1);
    assert!(control.get_change_by_hash(&incoming.hash()).is_some());

    for (label, truncated) in identified_truncations(&signatures) {
        let mut target = document(1, "existing");
        let original_heads = target.get_heads();
        let stream = [incoming.raw_bytes(), truncated].concat();
        assert_eq!(target.load_incremental(&stream).unwrap(), 0, "{label}");
        assert_eq!(target.get_heads(), original_heads, "{label}");
        assert!(
            target.get_change_by_hash(&incoming.hash()).is_none(),
            "{label}"
        );
        assert!(target.get(ROOT, "incoming").unwrap().is_none(), "{label}");
        assert_eq!(
            target.get(ROOT, "existing").unwrap().unwrap().0.as_i64(),
            Some(1),
            "{label}"
        );
    }
}

#[test]
fn snapshot_ignore_recovers_before_incomplete_prefix() {
    assert_snapshot_recovery(incomplete_prefixes());
}

#[test]
fn snapshot_ignore_recovers_before_unknown_chunk() {
    assert_snapshot_recovery(unknown_chunks());
}

#[test]
fn incremental_load_retains_change_before_incomplete_prefix() {
    assert_incremental_recovery(incomplete_prefixes());
}

#[test]
fn incremental_load_retains_change_before_unknown_chunk() {
    assert_incremental_recovery(unknown_chunks());
}
