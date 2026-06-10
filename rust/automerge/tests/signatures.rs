use automerge::marks::{ExpandMark, Mark};
use automerge::sync::{
    ChunkList, Message, MessageFlags, MessageVersion, State as SyncState, SyncDoc,
};
use automerge::transaction::Transactable;
use automerge::{
    ActorId, Author, AutoCommit, Automerge, AutomergeError, LoadOptions, ObjType, ReadDoc,
    ScalarValue, SignatureError, SignatureState, Value, ROOT,
};

fn author() -> Author {
    Author::from(vec![7; 32])
}

fn other_author() -> Author {
    Author::from(vec![8; 32])
}

fn sign_pending(doc: &mut AutoCommit, byte: u8) {
    let mut signatures = SignatureState::new();
    doc.reconcile_signatures(&mut signatures).unwrap();
    let hashes = signatures
        .pending_signing_requests()
        .map(|request| request.hash())
        .collect::<Vec<_>>();
    assert!(!hashes.is_empty());
    for hash in hashes {
        signatures.complete_signing(hash, vec![byte; 64]);
    }
    doc.reconcile_signatures(&mut signatures).unwrap();
}

fn load_signed_accepting_all(saved: &[u8]) -> AutoCommit {
    let mut doc = AutoCommit::load_with_options(saved, LoadOptions::new().signing()).unwrap();
    let mut signatures = SignatureState::new();
    loop {
        doc.reconcile_signatures(&mut signatures).unwrap();
        let requests = signatures
            .pending_verification_requests()
            .cloned()
            .collect::<Vec<_>>();
        if requests.is_empty() {
            break;
        }
        for request in requests {
            signatures.complete_verification(request.id(), true);
        }
    }
    doc.reconcile_signatures(&mut signatures).unwrap();
    doc
}

#[test]
fn normal_save_filters_signing_incomplete_successors() {
    let mut doc = AutoCommit::new().with_author(Some(author())).with_signing();
    doc.put(ROOT, "key", "one").unwrap();
    let first = doc.get_heads()[0];
    let mut signatures = SignatureState::new();
    doc.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(first, vec![34; 64]);
    doc.reconcile_signatures(&mut signatures).unwrap();

    doc.put(ROOT, "key", "two").unwrap();
    let saved = doc.save();
    let mut loaded = AutoCommit::load_with_options(&saved, LoadOptions::new().signing()).unwrap();
    let mut verifier = SignatureState::new();
    loaded.reconcile_signatures(&mut verifier).unwrap();
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(request.hash(), first);
    verifier.complete_verification(request.id(), true);
    loaded.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(
        loaded.get(ROOT, "key").unwrap().unwrap().0,
        Value::str("one")
    );
}

#[test]
fn normal_save_filters_signing_incomplete_local_changes() {
    let mut doc = AutoCommit::new().with_author(Some(author())).with_signing();
    doc.put(ROOT, "key", "value").unwrap();

    let saved = doc.save();
    let loaded = AutoCommit::load_with_options(&saved, LoadOptions::new().signing()).unwrap();
    assert_eq!(loaded.get(ROOT, "key").unwrap(), None);

    sign_pending(&mut doc, 33);
    let saved = doc.save();
    let loaded = load_signed_accepting_all(&saved);
    assert_eq!(
        loaded.get(ROOT, "key").unwrap().unwrap().0,
        Value::str("value")
    );
}

#[test]
fn normal_save_filters_unsigned_list_suffix() {
    let mut doc = AutoCommit::new().with_author(Some(author())).with_signing();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, "one").unwrap();
    doc.insert(&list, 1, "two").unwrap();
    sign_pending(&mut doc, 35);

    doc.insert(&list, 2, "three").unwrap();
    let loaded = load_signed_accepting_all(&doc.save());
    let (_, loaded_list) = loaded.get(ROOT, "list").unwrap().unwrap();
    assert_eq!(loaded.length(&loaded_list), 2);
    assert_eq!(
        loaded.get(&loaded_list, 0).unwrap().unwrap().0,
        Value::str("one")
    );
    assert_eq!(
        loaded.get(&loaded_list, 1).unwrap().unwrap().0,
        Value::str("two")
    );
}

#[test]
fn normal_save_filters_unsigned_text_suffix() {
    let mut doc = AutoCommit::new().with_author(Some(author())).with_signing();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello").unwrap();
    sign_pending(&mut doc, 36);

    doc.splice_text(&text, 5, 0, " world").unwrap();
    let loaded = load_signed_accepting_all(&doc.save());
    let (_, loaded_text) = loaded.get(ROOT, "text").unwrap().unwrap();
    assert_eq!(loaded.text(&loaded_text).unwrap(), "hello");
}

#[test]
fn normal_save_filters_unsigned_mark() {
    let mut doc = AutoCommit::new().with_author(Some(author())).with_signing();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello").unwrap();
    sign_pending(&mut doc, 40);

    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, "hello".len()),
        ExpandMark::Both,
    )
    .unwrap();
    let loaded = load_signed_accepting_all(&doc.save());
    let (_, loaded_text) = loaded.get(ROOT, "text").unwrap().unwrap();
    assert_eq!(loaded.text(&loaded_text).unwrap(), "hello");
    assert!(loaded.marks(&loaded_text).unwrap().is_empty());
}

#[test]
fn normal_save_filters_unsigned_unmark() {
    let mut doc = AutoCommit::new().with_author(Some(author())).with_signing();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, "hello".len()),
        ExpandMark::Both,
    )
    .unwrap();
    sign_pending(&mut doc, 41);

    doc.unmark(&text, "bold", 0, "hello".len(), ExpandMark::Before)
        .unwrap();
    let loaded = load_signed_accepting_all(&doc.save());
    let (_, loaded_text) = loaded.get(ROOT, "text").unwrap().unwrap();
    let marks = loaded.marks(&loaded_text).unwrap();
    assert_eq!(marks.len(), 1);
    assert_eq!(marks[0].start, 0);
    assert_eq!(marks[0].end, "hello".len());
    assert_eq!(marks[0].name(), "bold");
    assert_eq!(marks[0].value(), &ScalarValue::from(true));
}

#[test]
fn normal_save_filters_unsigned_object_creation_and_descendants() {
    let mut doc = AutoCommit::new().with_author(Some(author())).with_signing();
    doc.put(ROOT, "stable", "signed").unwrap();
    sign_pending(&mut doc, 37);

    let nested = doc.put_object(ROOT, "nested", ObjType::Map).unwrap();
    doc.put(&nested, "key", "unsigned descendant").unwrap();
    let loaded = load_signed_accepting_all(&doc.save());
    assert_eq!(
        loaded.get(ROOT, "stable").unwrap().unwrap().0,
        Value::str("signed")
    );
    assert_eq!(loaded.get(ROOT, "nested").unwrap(), None);
}

#[test]
fn normal_save_filters_unsigned_counter_increment() {
    let mut doc = AutoCommit::new().with_author(Some(author())).with_signing();
    doc.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
    sign_pending(&mut doc, 38);

    doc.increment(ROOT, "counter", 2).unwrap();
    let loaded = load_signed_accepting_all(&doc.save());
    assert_eq!(
        loaded.get(ROOT, "counter").unwrap().unwrap().0,
        Value::counter(1)
    );
}

#[test]
fn normal_save_keeps_verified_remote_change_while_filtering_local_unsigned() {
    let mut doc = AutoCommit::new().with_author(Some(author())).with_signing();
    doc.put(ROOT, "local", "signed").unwrap();
    sign_pending(&mut doc, 39);

    let mut remote = AutoCommit::new().with_author(Some(other_author()));
    remote.apply_changes(doc.get_changes(&[])).unwrap();
    remote.put(ROOT, "remote", "verified").unwrap();
    let remote_changes = remote.get_changes(&doc.get_heads());
    doc.apply_changes(remote_changes).unwrap();
    let mut verifier = SignatureState::new();
    doc.reconcile_signatures(&mut verifier).unwrap();
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    verifier.complete_verification(request.id(), true);
    doc.reconcile_signatures(&mut verifier).unwrap();

    doc.put(ROOT, "unsigned", "local").unwrap();
    let loaded = load_signed_accepting_all(&doc.save());
    assert_eq!(
        loaded.get(ROOT, "local").unwrap().unwrap().0,
        Value::str("signed")
    );
    assert_eq!(
        loaded.get(ROOT, "remote").unwrap().unwrap().0,
        Value::str("verified")
    );
    assert_eq!(loaded.get(ROOT, "unsigned").unwrap(), None);
}

#[test]
fn filtered_signed_save_can_be_mutated_after_load() {
    let mut doc = AutoCommit::new().with_author(Some(author())).with_signing();
    let map = doc.put_object(ROOT, "map", ObjType::Map).unwrap();
    let list = doc.put_object(&map, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, "signed").unwrap();
    sign_pending(&mut doc, 42);

    doc.insert(&list, 1, "unsigned").unwrap();
    let mut loaded = load_signed_accepting_all(&doc.save());
    let (_, loaded_map) = loaded.get(ROOT, "map").unwrap().unwrap();
    let (_, loaded_list) = loaded.get(&loaded_map, "list").unwrap().unwrap();
    assert_eq!(loaded.length(&loaded_list), 1);

    loaded.set_author(Some(author()));
    loaded.put(&loaded_map, "after_load", "ok").unwrap();
    loaded.insert(&loaded_list, 1, "mutated").unwrap();
    assert_eq!(
        loaded.get(&loaded_map, "after_load").unwrap().unwrap().0,
        Value::str("ok")
    );
    assert_eq!(loaded.length(&loaded_list), 2);
}

#[test]
fn filtered_signed_save_tolerates_unused_actor_from_omitted_change() {
    let mut doc = AutoCommit::new()
        .with_actor(ActorId::from([1]))
        .with_author(Some(author()))
        .with_signing();
    doc.put(ROOT, "signed", "value").unwrap();
    sign_pending(&mut doc, 43);

    doc.set_actor(ActorId::from([2]));
    doc.put(ROOT, "unsigned", "omitted").unwrap();

    let loaded = load_signed_accepting_all(&doc.save());
    assert_eq!(
        loaded.get(ROOT, "signed").unwrap().unwrap().0,
        Value::str("value")
    );
    assert_eq!(loaded.get(ROOT, "unsigned").unwrap(), None);
}

#[test]
fn signed_document_reconciliation_gates_save() {
    let author = author();
    let mut doc = AutoCommit::new()
        .with_author(Some(author.clone()))
        .with_signing();

    doc.put(ROOT, "key", "value").unwrap();
    let heads = doc.get_heads();
    assert_eq!(heads.len(), 1);

    let err = doc.try_save_signed().unwrap_err();
    match err {
        AutomergeError::Signature(SignatureError::MissingSignature { hash }) => {
            assert_eq!(hash, heads[0]);
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let mut signatures = SignatureState::new();
    let report = doc.reconcile_signatures(&mut signatures).unwrap();
    assert_eq!(report.signing_requested, 1);
    assert_eq!(report.signatures_attached, 0);

    let requests = signatures.pending_signing_requests().collect::<Vec<_>>();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].hash(), heads[0]);
    assert_eq!(requests[0].author(), &author);
    assert!(requests[0]
        .bytes_to_sign()
        .starts_with(b"automerge-change-signature-v1"));
    assert!(requests[0].bytes_to_sign().ends_with(heads[0].as_ref()));

    signatures.complete_signing(heads[0], vec![42; 64]);
    let report = doc.reconcile_signatures(&mut signatures).unwrap();
    assert_eq!(report.signing_requested, 0);
    assert_eq!(report.signatures_attached, 1);
    assert!(doc.missing_signature_hashes().is_empty());

    let saved = doc.try_save_signed().unwrap();
    assert!(!saved.is_empty());
}

#[test]
fn non_signing_documents_do_not_emit_requests() {
    let mut doc = AutoCommit::new().with_author(Some(author()));
    doc.put(ROOT, "key", "value").unwrap();

    let mut signatures = SignatureState::new();
    let report = doc.reconcile_signatures(&mut signatures).unwrap();
    assert_eq!(report.signing_requested, 0);
    assert_eq!(signatures.pending_signing_requests().count(), 0);
    assert!(!doc.save().is_empty());
}

#[test]
fn load_options_construct_signed_documents() {
    let author = author();
    let mut doc =
        Automerge::load_with_options(&[], LoadOptions::new().author(author.clone()).signing())
            .unwrap();

    let mut tx = doc.transaction();
    tx.put(ROOT, "key", "value").unwrap();
    tx.commit();

    let missing = doc.missing_signature_hashes();
    assert_eq!(missing.len(), 1);
}

#[test]
fn signed_apply_requires_verification_before_visibility() {
    let author = author();
    let mut source = AutoCommit::new().with_author(Some(author));
    source.put(ROOT, "key", "value").unwrap();
    let change = source.get_changes(&[]).pop().unwrap();

    let mut remote = AutoCommit::new().with_signing();
    remote.apply_changes([change.clone()]).unwrap();
    assert!(remote.get(ROOT, "key").unwrap().is_none());

    let mut signatures = SignatureState::new();
    let report = remote.reconcile_signatures(&mut signatures).unwrap();
    assert_eq!(report.verification_requested, 1);
    assert!(remote.get(ROOT, "key").unwrap().is_none());

    let request = signatures
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(request.hash(), change.hash());
    assert!(request
        .bytes_to_verify()
        .starts_with(b"automerge-change-signature-v1"));
    assert!(request.bytes_to_verify().ends_with(change.hash().as_ref()));
    assert!(request.signature().is_none());

    signatures.complete_verification(request.id(), true);
    let report = remote.reconcile_signatures(&mut signatures).unwrap();
    assert_eq!(report.verification_accepted, 1);
    assert_eq!(
        remote.get(ROOT, "key").unwrap().unwrap().0,
        Value::str("value")
    );
}

#[test]
fn verified_same_author_child_verifies_ancestors() {
    let author = author();
    let mut source = AutoCommit::new().with_author(Some(author));
    source.put(ROOT, "first", "one").unwrap();
    source.commit();
    source.put(ROOT, "second", "two").unwrap();
    source.commit();
    let changes = source.get_changes(&[]);
    assert_eq!(changes.len(), 2);

    let mut remote = AutoCommit::new().with_signing();
    remote.apply_changes(changes.clone()).unwrap();
    let mut signatures = SignatureState::new();
    let report = remote.reconcile_signatures(&mut signatures).unwrap();
    assert_eq!(report.verification_requested, 1);

    let request = signatures
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(request.hash(), changes[1].hash());
    signatures.complete_verification(request.id(), true);

    let report = remote.reconcile_signatures(&mut signatures).unwrap();
    assert_eq!(report.verification_accepted, 1);
    assert_eq!(
        remote.get(ROOT, "first").unwrap().unwrap().0,
        Value::str("one")
    );
    assert_eq!(
        remote.get(ROOT, "second").unwrap().unwrap().0,
        Value::str("two")
    );
}

#[test]
fn rejected_signed_changes_remain_invisible() {
    let mut source = AutoCommit::new().with_author(Some(author()));
    source.put(ROOT, "key", "value").unwrap();
    let change = source.get_changes(&[]).pop().unwrap();

    let mut remote = AutoCommit::new().with_signing();
    let mut signatures = SignatureState::new();
    remote.apply_changes([change]).unwrap();
    remote.reconcile_signatures(&mut signatures).unwrap();
    let request = signatures
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();

    signatures.complete_verification(request.id(), false);
    let report = remote.reconcile_signatures(&mut signatures).unwrap();
    assert_eq!(report.verification_rejected, 1);
    assert!(remote.get(ROOT, "key").unwrap().is_none());
}

#[test]
fn signed_get_changes_is_gated() {
    let author = author();
    let mut doc = AutoCommit::new()
        .with_author(Some(author.clone()))
        .with_signing();

    doc.put(ROOT, "key", "value").unwrap();
    assert!(doc.get_changes(&[]).is_empty());
    assert!(doc.get_signed_changes(&[]).unwrap().is_empty());

    let hash = doc.get_heads()[0];
    let mut signatures = SignatureState::new();
    doc.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(hash, vec![1; 64]);
    doc.reconcile_signatures(&mut signatures).unwrap();

    let changes = doc.get_changes(&[]);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].hash(), hash);
    assert_eq!(changes[0].signature().unwrap().as_bytes(), &[1; 64]);
}

#[test]
fn signed_get_changes_hides_signed_descendant_until_unsigned_ancestor_exportable() {
    let mut doc = AutoCommit::new().with_author(Some(author())).with_signing();
    doc.put(ROOT, "first", "one").unwrap();
    doc.commit();
    doc.put(ROOT, "second", "two").unwrap();
    doc.commit();
    let head = doc.get_heads()[0];

    let mut signatures = SignatureState::new();
    doc.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(head, vec![2; 64]);
    doc.reconcile_signatures(&mut signatures).unwrap();

    assert!(doc.get_changes(&[]).is_empty());
}

#[test]
fn signed_standalone_changes_carry_detached_signatures() {
    let mut source = AutoCommit::new().with_author(Some(author())).with_signing();
    source.put(ROOT, "key", "value").unwrap();
    let hash = source.get_heads()[0];

    let mut signatures = SignatureState::new();
    source.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(hash, vec![9; 64]);
    source.reconcile_signatures(&mut signatures).unwrap();

    let signed_change = source.get_signed_changes(&[]).unwrap().pop().unwrap();
    assert_eq!(signed_change.hash(), hash);
    assert_eq!(signed_change.signature().unwrap().as_bytes(), &[9; 64]);

    let reparsed = automerge::Change::try_from(signed_change.raw_bytes()).unwrap();
    assert_eq!(reparsed.hash(), hash);
    assert_eq!(reparsed.signature().unwrap().as_bytes(), &[9; 64]);

    let mut remote = AutoCommit::new().with_signing();
    remote.apply_changes([reparsed]).unwrap();
    let mut verifier = SignatureState::new();
    remote.reconcile_signatures(&mut verifier).unwrap();
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(request.hash(), hash);
    assert_eq!(request.signature().unwrap().as_bytes(), &[9; 64]);
}

#[test]
fn bundle_persists_signatures_by_change_row() {
    let mut source = AutoCommit::new().with_author(Some(author())).with_signing();
    source.put(ROOT, "key", "value").unwrap();
    let hash = source.get_heads()[0];

    let mut signatures = SignatureState::new();
    source.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(hash, vec![13; 64]);
    source.reconcile_signatures(&mut signatures).unwrap();

    let bundle = source.bundle([hash]).unwrap();
    let changes = bundle.to_changes().unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].hash(), hash);
    assert_eq!(changes[0].signature().unwrap().as_bytes(), &[13; 64]);
}

#[test]
fn rejected_signed_document_load_remains_invisible() {
    let mut source = AutoCommit::new().with_author(Some(author())).with_signing();
    source.put(ROOT, "key", "value").unwrap();
    let head = source.get_heads()[0];

    let mut signatures = SignatureState::new();
    source.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(head, vec![21; 64]);
    source.reconcile_signatures(&mut signatures).unwrap();

    let saved = source.try_save_signed().unwrap();
    let mut loaded = Automerge::load_with_options(&saved, LoadOptions::new().signing()).unwrap();
    let mut verifier = SignatureState::new();
    loaded.reconcile_signatures(&mut verifier).unwrap();
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    verifier.complete_verification(request.id(), false);
    let report = loaded.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_rejected, 1);
    assert!(loaded.get(ROOT, "key").unwrap().is_none());
}

#[test]
fn signed_load_incremental_from_empty_doc_waits_for_verification() {
    let mut source = AutoCommit::new().with_author(Some(author())).with_signing();
    source.put(ROOT, "first", "one").unwrap();
    source.commit();
    source.put(ROOT, "second", "two").unwrap();
    source.commit();
    let head = source.get_heads()[0];

    let mut signatures = SignatureState::new();
    source.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(head, vec![31; 64]);
    source.reconcile_signatures(&mut signatures).unwrap();
    let saved = source.try_save_signed().unwrap();

    let mut loaded = AutoCommit::new().with_signing();
    loaded.load_incremental(&saved).unwrap();
    assert_eq!(loaded.get(ROOT, "first").unwrap(), None);
    assert_eq!(loaded.get(ROOT, "second").unwrap(), None);

    let mut verifier = SignatureState::new();
    let report = loaded.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_requested, 1);
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(request.hash(), head);
    assert_eq!(request.signature().unwrap().as_bytes(), &[31; 64]);

    verifier.complete_verification(request.id(), true);
    let report = loaded.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_accepted, 1);
    assert_eq!(
        loaded.get(ROOT, "first").unwrap().unwrap().0,
        Value::str("one")
    );
    assert_eq!(
        loaded.get(ROOT, "second").unwrap().unwrap().0,
        Value::str("two")
    );
}

#[test]
fn signed_load_incremental_into_existing_doc_waits_for_verification() {
    let mut source = AutoCommit::new().with_author(Some(author())).with_signing();
    source.put(ROOT, "base", "one").unwrap();
    source.commit();
    let base_head = source.get_heads()[0];

    let mut signatures = SignatureState::new();
    source.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(base_head, vec![41; 64]);
    source.reconcile_signatures(&mut signatures).unwrap();

    let mut target = AutoCommit::new().with_signing();
    target
        .load_incremental(&source.try_save_signed().unwrap())
        .unwrap();
    let mut verifier = SignatureState::new();
    target.reconcile_signatures(&mut verifier).unwrap();
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    verifier.complete_verification(request.id(), true);
    target.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(
        target.get(ROOT, "base").unwrap().unwrap().0,
        Value::str("one")
    );

    source.put(ROOT, "next", "two").unwrap();
    source.commit();
    let next_head = source.get_heads()[0];
    source.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(next_head, vec![42; 64]);
    source.reconcile_signatures(&mut signatures).unwrap();

    target
        .load_incremental(&source.try_save_signed().unwrap())
        .unwrap();
    assert_eq!(
        target.get(ROOT, "base").unwrap().unwrap().0,
        Value::str("one")
    );
    assert_eq!(target.get(ROOT, "next").unwrap(), None);

    let mut verifier = SignatureState::new();
    let report = target.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_requested, 1);
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(request.hash(), next_head);
    assert_eq!(request.signature().unwrap().as_bytes(), &[42; 64]);

    verifier.complete_verification(request.id(), true);
    target.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(
        target.get(ROOT, "next").unwrap().unwrap().0,
        Value::str("two")
    );
}

#[test]
fn signed_load_incremental_standalone_change_uses_attached_signature() {
    let mut source = AutoCommit::new().with_author(Some(author())).with_signing();
    source.put(ROOT, "key", "value").unwrap();
    let head = source.get_heads()[0];

    let mut signatures = SignatureState::new();
    source.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(head, vec![51; 64]);
    source.reconcile_signatures(&mut signatures).unwrap();
    let signed_change = source.get_signed_changes(&[]).unwrap().pop().unwrap();

    let mut target = AutoCommit::new().with_signing();
    target.load_incremental(signed_change.raw_bytes()).unwrap();
    assert_eq!(target.get(ROOT, "key").unwrap(), None);

    let mut verifier = SignatureState::new();
    target.reconcile_signatures(&mut verifier).unwrap();
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(request.hash(), head);
    assert_eq!(request.signature().unwrap().as_bytes(), &[51; 64]);
    verifier.complete_verification(request.id(), true);
    target.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(
        target.get(ROOT, "key").unwrap().unwrap().0,
        Value::str("value")
    );
}

#[test]
fn signed_sync_acknowledges_received_unsigned_changes() {
    let mut source = AutoCommit::new().with_author(Some(author()));
    let mut remote = AutoCommit::new().with_signing();
    let mut source_state = SyncState::new();
    let mut remote_state = SyncState::new();

    source.put(ROOT, "key", "value").unwrap();

    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .expect("initial sync should advertise heads");
    remote
        .sync()
        .receive_sync_message(&mut remote_state, message)
        .unwrap();

    let response = remote
        .sync()
        .generate_sync_message(&mut remote_state)
        .expect("signed peer should request missing heads");
    source
        .sync()
        .receive_sync_message(&mut source_state, response)
        .unwrap();

    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .expect("unsigned peer should send requested change");
    assert!(!message.changes.is_empty());
    remote
        .sync()
        .receive_sync_message(&mut remote_state, message)
        .unwrap();
    assert_eq!(remote.get(ROOT, "key").unwrap(), None);

    let response = remote
        .sync()
        .generate_sync_message(&mut remote_state)
        .expect("signed peer should acknowledge received pending heads");
    source
        .sync()
        .receive_sync_message(&mut source_state, response)
        .unwrap();

    let message = source.sync().generate_sync_message(&mut source_state);
    if let Some(message) = message {
        assert!(message.changes.is_empty());
    }
}

#[test]
fn signed_sync_uses_exportable_heads() {
    let mut source = AutoCommit::new().with_author(Some(author())).with_signing();
    let mut remote = AutoCommit::new().with_signing();
    let mut source_state = SyncState::new();
    let mut remote_state = SyncState::new();

    source.put(ROOT, "key", "value").unwrap();
    let head = source.get_heads()[0];

    if let Some(message) = source.sync().generate_sync_message(&mut source_state) {
        assert!(message.heads.is_empty());
        assert!(message.changes.is_empty());
        remote
            .sync()
            .receive_sync_message(&mut remote_state, message)
            .unwrap();
    }
    if let Some(message) = remote.sync().generate_sync_message(&mut remote_state) {
        source
            .sync()
            .receive_sync_message(&mut source_state, message)
            .unwrap();
    }
    assert_eq!(remote.get(ROOT, "key").unwrap(), None);

    let mut signatures = SignatureState::new();
    source.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(head, vec![61; 64]);
    source.reconcile_signatures(&mut signatures).unwrap();

    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .expect("signed change should now be exportable");
    remote
        .sync()
        .receive_sync_message(&mut remote_state, message)
        .unwrap();
    assert_eq!(remote.get(ROOT, "key").unwrap(), None);

    let mut verifier = SignatureState::new();
    let report = remote.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_requested, 1);
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(request.hash(), head);
    assert_eq!(request.signature().unwrap().as_bytes(), &[61; 64]);

    verifier.complete_verification(request.id(), true);
    remote.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(
        remote.get(ROOT, "key").unwrap().unwrap().0,
        Value::str("value")
    );
}

#[test]
fn signed_sync_sends_proof_bundle_for_retained_frontier_signature() {
    let mut source = AutoCommit::new().with_author(Some(author())).with_signing();
    let mut remote = AutoCommit::new().with_signing();
    let mut source_state = SyncState::new();
    let mut remote_state = SyncState::new();

    source.put(ROOT, "first", "one").unwrap();
    let _first = source.get_heads()[0];
    source.put(ROOT, "second", "two").unwrap();
    let second = source.get_heads()[0];

    let mut signer = SignatureState::new();
    let report = source.reconcile_signatures(&mut signer).unwrap();
    assert_eq!(report.signing_requested, 1);
    assert_eq!(
        signer.pending_signing_requests().next().unwrap().hash(),
        second
    );
    signer.complete_signing(second, vec![81; 64]);
    source.reconcile_signatures(&mut signer).unwrap();
    assert!(source.missing_signature_hashes().is_empty());

    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .expect("initial signed sync hello should be sent");
    assert_eq!(message.heads, vec![second]);
    assert!(message.changes.is_empty());
    remote
        .sync()
        .receive_sync_message(&mut remote_state, message)
        .unwrap();

    let response = remote
        .sync()
        .generate_sync_message(&mut remote_state)
        .expect("receiver should request advertised signed head and advertise v2 support");
    source
        .sync()
        .receive_sync_message(&mut source_state, response)
        .unwrap();

    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .expect("source should send proof bundle after v2 negotiation");
    assert_eq!(message.heads, vec![second]);
    assert_eq!(message.changes.len(), 1);
    remote
        .sync()
        .receive_sync_message(&mut remote_state, message)
        .unwrap();
    assert_eq!(remote.get(ROOT, "first").unwrap(), None);
    assert_eq!(remote.get(ROOT, "second").unwrap(), None);

    let mut verifier = SignatureState::new();
    let report = remote.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_requested, 1);
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(request.hash(), second);
    assert_eq!(request.signature().unwrap().as_bytes(), &[81; 64]);

    verifier.complete_verification(request.id(), true);
    let report = remote.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_accepted, 1);
    assert_eq!(
        remote.get(ROOT, "first").unwrap().unwrap().0,
        Value::str("one")
    );
    assert_eq!(
        remote.get(ROOT, "second").unwrap().unwrap().0,
        Value::str("two")
    );
}

#[test]
fn signed_sync_sends_requested_unsigned_cross_actor_dependency() {
    let mut source = AutoCommit::new()
        .with_actor(ActorId::from([1]))
        .with_author(Some(author()))
        .with_signing();
    source.put(ROOT, "alice", "one").unwrap();
    let alice_hash = source.get_heads()[0];

    source.set_actor(ActorId::from([2]));
    source.set_author(Some(other_author()));
    source.put(ROOT, "bob", "two").unwrap();
    let bob_hash = source.get_heads()[0];

    let mut signatures = SignatureState::new();
    source.reconcile_signatures(&mut signatures).unwrap();
    let requested = signatures
        .pending_signing_requests()
        .map(|request| request.hash())
        .collect::<Vec<_>>();
    assert!(requested.contains(&alice_hash));
    assert!(requested.contains(&bob_hash));
    signatures.complete_signing(bob_hash, vec![89; 64]);
    source.reconcile_signatures(&mut signatures).unwrap();
    assert_eq!(source.missing_signature_hashes(), vec![alice_hash]);

    let mut remote = AutoCommit::new().with_signing();
    let mut source_state = SyncState::new();
    let mut remote_state = SyncState::new();

    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .expect("source should send initial signed-sync state");
    assert!(message.heads.is_empty());
    assert!(message.changes.is_empty());

    source
        .sync()
        .receive_sync_message(
            &mut source_state,
            Message {
                heads: Vec::new(),
                need: vec![bob_hash],
                have: Vec::new(),
                changes: ChunkList::empty(),
                flags: Some(MessageFlags::new()),
                version: MessageVersion::V2,
            },
        )
        .unwrap();

    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .expect("source should send explicitly requested Bob with unsigned Alice dependency");
    assert!(message.heads.is_empty());
    assert_eq!(message.changes.len(), 1);
    remote
        .sync()
        .receive_sync_message(&mut remote_state, message)
        .unwrap();
    assert_eq!(remote.get(ROOT, "alice").unwrap(), None);
    assert_eq!(remote.get(ROOT, "bob").unwrap(), None);

    let mut verifier = SignatureState::new();
    let report = remote.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_requested, 2);
    let requests = verifier
        .pending_verification_requests()
        .cloned()
        .collect::<Vec<_>>();
    for request in requests {
        if request.hash() == alice_hash {
            assert!(request.signature().is_none());
        } else if request.hash() == bob_hash {
            assert_eq!(request.signature().unwrap().as_bytes(), &[89; 64]);
        } else {
            panic!("unexpected verification request for {}", request.hash());
        }
        verifier.complete_verification(request.id(), true);
    }
    let report = remote.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_accepted, 2);
    assert_eq!(
        remote.get(ROOT, "alice").unwrap().unwrap().0,
        Value::str("one")
    );
    assert_eq!(
        remote.get(ROOT, "bob").unwrap().unwrap().0,
        Value::str("two")
    );
}

#[test]
fn signed_sync_includes_cross_actor_bundle_dependencies() {
    let mut alice = AutoCommit::new()
        .with_actor(ActorId::from([1]))
        .with_author(Some(author()))
        .with_signing();
    alice.put(ROOT, "alice", "one").unwrap();
    let alice_hash = alice.get_heads()[0];

    let mut signatures = SignatureState::new();
    alice.reconcile_signatures(&mut signatures).unwrap();
    signatures.complete_signing(alice_hash, vec![91; 64]);
    alice.reconcile_signatures(&mut signatures).unwrap();

    let mut source = alice
        .fork()
        .with_actor(ActorId::from([2]))
        .with_author(Some(other_author()));
    source.put(ROOT, "bob", "two").unwrap();
    let bob_hash = source.get_heads()[0];

    let mut signatures = SignatureState::new();
    let report = source.reconcile_signatures(&mut signatures).unwrap();
    assert_eq!(report.signing_requested, 1);
    assert_eq!(
        signatures.pending_signing_requests().next().unwrap().hash(),
        bob_hash
    );
    signatures.complete_signing(bob_hash, vec![92; 64]);
    source.reconcile_signatures(&mut signatures).unwrap();

    let mut remote = AutoCommit::new().with_signing();
    let mut source_state = SyncState::new();
    let mut remote_state = SyncState::new();

    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .expect("initial sync should advertise Bob's head");
    assert_eq!(message.heads, vec![bob_hash]);
    assert!(message.changes.is_empty());
    remote
        .sync()
        .receive_sync_message(&mut remote_state, message)
        .unwrap();

    let response = remote
        .sync()
        .generate_sync_message(&mut remote_state)
        .expect("receiver should request Bob's missing head");
    assert_eq!(response.need, vec![bob_hash]);
    source
        .sync()
        .receive_sync_message(&mut source_state, response)
        .unwrap();

    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .expect("source should send Bob proof bundle");
    assert_eq!(message.heads, vec![bob_hash]);
    assert_eq!(message.changes.len(), 1);
    remote
        .sync()
        .receive_sync_message(&mut remote_state, message)
        .unwrap();
    assert_eq!(remote.get(ROOT, "bob").unwrap(), None);

    let response = remote
        .sync()
        .generate_sync_message(&mut remote_state)
        .expect("receiver should acknowledge received cross-actor proof bundle");
    assert_eq!(response.heads, vec![bob_hash]);
    assert!(response.need.is_empty());
    source
        .sync()
        .receive_sync_message(&mut source_state, response)
        .unwrap();
    assert_eq!(remote.get(ROOT, "alice").unwrap(), None);
    assert_eq!(remote.get(ROOT, "bob").unwrap(), None);

    let mut verifier = SignatureState::new();
    let report = remote.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_requested, 2);
    let requests = verifier
        .pending_verification_requests()
        .cloned()
        .collect::<Vec<_>>();
    for request in requests {
        if request.hash() == alice_hash {
            assert_eq!(request.signature().unwrap().as_bytes(), &[91; 64]);
        } else if request.hash() == bob_hash {
            assert_eq!(request.signature().unwrap().as_bytes(), &[92; 64]);
        } else {
            panic!("unexpected verification request for {}", request.hash());
        }
        verifier.complete_verification(request.id(), true);
    }
    let report = remote.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_accepted, 2);
    assert_eq!(
        remote.get(ROOT, "alice").unwrap().unwrap().0,
        Value::str("one")
    );
    assert_eq!(
        remote.get(ROOT, "bob").unwrap().unwrap().0,
        Value::str("two")
    );

    {
        let response = remote.sync().generate_sync_message(&mut remote_state);
        if let Some(response) = response {
            source
                .sync()
                .receive_sync_message(&mut source_state, response)
                .unwrap();
        }
    }
    {
        let message = source.sync().generate_sync_message(&mut source_state);
        if let Some(message) = message {
            assert!(message.changes.is_empty());
        }
    }
}

#[test]
fn signed_sync_rejected_change_is_acknowledged_but_invisible() {
    let mut source = AutoCommit::new().with_author(Some(author())).with_signing();
    let mut remote = AutoCommit::new().with_signing();
    let mut source_state = SyncState::new();
    let mut remote_state = SyncState::new();

    source.put(ROOT, "key", "value").unwrap();
    let head = source.get_heads()[0];

    let mut signer = SignatureState::new();
    source.reconcile_signatures(&mut signer).unwrap();
    signer.complete_signing(head, vec![71; 64]);
    source.reconcile_signatures(&mut signer).unwrap();

    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .expect("initial sync should advertise heads");
    remote
        .sync()
        .receive_sync_message(&mut remote_state, message)
        .unwrap();

    let response = remote
        .sync()
        .generate_sync_message(&mut remote_state)
        .expect("receiver should request advertised signed change");
    source
        .sync()
        .receive_sync_message(&mut source_state, response)
        .unwrap();

    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .expect("source should send requested signed change");
    assert!(!message.changes.is_empty());
    remote
        .sync()
        .receive_sync_message(&mut remote_state, message)
        .unwrap();

    let mut verifier = SignatureState::new();
    let report = remote.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_requested, 1);
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(request.hash(), head);
    verifier.complete_verification(request.id(), false);
    let report = remote.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_rejected, 1);
    assert_eq!(remote.get(ROOT, "key").unwrap(), None);

    let response = remote
        .sync()
        .generate_sync_message(&mut remote_state)
        .expect("rejected received head should still be acknowledged");
    source
        .sync()
        .receive_sync_message(&mut source_state, response)
        .unwrap();

    if let Some(message) = source.sync().generate_sync_message(&mut source_state) {
        assert!(message.changes.is_empty());
    }
    assert_eq!(remote.get(ROOT, "key").unwrap(), None);
}

#[test]
fn signed_document_save_persists_retained_signature_table() {
    let mut source = AutoCommit::new().with_author(Some(author())).with_signing();
    source.put(ROOT, "first", "one").unwrap();
    source.commit();
    source.put(ROOT, "second", "two").unwrap();
    source.commit();
    let heads = source.get_heads();
    assert_eq!(heads.len(), 1);

    let mut signatures = SignatureState::new();
    source.reconcile_signatures(&mut signatures).unwrap();
    let requests = signatures
        .pending_signing_requests()
        .map(|request| request.hash())
        .collect::<Vec<_>>();
    assert_eq!(requests, heads);
    signatures.complete_signing(heads[0], vec![11; 64]);
    source.reconcile_signatures(&mut signatures).unwrap();

    let saved = source.try_save_signed().unwrap();
    let mut loaded = Automerge::load_with_options(&saved, LoadOptions::new().signing()).unwrap();
    assert!(loaded.get(ROOT, "first").unwrap().is_none());
    assert!(loaded.get(ROOT, "second").unwrap().is_none());

    let mut verifier = SignatureState::new();
    let report = loaded.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_requested, 1);
    let request = verifier
        .pending_verification_requests()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(request.hash(), heads[0]);
    assert_eq!(request.signature().unwrap().as_bytes(), &[11; 64]);

    verifier.complete_verification(request.id(), true);
    let report = loaded.reconcile_signatures(&mut verifier).unwrap();
    assert_eq!(report.verification_accepted, 1);
    assert!(loaded.has_signature(&heads[0]));
    assert!(loaded.missing_signature_hashes().is_empty());
    assert_eq!(
        loaded.get(ROOT, "first").unwrap().unwrap().0,
        Value::str("one")
    );
    assert_eq!(
        loaded.get(ROOT, "second").unwrap().unwrap().0,
        Value::str("two")
    );
}
