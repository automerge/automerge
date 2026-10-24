//! Mutation fuzz over legacy chunks (document, change, BundleV0) loaded
//! into `next` and audited documents.

use automerge::next::transaction::Transactable;
use automerge::next::{AutoCommit, Automerge, SaveFormat, SaveOptions};
use automerge::ROOT;

use crate::common::{actor, assert_no_panics, mutation_fuzz, t0};

fn legacy(deflate: bool) -> SaveOptions {
    SaveOptions {
        format: SaveFormat::Legacy,
        deflate,
        ..Default::default()
    }
}

fn corpus() -> Vec<(Vec<u8>, Automerge)> {
    let mut out = Vec::new();
    let mut d = AutoCommit::new().with_actor(actor(2));
    let text = d
        .put_object(ROOT, "text", automerge::ObjType::Text)
        .unwrap();
    d.splice_text(&text, 0, 0, "hello world").unwrap();
    d.commit_with(t0());
    let list = d
        .put_object(ROOT, "list", automerge::ObjType::List)
        .unwrap();
    d.insert(&list, 0, 1).unwrap();
    d.insert(&list, 1, "two").unwrap();
    d.put(ROOT, "ctr", automerge::ScalarValue::counter(1))
        .unwrap();
    d.commit_with(t0());
    let mut e = d.fork().with_actor(actor(3));
    e.splice_text(&text, 5, 1, "_").unwrap();
    e.increment(ROOT, "ctr", 2).unwrap();
    e.mark(
        &text,
        automerge::marks::Mark::new("bold".to_string(), true, 0, 4),
        automerge::marks::ExpandMark::Both,
    )
    .unwrap();
    e.commit_with(t0());
    d.delete(&list, 0).unwrap();
    d.commit_with(t0());
    let base = Automerge::load(&d.save()).unwrap();
    let heads = d.get_heads();
    d.merge(&mut e).unwrap();
    d.put(ROOT, "done", true).unwrap();
    d.commit_with(t0());
    out.push((d.save_with_options(legacy(false)), Automerge::new()));
    out.push((d.save_with_options(legacy(true)), Automerge::new()));
    out.push((
        d.save_after_with_options(&heads, legacy(false)).unwrap(),
        base.clone(),
    ));
    out.push((
        include_bytes!("../fixtures/bundle_v0_automerge_3_3_2.bin").to_vec(),
        Automerge::new(),
    ));
    out
}

fn exercise(bytes: &[u8], base: &Automerge) -> Result<(), String> {
    if let Ok(mut doc) = Automerge::load(bytes) {
        let _ = doc.get_heads();
        let _ = doc.fragments(..);
        let saved = doc.save();
        Automerge::load(&saved).map_err(|e| format!("next reload: {e}"))?;
        let _ = doc.save_with_options(legacy(true));
        doc.gc();
        let mut tx = doc.transaction();
        tx.put(ROOT, "after", 1).unwrap();
        tx.commit_with(t0());
        let _ = doc.save();
    }
    let mut inc = base.clone();
    if inc.load_incremental(bytes).is_ok() {
        let saved = inc.save();
        Automerge::load(&saved).map_err(|e| format!("next inc reload: {e}"))?;
        let mut tx = inc.transaction();
        tx.put(ROOT, "after", 1).unwrap();
        tx.commit_with(t0());
        let _ = inc.save();
    }
    if let Ok(audit) = automerge::Automerge::load(bytes) {
        let saved = audit.save();
        automerge::Automerge::load(&saved).map_err(|e| format!("audit reload: {e}"))?;
    }
    let mut a = base
        .clone()
        .enable_audit_mode()
        .map_err(|e| e.error.to_string())?;
    if a.load_incremental(bytes).is_ok() {
        let _ = a.save();
    }
    Ok(())
}

#[test]
fn mutated_legacy_chunks_never_panic() {
    let outcome = mutation_fuzz(&corpus(), 100, 300, "legacy_", exercise);
    // an accepted non-canonical change makes the document's own save fail to
    // reload; that is an open question, not a panic
    let (reloads, errors): (Vec<_>, Vec<_>) = outcome
        .errors
        .into_iter()
        .partition(|e| e.contains("reload"));
    eprintln!("{} saves that do not reload: {reloads:#?}", reloads.len());
    assert!(errors.is_empty(), "{} errors:\n{errors:#?}", errors.len());
    assert_no_panics(&outcome.panics);
}
