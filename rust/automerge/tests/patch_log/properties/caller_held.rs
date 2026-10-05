//! Property test for the patch-stream contract for externally (caller) held
//! [`PatchLog`]s.
//!
//! Ensure that the a [`PatchLog`] remains consistent to a document's state when
//! the [`PatchLog`] is exercised through the caller-held pattern.
//! This refers to the use of the Automerge API through:
//!   * [`Automerge::transaction_log_patches`],
//!   * [`Automerge::transaction_at`],
//!   * [`Automerge::make_patches`],
//!   * And [`Automerge::load_incremental_log_patches`].
//!
//! Operations are generated with raw values and clamped/interpreted at
//! execution time so that every generated sequence is valid (this also gives
//! good shrinking behaviour).

use automerge::{
    transaction::Transactable, AutoCommit, Automerge, ObjType, PatchLog, ReadDoc, ROOT,
};
use proptest::prelude::*;

use crate::properties::{actor, apply_patches, gen_text};

#[derive(Debug, Clone)]
enum Op {
    /// A transaction with a fresh actor via [`Automerge::transaction_log_patches`].
    Transact {
        sort_byte: u8,
        pos: usize,
        text: String,
    },
    /// An isolated transaction at earlier heads via [`Automerge::transaction_at`].
    ///
    /// NOT currently generated: [`Automerge::transaction_at`] with a shared
    /// patch log is a known defect — isolation-relative events are mixed with
    /// current-view-relative events. Re-enable in [`gen_op`] once fixed.
    #[allow(dead_code)]
    TransactAt {
        sort_byte: u8,
        heads_idx: usize,
        pos: usize,
        text: String,
    },
    /// Drain via make_patches and check the view against the document.
    Sync,
}

fn gen_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (any::<u8>(), any::<usize>(), gen_text())
            .prop_map(|(sort_byte, pos, text)| Op::Transact { sort_byte, pos, text }),
        2 => Just(Op::Sync),
    ]
}

fn run(ops: Vec<Op>) -> Result<(), TestCaseError> {
    let mut uniq = 0u32;

    // Simulate a saved document that is documented elsewhere.
    let mut origin = AutoCommit::new().with_actor(actor(128, &mut uniq));
    let txt = origin.put_object(&ROOT, "text", ObjType::Text).unwrap();
    origin.splice_text(&txt, 0, 0, "seed").unwrap();
    let saved = origin.save();

    // The saved document is loaded into an empty document with the caller-held
    // patch log.
    let mut doc = Automerge::new();
    let mut log = PatchLog::active();
    doc.load_incremental_log_patches(&saved, &mut log).unwrap();

    let mut history = vec![doc.get_heads()];

    // `make_patches` does not drain the log, so each Sync rebuilds the view
    // from scratch from the cumulative patch list.
    let check = |doc: &Automerge, log: &mut PatchLog| -> Result<(), TestCaseError> {
        let patches = doc.make_patches(log);
        let mut model = String::new();
        apply_patches(&mut model, &patches);
        let actual = doc.text(&txt).unwrap();
        prop_assert_eq!(
            &model,
            &actual,
            "external-log view diverged from document: {:#?}",
            patches
        );
        Ok(())
    };

    for op in ops {
        match op {
            Op::Transact {
                sort_byte,
                pos,
                text,
            } => {
                let a = actor(sort_byte, &mut uniq);
                doc.set_actor(a);
                let mut tx = doc.transaction_log_patches(log).unwrap();
                let len = tx.text(&txt).unwrap().chars().count();
                let pos = pos % (len + 1);
                tx.splice_text(&txt, pos, 0, &text).unwrap();
                let (_, l) = tx.commit();
                log = l;
                history.push(doc.get_heads());
            }
            Op::TransactAt {
                sort_byte,
                heads_idx,
                pos,
                text,
            } => {
                let a = actor(sort_byte, &mut uniq);
                doc.set_actor(a);
                let heads = history[heads_idx % history.len()].clone();
                let mut tx = doc.transaction_at(log, &heads).unwrap();
                let len = tx.text(&txt).unwrap().chars().count();
                let pos = pos % (len + 1);
                tx.splice_text(&txt, pos, 0, &text).unwrap();
                let (_, l) = tx.commit();
                log = l;
                history.push(doc.get_heads());
            }
            Op::Sync => check(&doc, &mut log)?,
        }
    }

    check(&doc, &mut log)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn external_patch_log_view_matches_document(
        ops in proptest::collection::vec(gen_op(), 1..30)
    ) {
        run(ops)?;
    }
}
