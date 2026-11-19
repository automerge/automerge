//! `init_root_from_hydrate` into an empty document encodes each op once, as
//! its change's columns, and loads the op set from those bytes; the commit
//! reuses them. The document must be exactly the one the same change gives
//! when applied op by op: the same save, the same heads, the same reads
//! through every index, and the same again after further edits. A save that
//! loads checks each change's hash, so the change's bytes must be the ones
//! encoding its ops afresh gives.

use automerge::marks::{ExpandMark, Mark};
use automerge::transaction::Transactable;
use automerge::{
    hydrate, hydrate_list, hydrate_map, hydrate_text, ActorId, Author, AutoCommit, Automerge,
    ObjId, ObjType, PatchLog, ReadDoc, ScalarValue, TextEncoding, Value, ROOT,
};

fn actor() -> ActorId {
    ActorId::from(&[7u8; 16][..])
}

/// A small deterministic generator, so the documents are varied but the
/// same on every run.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn scalar(rng: &mut Lcg) -> hydrate::Value {
    let v = match rng.below(10) {
        0 => ScalarValue::Null,
        1 => ScalarValue::Boolean(rng.below(2) == 0),
        2 => ScalarValue::Int(rng.below(1000) as i64 - 500),
        3 => ScalarValue::Uint(rng.below(1 << 20)),
        4 => ScalarValue::F64(rng.below(1000) as f64 / 7.0),
        5 => ScalarValue::Bytes(vec![rng.below(256) as u8; rng.below(5) as usize]),
        6 => ScalarValue::Timestamp(rng.below(1 << 30) as i64),
        7 => ScalarValue::counter(rng.below(100) as i64),
        // Runs of one value, so columns hold runs as well as literals.
        8 => ScalarValue::Str("same".into()),
        _ => ScalarValue::Str(format!("s{}", rng.below(50)).into()),
    };
    hydrate::Value::from(v)
}

const WORDS: [&str; 6] = ["héllo ", "wörld ", "🦀 ", "a", "", "text\n"];

fn value(rng: &mut Lcg, depth: u32) -> hydrate::Value {
    match if depth == 0 { 3 } else { rng.below(6) } {
        0 => map(rng, depth - 1).into(),
        1 => {
            let n = rng.below(12) as usize;
            let items: Vec<_> = (0..n).map(|_| value(rng, depth - 1)).collect();
            hydrate::Value::from(items)
        }
        2 => {
            let n = rng.below(8) as usize;
            let s: String = (0..n).map(|_| WORDS[rng.below(6) as usize]).collect();
            hydrate_text!(s).into()
        }
        _ => scalar(rng),
    }
}

fn map(rng: &mut Lcg, depth: u32) -> hydrate::Map {
    let n = rng.below(10) as usize;
    let entries: std::collections::HashMap<String, hydrate::Value> = (0..n)
        .map(|_| (format!("k{}", rng.below(30)), value(rng, depth)))
        .collect();
    hydrate::Map::from(entries)
}

/// `doc` and `reference` are the same document: save, heads, and every
/// object read through the op set's indexes.
fn assert_same(doc: &Automerge, reference: &Automerge) {
    assert_eq!(doc.get_heads(), reference.get_heads());
    assert_eq!(doc.save(), reference.save());
    assert_eq!(doc.hydrate(None), reference.hydrate(None));
    // A save loads (which checks each change's hash) to the same bytes.
    let loaded = Automerge::load(&doc.save()).unwrap();
    assert_eq!(loaded.save(), doc.save());
    assert_same_object(doc, reference, &ROOT, ObjType::Map);
}

fn assert_same_object(doc: &Automerge, reference: &Automerge, obj: &ObjId, typ: ObjType) {
    assert_eq!(doc.length(obj), reference.length(obj), "length of {obj}");
    assert_eq!(
        doc.parents(obj).unwrap().path(),
        reference.parents(obj).unwrap().path()
    );
    let children: Vec<(Value<'_>, ObjId)> = match typ {
        ObjType::Map | ObjType::Table => doc
            .keys(obj)
            .map(|k| {
                let got = doc.get(obj, &k).unwrap().unwrap();
                assert_eq!(Some(got.clone()), reference.get(obj, &k).unwrap());
                got
            })
            .collect(),
        ObjType::List => (0..doc.length(obj))
            .map(|i| {
                let got = doc.get(obj, i).unwrap().unwrap();
                assert_eq!(Some(got.clone()), reference.get(obj, i).unwrap());
                got
            })
            .collect(),
        ObjType::Text => {
            assert_eq!(doc.text(obj).unwrap(), reference.text(obj).unwrap());
            assert_eq!(doc.marks(obj).unwrap(), reference.marks(obj).unwrap());
            vec![]
        }
    };
    for (value, child) in children {
        if let Value::Object(typ) = value {
            assert_same_object(doc, reference, &child, typ);
        }
    }
}

/// The objects of `doc` by type, in a fixed order.
fn objects<R: ReadDoc>(doc: &R, obj: &ObjId, typ: ObjType, out: &mut Vec<(ObjId, ObjType)>) {
    out.push((obj.clone(), typ));
    let children: Vec<_> = match typ {
        ObjType::Map | ObjType::Table => doc
            .keys(obj)
            .map(|k| doc.get(obj, &k).unwrap().unwrap())
            .collect(),
        ObjType::List => (0..doc.length(obj))
            .map(|i| doc.get(obj, i).unwrap().unwrap())
            .collect(),
        ObjType::Text => vec![],
    };
    for (value, child) in children {
        if let Value::Object(typ) = value {
            objects(doc, &child, typ, out);
        }
    }
}

/// Edits every object of `doc` somehow: in the batch's own transaction
/// these are the ops after the batch, whose columns the commit joins on.
/// They overwrite, delete, increment and mark the batch's ops, insert
/// beside them, and make new objects.
fn edit(tx: &mut impl Transactable, objs: &[(ObjId, ObjType)], rng: &mut Lcg) {
    for (obj, typ) in objs {
        match typ {
            ObjType::Map | ObjType::Table => {
                let keys: Vec<String> = tx.keys(obj).collect();
                for key in keys {
                    match rng.below(5) {
                        0 => tx.put(obj, &key, "overwritten").unwrap(),
                        1 => tx.delete(obj, &key).unwrap(),
                        2 => {
                            if let Some((Value::Scalar(s), _)) = tx.get(obj, &key).unwrap() {
                                if matches!(s.as_ref(), ScalarValue::Counter(_)) {
                                    tx.increment(obj, &key, 3).unwrap();
                                }
                            }
                        }
                        _ => {}
                    }
                }
                let new = tx.put_object(obj, "new", ObjType::List).unwrap();
                tx.insert(&new, 0, 1).unwrap();
            }
            ObjType::List => {
                let len = tx.length(obj);
                tx.insert(obj, rng.below(len as u64 + 1) as usize, "inserted")
                    .unwrap();
                if len > 1 {
                    tx.delete(obj, rng.below(len as u64) as usize).unwrap();
                }
            }
            ObjType::Text => {
                let len = tx.length(obj);
                let at = rng.below(len as u64 + 1) as usize;
                tx.splice_text(obj, at, 0, "ins").unwrap();
                tx.mark(
                    obj,
                    Mark::new("bold".into(), true, 0, len + 3),
                    ExpandMark::Both,
                )
                .unwrap();
                if len > 2 {
                    tx.splice_text(obj, 1, 1, "").unwrap();
                }
            }
        }
    }
}

/// `map` written into an empty document, `edits` more ops in the same
/// transaction, and the commit: checked against the change applied op by
/// op, then both edited alike in a second change and checked again.
fn check(map: &hydrate::Map, edits: Option<u64>) {
    let mut doc = Automerge::new().with_actor(actor());
    let mut tx = doc.transaction();
    tx.init_root_from_hydrate(map).unwrap();
    if let Some(seed) = edits {
        let mut objs = vec![];
        objects(&tx, &ROOT, ObjType::Map, &mut objs);
        edit(&mut tx, &objs, &mut Lcg(seed));
    }
    tx.commit();

    let mut reference = Automerge::new().with_actor(actor());
    reference.apply_changes(doc.get_changes(&[])).unwrap();
    assert_same(&doc, &reference);

    // The loaded op set takes later changes as a spliced one does.
    let mut objs = vec![];
    objects(&doc, &ROOT, ObjType::Map, &mut objs);
    for d in [&mut doc, &mut reference] {
        let mut tx = d.transaction();
        edit(&mut tx, &objs, &mut Lcg(99));
        tx.commit();
    }
    assert_same(&doc, &reference);
}

#[test]
fn a_loaded_root_equals_the_change_applied_op_by_op() {
    check(
        &hydrate_map! {
            "name" => "test",
            "count" => ScalarValue::counter(5),
            "items" => hydrate_list!["a", "b", hydrate_map! { "x" => 1_i64 }],
            "text" => hydrate_text!("hello 🦀 world"),
        },
        None,
    );
}

#[test]
fn ops_after_the_batch_join_its_columns() {
    check(
        &hydrate_map! {
            "name" => "test",
            "count" => ScalarValue::counter(5),
            "items" => hydrate_list!["a", "b", hydrate_map! { "x" => 1_i64 }],
            "text" => hydrate_text!("hello 🦀 world"),
        },
        Some(1),
    );
}

/// Ops after the batch that only continue its last runs: the batch ends
/// in a text's characters and the edits insert more into it.
#[test]
fn runs_meeting_at_the_join_merge() {
    let map = hydrate_map! { "t" => hydrate_text!("aaaaaaaa") };
    let mut doc = Automerge::new().with_actor(actor());
    let mut tx = doc.transaction();
    tx.init_root_from_hydrate(&map).unwrap();
    let (_, t) = tx.get(ROOT, "t").unwrap().unwrap();
    tx.splice_text(&t, 8, 0, "aaaa").unwrap();
    tx.commit();
    let mut reference = Automerge::new().with_actor(actor());
    reference.apply_changes(doc.get_changes(&[])).unwrap();
    assert_same(&doc, &reference);
}

#[test]
fn varied_documents_load_as_they_splice() {
    for seed in 0..40 {
        let mut rng = Lcg(seed);
        let root = map(&mut rng, 3);
        check(&root, None);
        check(&root, Some(seed));
    }
}

/// A root large enough that each column the stream hands over as slabs
/// (keys, actions, value metadata) spans many of them, with runs and
/// literals crossing their seams.
#[test]
fn a_root_of_many_slabs_loads_as_it_splices() {
    let mut rng = Lcg(0x5eed);
    let mut root = std::collections::HashMap::new();
    for n in 0..5_000 {
        // Runs of one key length and of one value, among literals.
        let key = if n % 7 < 3 {
            format!("k{:05}", n)
        } else {
            format!("key-{n}")
        };
        let value = if n % 11 < 4 {
            hydrate::Value::from(ScalarValue::Str("same".into()))
        } else if n % 97 == 0 {
            value(&mut rng, 2)
        } else {
            scalar(&mut rng)
        };
        root.insert(key, value);
    }
    let root = hydrate::Map::from(root);
    check(&root, None);
    check(&root, Some(7));
}

#[test]
fn an_empty_root_commits_nothing() {
    let mut doc = Automerge::new().with_actor(actor());
    let mut tx = doc.transaction();
    tx.init_root_from_hydrate(&hydrate_map! {}).unwrap();
    tx.commit();
    assert!(doc.get_heads().is_empty());
}

#[test]
fn text_encodings_other_than_the_default_load() {
    for encoding in [TextEncoding::Utf8CodeUnit, TextEncoding::Utf16CodeUnit] {
        let map = hydrate::Map::from(std::collections::HashMap::from([(
            "t",
            hydrate::Value::from(hydrate::Text::new(encoding, "é🦀x")),
        )]));
        let mut doc = Automerge::new_with_encoding(encoding).with_actor(actor());
        let mut tx = doc.transaction();
        tx.init_root_from_hydrate(&map).unwrap();
        tx.commit();
        let mut reference = Automerge::new_with_encoding(encoding).with_actor(actor());
        reference.apply_changes(doc.get_changes(&[])).unwrap();
        assert_same(&doc, &reference);
    }
}

/// Rolling back a root streamed into an empty document leaves it empty,
/// and it takes the same root again as a new document does.
#[test]
fn a_rolled_back_root_leaves_the_document_empty() {
    let map =
        hydrate_map! { "a" => 1_i64, "t" => hydrate_text!("xy"), "l" => hydrate_list![1_i64] };
    let mut doc = Automerge::new().with_actor(actor());
    let mut tx = doc.transaction();
    tx.init_root_from_hydrate(&map).unwrap();
    tx.put(ROOT, "after", 2_i64).unwrap();
    assert_eq!(tx.rollback(), 7);
    assert_eq!(doc.keys(ROOT).count(), 0);
    assert!(doc.get_heads().is_empty());
    let mut tx = doc.transaction();
    tx.init_root_from_hydrate(&map).unwrap();
    tx.commit();
    let mut fresh = Automerge::new().with_actor(actor());
    let mut tx = fresh.transaction();
    tx.init_root_from_hydrate(&map).unwrap();
    tx.commit();
    assert_eq!(doc.save(), fresh.save());
    let mut reference = Automerge::new().with_actor(actor());
    reference.apply_changes(doc.get_changes(&[])).unwrap();
    assert_same(&doc, &reference);
}

/// Patches logged while a root is written into an empty document, and
/// while ops after it in the same transaction are, describe the document.
#[test]
fn a_loaded_root_logs_its_patches() {
    for seed in 0..10 {
        let mut rng = Lcg(seed);
        let root = map(&mut rng, 3);
        for edits in [false, true] {
            let mut doc = Automerge::new().with_actor(actor());
            let mut tx = doc.transaction_log_patches(PatchLog::active()).unwrap();
            tx.init_root_from_hydrate(&root).unwrap();
            if edits {
                // Ops after the root, overwriting and deleting its own.
                // (Marks are left out: hydrate cannot apply mark patches.)
                let keys: Vec<String> = tx.keys(ROOT).collect();
                if let Some(first) = keys.first() {
                    tx.delete(ROOT, first).unwrap();
                }
                if let Some(last) = keys.last() {
                    tx.put(ROOT, last, "overwritten").unwrap();
                }
                tx.put(ROOT, "after", 1_i64).unwrap();
            }
            let (_, mut log) = tx.commit();
            let mut view = hydrate::Value::from(hydrate_map! {});
            view.apply_patches(TextEncoding::platform_default(), doc.make_patches(&mut log))
                .unwrap();
            assert_eq!(view, doc.hydrate(None));
        }
    }
}

/// The document's actor need not be its first: an empty change by an actor
/// that sorts before it leaves the op set empty but the actor second.
#[test]
fn a_root_by_a_later_actor_loads_as_it_splices() {
    let root = hydrate_map! {
        "items" => hydrate_list!["a", hydrate_map! { "x" => 1_i64 }],
        "text" => hydrate_text!("abc"),
    };
    let mut doc = AutoCommit::new().with_actor(ActorId::from(&[1u8; 16][..]));
    doc.empty_change(Default::default());
    doc.set_actor(actor());
    doc.init_root_from_hydrate(&root).unwrap();
    doc.commit();
    let mut reference = Automerge::new().with_actor(actor());
    reference
        .apply_changes(doc.document().get_changes(&[]))
        .unwrap();
    assert_same(doc.document(), &reference);
    assert_eq!(doc.hydrate(ROOT, None).unwrap(), root.into());
}

/// A root written by a masked author is recorded but hidden, as each op of
/// the same change applied one by one under the same mask is.
#[test]
fn a_masked_root_loads_hidden() {
    let alice = Author::try_from("aaaa").unwrap();
    let root =
        hydrate_map! { "a" => 1_i64, "t" => hydrate_text!("xy"), "l" => hydrate_list![1_i64] };
    let mut doc = AutoCommit::new()
        .with_actor(actor())
        .with_author(Some(alice.clone()));
    doc.empty_change(Default::default());
    let epoch = doc.get_heads();
    doc.mask_author(alice.clone(), &epoch);
    doc.init_root_from_hydrate(&root).unwrap();
    doc.commit();
    assert_eq!(doc.document().keys(ROOT).count(), 0);

    let mut reference = AutoCommit::new().with_actor(actor());
    reference
        .apply_changes(doc.document().get_changes(&[]))
        .unwrap();
    reference.mask_author(alice.clone(), &epoch);
    assert_same(doc.document(), reference.document());

    // Revealed, both show the root.
    doc.reveal_author(&alice);
    reference.reveal_author(&alice);
    assert_same(doc.document(), reference.document());
    assert_eq!(doc.hydrate(ROOT, None).unwrap(), root.into());
}
