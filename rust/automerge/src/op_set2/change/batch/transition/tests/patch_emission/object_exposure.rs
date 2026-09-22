use super::*;

/// Mark `key` in a hydrated map view as conflicted.
fn set_conflict(view: &mut crate::hydrate::Value, key: &str) {
    std::ops::DerefMut::deref_mut(view.as_map().unwrap())
        .get_mut(key)
        .unwrap()
        .conflict = true;
}

/// A retained existing object whose conflict clears is re-put and its real
/// children are exposed exactly once. Replaying the patches onto the
/// corresponding before-view (existing contents, conflicted) and onto an
/// empty view lacking the contents must both reproduce the after document.
#[test]
fn map_retained_existing_object_conflict_clear_exposes_children_once() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "lower", false).unwrap();
    let child = tx.put_object(ROOT, "obj", ObjType::Map).unwrap();
    tx.put(&child, "a", 1).unwrap();
    let nested = tx.put_object(&child, "nested", ObjType::List).unwrap();
    tx.insert(&nested, 0, "x").unwrap();
    tx.commit();
    let lower = opid(&doc, &doc.get(ROOT, "lower").unwrap().unwrap().1);
    let child_id = opid(&doc, &child);

    let mut before = summary(child_id, Value::map());
    before.add_existing(lower, Value::scalar(false));
    let after = summary(child_id, Value::map());
    let mut log = PatchLog::active();
    ValueTransition::new(before, after).emit_map(ObjId::root(), "obj", &mut log);
    let patches = doc.make_patches(&mut log);

    let mut expected = doc.hydrate(None);
    expected.as_map().unwrap().remove("lower");

    // Corresponding before-view: the object already holds its existing
    // contents and is conflicted; only the conflict clears.
    let mut before_view = expected.clone();
    set_conflict(&mut before_view, "obj");
    assert_ne!(before_view, expected);
    before_view
        .apply_patches(doc.text_encoding(), patches.clone())
        .unwrap();
    assert_eq!(before_view, expected, "patches={patches:?}");

    // Reconstruction view: the key exists as a conflicted, empty object.
    let mut view = crate::hydrate::Value::map();
    view.apply_patches(
        doc.text_encoding(),
        vec![crate::Patch {
            obj: crate::ObjId::Root,
            path: vec![],
            action: PatchAction::PutMap {
                key: "obj".into(),
                value: (crate::Value::Object(ObjType::Map), child.clone()),
                conflict: true,
            },
        }],
    )
    .unwrap();
    view.apply_patches(doc.text_encoding(), patches.clone())
        .unwrap();
    assert_eq!(view, expected, "patches={patches:?}");

    let puts = patches
        .iter()
        .filter(|p| matches!(&p.action, PatchAction::PutMap { key, .. } if key == "obj"))
        .count();
    assert_eq!(puts, 1);
    let conflicted = patches
    .iter()
    .filter(|p| matches!(&p.action, PatchAction::PutMap { key, conflict: true, .. } if key == "obj"))
    .count();
    assert_eq!(conflicted, 0);
}

/// Old case: an existing object survives a deleted higher winner while an
/// incoming lower loser arrives. Replaced, conflicted, and exposed with its
/// real children. Replay starts from the corresponding before-view, where
/// the key shows the old scalar winner over the conflicting object, and
/// from an empty view.
#[test]
fn map_replaced_by_existing_survivor_exposes_it_despite_incoming_loser() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "lower", false).unwrap();
    let child = tx.put_object(ROOT, "obj", ObjType::Map).unwrap();
    tx.put(&child, "a", 1).unwrap();
    tx.put(ROOT, "z", false).unwrap();
    tx.commit();
    let child_id = opid(&doc, &child);
    let higher = opid(&doc, &doc.get(ROOT, "z").unwrap().unwrap().1);
    let incoming_lower = opid(&doc, &doc.get(ROOT, "lower").unwrap().unwrap().1);
    assert!(incoming_lower < child_id && child_id < higher);

    let mut before = summary(child_id, Value::map());
    before.add_existing(higher, Value::scalar(false));
    let mut after = summary(child_id, Value::map());
    after.add_incoming(incoming_lower, Value::scalar(true));
    let mut log = PatchLog::active();
    ValueTransition::new(before, after).emit_map(ObjId::root(), "obj", &mut log);
    let patches = doc.make_patches(&mut log);

    let mut expected = doc.hydrate(None);
    expected.as_map().unwrap().remove("z");
    expected.as_map().unwrap().remove("lower");
    set_conflict(&mut expected, "obj");

    // Corresponding before-view: the old scalar winner, conflicted with
    // the object it hid.
    let mut before_view = crate::hydrate::Value::map();
    before_view
        .apply_patches(
            doc.text_encoding(),
            vec![crate::Patch {
                obj: crate::ObjId::Root,
                path: vec![],
                action: PatchAction::PutMap {
                    key: "obj".into(),
                    value: (crate::Value::from(false), doc.id_to_exid(higher)),
                    conflict: true,
                },
            }],
        )
        .unwrap();
    assert_ne!(before_view, expected);
    before_view
        .apply_patches(doc.text_encoding(), patches.clone())
        .unwrap();
    assert_eq!(before_view, expected, "patches={patches:?}");

    let mut view = crate::hydrate::Value::map();
    view.apply_patches(doc.text_encoding(), patches.clone())
        .unwrap();
    assert_eq!(view, expected, "patches={patches:?}");
}

/// Real attachment and after-contents for the map/list exposure cases below.
/// IDs supply encoder fixtures, not proof of every case's ingestion reachability.
struct ObjectFixture {
    doc: Automerge,
    owner: crate::ObjId,
    child: crate::ObjId,
    child_id: OpId,
    old_id: OpId,
    counter_id: OpId,
    new_id: OpId,
    list: bool,
}

impl ObjectFixture {
    fn new(list: bool) -> Self {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let owner = if list {
            tx.put_object(ROOT, "items", ObjType::List).unwrap()
        } else {
            ROOT
        };
        let child = if list {
            tx.insert_object(&owner, 0, ObjType::List).unwrap()
        } else {
            tx.put_object(&owner, "obj", ObjType::List).unwrap()
        };
        tx.insert(&child, 0, ScalarValue::counter(15)).unwrap();
        tx.insert(&child, 1, "new").unwrap();
        tx.put(ROOT, "old", false).unwrap();
        tx.commit();
        let child_id = opid(&doc, &child);
        let old_id = opid(&doc, &doc.get(ROOT, "old").unwrap().unwrap().1);
        let counter_id = opid(&doc, &doc.get(&child, 0).unwrap().unwrap().1);
        let new_id = opid(&doc, &doc.get(&child, 1).unwrap().unwrap().1);
        assert!(child_id < old_id);
        Self {
            doc,
            owner,
            child,
            child_id,
            old_id,
            counter_id,
            new_id,
            list,
        }
    }

    fn emit(&self, before: CandidateSummary, after: CandidateSummary, appeared: bool) -> PatchLog {
        let transition = ValueTransition::new(before, after);
        if appeared {
            assert!(matches!(transition.0, Transition::Appeared { .. }));
        } else {
            assert!(matches!(transition.0, Transition::WinnerReplaced { .. }));
        }
        let mut log = PatchLog::active();
        if self.list {
            transition.emit_sequence(
                opid(&self.doc, &self.owner).into(),
                0,
                SequenceType::List,
                self.doc.text_encoding(),
                &RichTextDiff::default(),
                &mut log,
            );
        } else {
            transition.emit_map(ObjId::root(), "obj", &mut log);
        }
        log
    }

    /// Replay from an absent property/element or the old scalar winner.
    fn assert_replay(&self, appeared: bool, was_conflicted: bool, mut log: PatchLog) {
        let patches = self.doc.make_patches(&mut log);
        let mut expected = self.doc.hydrate(None);
        expected.as_map().unwrap().remove("old");
        let mut view = expected.clone();
        let old_value = (crate::Value::from(false), self.doc.id_to_exid(self.old_id));
        let reset = match (self.list, appeared) {
            (false, true) => PatchAction::DeleteMap { key: "obj".into() },
            (true, true) => PatchAction::DeleteSeq {
                index: 0,
                length: 1,
            },
            (false, false) => PatchAction::PutMap {
                key: "obj".into(),
                value: old_value,
                conflict: was_conflicted,
            },
            (true, false) => PatchAction::PutSeq {
                index: 0,
                value: old_value,
                conflict: was_conflicted,
            },
        };
        let path = if self.list {
            vec![Prop::from("items")]
        } else {
            vec![]
        };
        view.apply(path.iter(), self.doc.text_encoding(), reset)
            .unwrap();
        assert_ne!(view, expected);
        view.apply_patches(self.doc.text_encoding(), patches.clone())
            .unwrap();
        assert_eq!(
            view, expected,
            "list={}, appeared={appeared}, was_conflicted={was_conflicted}, patches={patches:?}",
            self.list
        );

        let parent: Vec<_> = patches.iter().filter(|p| p.obj == self.owner).collect();
        assert_eq!(parent.len(), 1);
        match &parent[0].action {
            PatchAction::PutMap {
                value, conflict, ..
            }
            | PatchAction::PutSeq {
                value, conflict, ..
            } => {
                assert_eq!(value.1, self.child);
                assert!(!conflict);
            }
            PatchAction::Insert { values, .. } => {
                let values: Vec<_> = values.iter().collect();
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].1, self.child);
                assert!(!values[0].2);
            }
            action => panic!("expected object assignment, got {action:?}"),
        }
    }
}

#[test]
fn existing_objects_expose_contents_without_reapplying_child_deltas() {
    let mut cases = 0;
    for list in [false, true] {
        for appeared in [false, true] {
            let fx = ObjectFixture::new(list);
            let mut before = CandidateSummary::default();
            if !appeared {
                before.add_existing(fx.old_id, Value::scalar(false));
                before.add_existing(fx.child_id, Value::list());
            }
            let after = summary(fx.child_id, Value::list());
            let mut log = fx.emit(before, after, appeared);
            // Full exposure already contains counter15 and "new". Neither
            // ordinary child update may be applied on top of those contents.
            log.increment_seq(fx.child_id.into(), 0, 5, fx.counter_id);
            log.insert(
                fx.child_id.into(),
                1,
                Value::scalar("new"),
                fx.new_id,
                false,
            );
            fx.assert_replay(appeared, true, log);
            cases += 1;
        }
    }
    assert_eq!(cases, 4);
}

#[test]
fn incoming_objects_are_populated_only_by_their_child_patches() {
    let mut cases = 0;
    for list in [false, true] {
        for appeared in [false, true] {
            let fx = ObjectFixture::new(list);
            let mut before = CandidateSummary::default();
            if !appeared {
                before.add_existing(fx.old_id, Value::scalar(false));
            }
            let mut after = CandidateSummary::default();
            after.add_incoming(fx.child_id, Value::list());
            let mut log = fx.emit(before, after, appeared);
            // Emitting only the parent must not expose any child contents.
            let parent_only = fx.doc.make_patches(&mut log.clone());
            assert_eq!(parent_only.len(), 1, "{parent_only:?}");
            assert_eq!(parent_only[0].obj, fx.owner);

            log.insert(fx.child_id.into(), 0, counter(15), fx.counter_id, false);
            log.insert(
                fx.child_id.into(),
                1,
                Value::scalar("new"),
                fx.new_id,
                false,
            );
            fx.assert_replay(appeared, false, log);
            cases += 1;
        }
    }
    assert_eq!(cases, 4);
}

#[test]
fn map_incoming_object_is_not_exposed() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let child = tx.put_object(ROOT, "obj", ObjType::Map).unwrap();
    tx.put(&child, "a", 1).unwrap();
    tx.commit();
    let child_id = opid(&doc, &child);
    let mut after = CandidateSummary::default();
    after.add_incoming(child_id, Value::map());
    let mut log = PatchLog::active();
    ValueTransition::new(CandidateSummary::default(), after).emit_map(
        ObjId::root(),
        "obj",
        &mut log,
    );
    let actions: Vec<_> = doc
        .make_patches(&mut log)
        .into_iter()
        .map(|p| p.action)
        .collect();
    assert_eq!(actions.len(), 1, "{actions:?}");
    let (key, value, conflict) = put_map_of(&actions[0]);
    assert_eq!(key, "obj");
    assert!(value.is_object());
    assert!(!conflict);
}
