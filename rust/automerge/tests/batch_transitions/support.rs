use automerge::{
    hydrate, transaction::Transactable, ActorId, Automerge, Change, ObjId, ObjType, Patch,
    PatchLog, Prop, ReadDoc, ScalarValue, ROOT,
};

pub(super) fn actor(n: u8) -> ActorId {
    ActorId::from(vec![n])
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Site {
    Map,
    List,
}

pub(super) const SITES: [Site; 2] = [Site::Map, Site::List];

impl Site {
    /// Create the same initial map property or existing list element for each
    /// scenario. The map key is explicit so tests retain their original paths.
    pub(super) fn initial(self, key: &str) -> (Automerge, ObjId, Prop) {
        let mut doc = Automerge::new().with_actor(actor(9));
        match self {
            Self::Map => (doc, ROOT, Prop::Map(key.into())),
            Self::List => {
                let mut tx = doc.transaction();
                let obj = tx.put_object(ROOT, "items", ObjType::List).unwrap();
                tx.insert(&obj, 0, "base").unwrap();
                tx.commit();
                (doc, obj, Prop::Seq(0))
            }
        }
    }

    /// Read the value/conflict metadata at the fixture's location after replay.
    pub(super) fn observed_value<'a>(
        self,
        view: &'a mut hydrate::Value,
        key: &str,
    ) -> (&'a hydrate::Value, bool) {
        let root = view.as_map().unwrap();
        match self {
            Self::Map => {
                let (_, item) = root.iter().find(|(name, _)| name.as_str() == key).unwrap();
                (&item.value, item.conflict)
            }
            Self::List => {
                let hydrate::Value::List(items) = root.get("items").unwrap() else {
                    panic!("expected list");
                };
                let item = items.iter().next().unwrap();
                (&item.value, item.conflict)
            }
        }
    }
}

pub(super) fn put(doc: &mut Automerge, obj: &ObjId, prop: &Prop, value: ScalarValue) -> Change {
    let mut tx = doc.transaction();
    tx.put(obj, prop.clone(), value).unwrap();
    tx.commit();
    doc.get_last_local_change().unwrap()
}

pub(super) fn delete(doc: &mut Automerge, obj: &ObjId, prop: &Prop) -> Change {
    let mut tx = doc.transaction();
    tx.delete(obj, prop.clone()).unwrap();
    tx.commit();
    doc.get_last_local_change().unwrap()
}

pub(super) fn increment(doc: &mut Automerge, obj: &ObjId, prop: &Prop, delta: i64) -> Change {
    let mut tx = doc.transaction();
    tx.increment(obj, prop.clone(), delta).unwrap();
    tx.commit();
    doc.get_last_local_change().unwrap()
}

pub(super) fn candidates(
    doc: &Automerge,
    obj: &ObjId,
    prop: &Prop,
) -> Vec<(automerge::Value<'static>, ObjId)> {
    doc.get_all(obj, prop.clone())
        .unwrap()
        .into_iter()
        .map(|(value, id)| (value.into_owned(), id))
        .collect()
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Schedule {
    Together,
    ReversedTogether,
    FirstThenSecond,
    SecondThenFirst,
}

pub(super) const SCHEDULES: [Schedule; 4] = [
    Schedule::Together,
    Schedule::ReversedTogether,
    Schedule::FirstThenSecond,
    Schedule::SecondThenFirst,
];

impl Schedule {
    pub(super) fn groups(self, first: &Change, second: &Change) -> Vec<Vec<Change>> {
        match self {
            Self::Together => vec![vec![first.clone(), second.clone()]],
            Self::ReversedTogether => vec![vec![second.clone(), first.clone()]],
            Self::FirstThenSecond => vec![vec![first.clone()], vec![second.clone()]],
            Self::SecondThenFirst => vec![vec![second.clone()], vec![first.clone()]],
        }
    }

    pub(super) fn is_grouped(self) -> bool {
        matches!(self, Self::Together | Self::ReversedTogether)
    }
}

pub(super) fn batch_orders(changes: Vec<Change>) -> [Vec<Change>; 2] {
    let reversed = changes.iter().rev().cloned().collect();
    [changes, reversed]
}

/// Check hydrated replay after every delivery, while exposing its patches for
/// the few scenarios which also assert an exact patch-language contract.
pub(super) struct Replay {
    pub(super) doc: Automerge,
    pub(super) view: hydrate::Value,
}

impl Replay {
    pub(super) fn new(doc: Automerge) -> Self {
        let view = doc.hydrate(None);
        Self { doc, view }
    }

    pub(super) fn apply(&mut self, changes: Vec<Change>, label: &str) -> Vec<Patch> {
        let mut log = PatchLog::active();
        self.doc
            .apply_changes_log_patches(changes, &mut log)
            .unwrap();
        let patches = self.doc.make_patches(&mut log);
        let result = self
            .view
            .apply_patches(self.doc.text_encoding(), patches.clone());
        assert!(result.is_ok(), "{label}: {result:?}; patches={patches:?}");
        assert_eq!(
            self.view,
            self.doc.hydrate(None),
            "{label}: patches={patches:?}"
        );
        patches
    }
}

pub(super) fn replay(doc: Automerge, groups: Vec<Vec<Change>>, label: &str) -> Automerge {
    let mut replay = Replay::new(doc);
    for (step, changes) in groups.into_iter().enumerate() {
        replay.apply(changes, &format!("{label}, step {step}"));
    }
    replay.doc
}
