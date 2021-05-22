use automerge_protocol as amp;

use super::{Cursors, StateTreeComposite};

#[derive(Clone)]
pub struct StateTreeChange {
    objects: im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    new_cursors: Cursors,
}

impl StateTreeChange {
    pub(super) fn empty() -> StateTreeChange {
        StateTreeChange {
            objects: im_rc::HashMap::new(),
            new_cursors: Cursors::new(),
        }
    }

    pub(super) fn single(object_id: amp::ObjectId, object: StateTreeComposite) -> StateTreeChange {
        StateTreeChange {
            objects: im_rc::hashmap! {object_id => object},
            new_cursors: Cursors::new(),
        }
    }

    pub(super) fn from_updates(
        objects: im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> StateTreeChange {
        StateTreeChange {
            objects,
            new_cursors: Cursors::new(),
        }
    }

    pub(super) fn with_cursors(mut self, mut cursors: Cursors) -> StateTreeChange {
        cursors.union(self.new_cursors);
        self.new_cursors = cursors;
        self
    }

    pub(super) fn objects(&self) -> im_rc::HashMap<amp::ObjectId, StateTreeComposite> {
        self.objects.clone()
    }

    pub(super) fn new_cursors(&self) -> Cursors {
        self.new_cursors.clone()
    }

    /// Combine with `other`, entries in the current change take precedence
    pub(super) fn union(&mut self, other: StateTreeChange) -> &mut StateTreeChange {
        for (k, v) in other.objects {
            self.objects.insert(k, v);
        }
        let mut cursors = self.new_cursors.clone();
        cursors.union(other.new_cursors);
        self.new_cursors = cursors;
        self
    }
}
