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

    pub(super) fn with_cursors(mut self, cursors: Cursors) -> StateTreeChange {
        self.new_cursors = cursors.union(self.new_cursors);
        self
    }

    pub(super) fn objects(&self) -> im_rc::HashMap<amp::ObjectId, StateTreeComposite> {
        self.objects.clone()
    }

    pub(super) fn new_cursors(&self) -> Cursors {
        self.new_cursors.clone()
    }

    /// Include changes from `other` in this change
    pub(super) fn update_with(&mut self, other: StateTreeChange) {
        self.objects = other.objects.union(self.objects.clone());
        self.new_cursors = other.new_cursors.union(self.new_cursors.clone());
    }

    /// Combine with `other`, entries in the current change take precedence
    pub(super) fn union(&self, other: StateTreeChange) -> StateTreeChange {
        StateTreeChange {
            objects: self.objects.clone().union(other.objects.clone()),
            new_cursors: self.new_cursors.clone().union(other.new_cursors),
        }
    }
}

impl Add for StateTreeChange {
    type Output = StateTreeChange;

    fn add(mut self, rhs: StateTreeChange) -> Self::Output {
        for (k, v) in rhs.objects {
            self.objects.insert(k, v);
        }
        self.new_cursors = self.new_cursors.union(rhs.new_cursors);
        self
    }
}

impl AddAssign for StateTreeChange {
    fn add_assign(&mut self, rhs: StateTreeChange) {
        for (k, v) in rhs.objects {
            self.objects.insert(k, v);
        }
        self.new_cursors = self.new_cursors.clone().union(rhs.new_cursors);
    }
}
