use super::{Cursors, StateTreeComposite};
use automerge_protocol as amp;
use std::ops::{Add, AddAssign};

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
}

impl Add for &StateTreeChange {
    type Output = StateTreeChange;

    fn add(self, rhs: &StateTreeChange) -> Self::Output {
        StateTreeChange {
            objects: self.objects.clone().union(rhs.objects.clone()),
            new_cursors: self.new_cursors.clone().union(rhs.new_cursors.clone()),
        }
    }
}

impl Add for StateTreeChange {
    type Output = StateTreeChange;

    fn add(self, rhs: StateTreeChange) -> Self::Output {
        &self + &rhs
    }
}

impl AddAssign for StateTreeChange {
    fn add_assign(&mut self, rhs: StateTreeChange) {
        self.objects = self.objects.clone().union(rhs.objects);
        self.new_cursors = self.new_cursors.clone().union(rhs.new_cursors);
    }
}
