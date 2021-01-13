use crate::actor_map::ActorMap;
use crate::concurrent_operations::ConcurrentOperations;
use crate::internal::{ElementID, Key, OpID};
use crate::op_handle::OpHandle;
use crate::ordered_set::{OrderedSet, SkipList};
use automerge_protocol as amp;
use fxhash::FxBuildHasher;
//use im_rc::{HashMap, HashSet};
use std::collections::{HashMap, HashSet};

/// ObjectHistory is what the OpSet uses to store operations for a particular
/// key, they represent the two possible container types in automerge, a map or
/// a sequence (tables and text are effectively the maps and sequences
/// respectively).

/// Stores operations on map objects
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObjState {
    pub props: HashMap<Key, ConcurrentOperations>,
    pub obj_type: amp::ObjType,
    pub inbound: HashSet<OpHandle, FxBuildHasher>,
    pub following: HashMap<ElementID, Vec<ElementID>, FxBuildHasher>,
    pub insertions: HashMap<ElementID, OpHandle, FxBuildHasher>,
    pub seq: SkipList<OpID>,
}

impl ObjState {
    pub fn new(obj_type: amp::ObjType) -> ObjState {
        let mut following = HashMap::default();
        following.insert(ElementID::Head, Vec::new());
        ObjState {
            props: HashMap::default(),
            following,
            insertions: HashMap::default(),
            obj_type,
            inbound: HashSet::default(),
            seq: SkipList::new(),
        }
    }

    pub fn is_seq(&self) -> bool {
        matches!(self.obj_type, amp::ObjType::Sequence(_))
    }

    fn get_parent(&self, id: &ElementID) -> Option<ElementID> {
        self.insertions.get(&id).and_then(|i| i.key.as_element_id())
    }

    fn insertions_after(&self, parent: &ElementID) -> Vec<ElementID> {
        self.following.get(parent).cloned().unwrap_or_default()
    }

    // this is the efficient way to do it for a SkipList
    pub fn index_of(&self, id: OpID) -> Option<usize> {
        let mut prev_id = id.into();
        let mut index = None;
        // reverse walk through the following/insertions and looking for something that not deleted
        while index.is_none() {
            prev_id = match self.get_previous(&prev_id) {
                Some(p) => p,
                None => return None,
            };
            match prev_id {
                ElementID::ID(id) => {
                    // FIXME maybe I can speed this up with self.props.get before looking for
                    index = self.seq.index_of(&id)
                }
                ElementID::Head => return None,
            }
        }
        index.map(|i| i + 1)
    }

    fn get_previous(&self, element: &ElementID) -> Option<ElementID> {
        let parent_id = match self.get_parent(element) {
            Some(p) => p,
            None => return None,
        };
        let children = self.insertions_after(&parent_id);
        let pos = match children.iter().position(|k| k == element) {
            Some(p) => p,
            None => return None,
        };
        if pos == 0 {
            Some(parent_id)
        } else {
            let mut prev_id = children[pos - 1]; // FIXME - use refs here
            loop {
                match self.insertions_after(&prev_id).last() {
                    Some(id) => prev_id = *id,
                    None => return Some(prev_id),
                }
            }
        }
    }

    pub fn insert_after(&mut self, elem: ElementID, op: OpHandle, actors: &ActorMap) {
        let eid = op.id.into();
        self.insertions.insert(eid, op);
        let following = self.following.entry(elem).or_default();
        following.push(eid);
        following.sort_unstable_by(|a, b| actors.cmp(b, a));
    }
}
