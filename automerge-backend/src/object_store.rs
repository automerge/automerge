use crate::actor_map::ActorMap;
use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::op_handle::OpHandle;
use crate::ordered_set::{OrderedSet, SkipList};
use automerge_protocol as amp;
use im_rc::{HashMap, HashSet};

/// ObjectHistory is what the OpSet uses to store operations for a particular
/// key, they represent the two possible container types in automerge, a map or
/// a sequence (tables and text are effectively the maps and sequences
/// respectively).

/// Stores operations on map objects
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObjState {
    pub props: HashMap<amp::Key, ConcurrentOperations>,
    pub obj_type: amp::ObjType,
    pub inbound: HashSet<OpHandle>,
    pub following: HashMap<amp::ElementID, Vec<amp::ElementID>>,
    pub insertions: HashMap<amp::ElementID, OpHandle>,
    pub seq: SkipList<amp::OpID>,
}

impl ObjState {
    pub fn new(obj_type: amp::ObjType) -> ObjState {
        let mut following = HashMap::new();
        following.insert(amp::ElementID::Head, Vec::new());
        ObjState {
            props: HashMap::new(),
            following,
            insertions: HashMap::new(),
            obj_type,
            inbound: HashSet::new(),
            seq: SkipList::new(),
        }
    }

    pub fn is_seq(&self) -> bool {
        match self.obj_type {
            amp::ObjType::Text | amp::ObjType::List => true,
            _ => false,
        }
    }

    fn get_parent(&self, id: &amp::ElementID) -> Option<amp::ElementID> {
        self.insertions.get(&id).and_then(|i| i.key.as_element_id())
    }

    fn insertions_after(&self, parent: &amp::ElementID) -> Vec<amp::ElementID> {
        self.following.get(parent).cloned().unwrap_or_default()
    }

    // this is the efficient way to do it for a SkipList
    pub fn index_of(&self, id: &amp::OpID) -> Result<usize, AutomergeError> {
        let mut prev_id = id.into();
        let mut index = None;
        // reverse walk through the following/insertions and looking for something that not deleted
        while index.is_none() {
            prev_id = self.get_previous(&prev_id)?;
            match &prev_id {
                amp::ElementID::ID(ref id) => {
                    // FIXME maybe I can speed this up with self.props.get before looking for
                    index = self.seq.index_of(id)
                }
                amp::ElementID::Head => break,
            }
        }
        Ok(index.map(|i| i + 1).unwrap_or(0))
    }

    fn get_previous(&self, element: &amp::ElementID) -> Result<amp::ElementID, AutomergeError> {
        let parent_id = self.get_parent(element).unwrap();
        let children = self.insertions_after(&parent_id);
        let pos = children
            .iter()
            .position(|k| k == element)
            .ok_or_else(|| AutomergeError::GeneralError("get_previous".to_string()))?;
        if pos == 0 {
            Ok(parent_id)
        } else {
            let mut prev_id = children[pos - 1].clone(); // FIXME - use refs here
            loop {
                match self.insertions_after(&prev_id).last() {
                    Some(id) => prev_id = id.clone(),
                    None => return Ok(prev_id.clone()),
                }
            }
        }
    }

    pub fn insert_after(&mut self, elem: amp::ElementID, op: OpHandle, actors: &ActorMap) {
        let eid = amp::ElementID::from(&op.id);
        self.insertions.insert(eid.clone(), op);
        let following = self.following.entry(elem).or_default();
        following.push(eid);
        following.sort_unstable_by(|a, b| actors.cmp(b, a));
    }
}
