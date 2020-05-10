use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::op_handle::OpHandle;
use crate::ordered_set::{OrderedSet, SkipList};
use crate::actor_map::ActorMap;
use crate::protocol::{ElementID, Key};
use im_rc::{HashMap, HashSet};
use automerge_protocol::{ObjType, OpID};

/// ObjectHistory is what the OpSet uses to store operations for a particular
/// key, they represent the two possible container types in automerge, a map or
/// a sequence (tables and text are effectively the maps and sequences
/// respectively).

/// Stores operations on map objects
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObjState {
    pub props: HashMap<Key, ConcurrentOperations>,
    pub obj_type: ObjType,
    pub inbound: HashSet<OpHandle>,
    pub following: HashMap<ElementID, Vec<ElementID>>,
    pub insertions: HashMap<ElementID, OpHandle>,
    pub seq: SkipList<OpID>,
}

impl ObjState {
    pub fn new(obj_type: ObjType) -> ObjState {
        let mut following = HashMap::new();
        following.insert(ElementID::Head, Vec::new());
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
            ObjType::Text | ObjType::List => true,
            _ => false,
        }
    }

    fn get_parent(&self, id: &ElementID) -> Option<ElementID> {
        self.insertions.get(&id).and_then(|i| i.key.as_element_id().ok())
    }

    fn insertions_after(&self, parent: &ElementID) -> Vec<ElementID> {
        self.following.get(parent).cloned().unwrap_or_default()
    }

    // this is the efficient way to do it for a SkipList
    pub fn index_of(&self, id: &OpID) -> Result<usize, AutomergeError> {
        let mut prev_id = id.into();
        let mut index = None;
        // reverse walk through the following/insertions and looking for something that not deleted
        while index.is_none() {
            prev_id = self.get_previous(&prev_id)?;
            match &prev_id {
                ElementID::ID(ref id) => {
                    // FIXME maybe I can speed this up with self.props.get before looking for
                    index = self.seq.index_of(id)
                }
                ElementID::Head => break,
            }
        }
        Ok(index.map(|i| i + 1).unwrap_or(0))
    }

    fn get_previous(&self, element: &ElementID) -> Result<ElementID, AutomergeError> {
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

    pub fn insert_after(&mut self, elem: ElementID, op: OpHandle, actors: &ActorMap) {
        let eid = ElementID::from(&op.id);
        self.insertions.insert(eid.clone(), op);
        let following = self.following.entry(elem).or_default();
        following.push(eid);
        following.sort_unstable_by(|a, b| actors.cmp(b,a));
    }
}

