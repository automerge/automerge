use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::skip_list::SkipList;
use crate::op_handle::OpHandle;
use crate::protocol::{ElementID, Key, ObjType, OpID};
use im_rc::{HashMap, HashSet};
use std::slice::Iter;

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
    pub seq: Vec<OpID>,
    skip_list: SkipList<OpID,bool>,
}

impl ObjState {
    pub fn new(obj_type: ObjType) -> ObjState {
        let mut following = HashMap::new();
        following.insert(ElementID::Head, Vec::new());
        ObjState {
            props: HashMap::new(),
            following,
            obj_type,
            inbound: HashSet::new(),
            seq: Vec::new(),
            skip_list: SkipList::new(),
        }
    }

    pub fn is_seq(&self) -> bool {
        match self.obj_type {
            ObjType::Text | ObjType::List => true,
            _ => false,
        }
    }

    // self.seq is the materialized list of active elements
    // self.ops_in_order() is an iterator across all elements
    // by walking both lists at the same time we can determine the index of
    // an element even if it was just deleted
    pub fn get_index_for(&self, target: &OpID) -> Result<usize, AutomergeError> {
        let _target = ElementID::ID(target.clone());
        let mut n = 0;
        for a in self.ops_in_order() {
            if a == &_target {
                return Ok(n);
            }

            if a.as_opid() == self.seq.get(n) {
                n += 1;
            }
        }
        Err(AutomergeError::MissingIndex(target.clone()))
    }

    fn ops_in_order(&self) -> ElementIterator {
        ElementIterator {
            following: &self.following,
            stack: vec![self.following.get(&ElementID::Head).unwrap().iter()],
        }
    }

    pub fn insert_after(&mut self, elem: ElementID, op: OpHandle) {
        let following = self.following.entry(elem).or_default();
        following.push(ElementID::ID(op.id));
        following.sort_unstable_by(|a, b| b.cmp(a));
    }
}

pub(crate) struct ElementIterator<'a> {
    pub following: &'a HashMap<ElementID, Vec<ElementID>>,
    pub stack: Vec<Iter<'a, ElementID>>,
}

impl<'a> Iterator for ElementIterator<'a> {
    type Item = &'a ElementID;

    fn next(&mut self) -> Option<&'a ElementID> {
        while let Some(mut last) = self.stack.pop() {
            if let Some(next) = last.next() {
                self.stack.push(last);
                if let Some(more) = self.following.get(next) {
                    self.stack.push(more.iter());
                }
                return Some(next);
            }
        }
        None
    }
}
