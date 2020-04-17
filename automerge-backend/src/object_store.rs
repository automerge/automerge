use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::op_handle::OpHandle;
#[allow(unused_imports)]
use crate::ordered_set::{OrderedSet, SkipList, VecOrderedSet};
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
    pub insertions: HashMap<ElementID, OpHandle>,
    //pub seq: VecOrderedSet<OpID>
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
            //seq: VecOrderedSet::new()
            seq: SkipList::new(),
        }
    }

    pub fn is_seq(&self) -> bool {
        match self.obj_type {
            ObjType::Text | ObjType::List => true,
            _ => false,
        }
    }

    fn get_parent(&self, key: &ElementID) -> Option<ElementID> {
        if key == &ElementID::Head {
            return None;
        }
        // if (!insertion) throw new TypeError(`Missing index entry for list element ${key}`)
        // FIXME key != elementID
        self.insertions
            .get(&key)
            .and_then(|i| i.key.as_element_id().ok())
    }

    fn insertions_after(&self, parent: &ElementID) -> Vec<ElementID> {
        self.following.get(parent).cloned().unwrap_or_default()
    }

    // this is the efficient way to do it for a SkipList
    pub fn index_of2(&self, id: &OpID) -> Result<usize, AutomergeError> {
        let mut prev_id = ElementID::ID(id.clone());
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

    // this is the efficient way to do it for a Vec
    #[allow(dead_code)]
    pub fn index_of1(&self, target: &OpID) -> Result<usize, AutomergeError> {
        let _target = ElementID::ID(target.clone());
        let mut n = 0;
        for a in self.ops_in_order() {
            if a == &_target {
                return Ok(n);
            }

            if a.as_opid() == self.seq.key_of(n) {
                n += 1;
            }
        }
        Err(AutomergeError::MissingIndex(target.clone()))
    }

    #[allow(dead_code)]
    fn ops_in_order(&self) -> ElementIterator {
        ElementIterator {
            following: &self.following,
            stack: vec![self.following.get(&ElementID::Head).unwrap().iter()],
        }
    }

    pub fn insert_after(&mut self, elem: ElementID, op: OpHandle) {
        let eid = ElementID::ID(op.id.clone());
        self.insertions.insert(eid.clone(), op);
        let following = self.following.entry(elem).or_default();
        following.push(eid);
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
