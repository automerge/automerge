use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::protocol::{ElementID, Key, ObjType, OpHandle, OpID};
use std::collections::{HashMap, HashSet};
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
    pub following: HashMap<ElementID, Vec<OpHandle>>,
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
        }
    }

    pub fn is_seq(&self) -> bool {
        match self.obj_type {
            ObjType::Text | ObjType::List => true,
            _ => false,
        }
    }

    pub fn get_index_for(&self, target: &OpID) -> Result<usize, AutomergeError> {
        self.ops_in_order()
            .scan(0, |n, oid| {
                let last = *n;
                let key = oid.to_key();
                if let Some(ops) = self.props.get(&key) {
                    if !ops.is_empty() {
                        *n += 1;
                    }
                }
                Some((last, oid))
            })
            .find_map(|(last, oid)| if oid == target { Some(last) } else { None })
            .ok_or_else(|| AutomergeError::MissingObjectError(target.to_object_id()))
    }

    pub fn ops_in_order(&self) -> ElementIterator {
        ElementIterator {
            following: &self.following,
            stack: vec![self.following.get(&ElementID::Head).unwrap().iter()],
        }
    }

    pub fn insert_after(&mut self, elem: ElementID, op: OpHandle) {
        let following = self.following.entry(elem).or_default();
        following.push(op);
        following.sort_unstable_by(|a, b| b.cmp(a));
    }
}

pub(crate) struct ElementIterator<'a> {
    pub following: &'a HashMap<ElementID, Vec<OpHandle>>,
    pub stack: Vec<Iter<'a, OpHandle>>,
}

impl<'a> Iterator for ElementIterator<'a> {
    type Item = &'a OpID;

    // I feel like I could be clever here and use iter.chain()
    // FIXME
    fn next(&mut self) -> Option<&'a OpID> {
        while let Some(mut last) = self.stack.pop() {
            if let Some(next) = last.next() {
                self.stack.push(last);
                if let Some(more) = self.following.get(&ElementID::ID(next.id.clone())) {
                    self.stack.push(more.iter());
                }
                return Some(&next.id);
            }
        }
        None
    }
}
