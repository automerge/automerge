use crate::types::ObjId;
use std::collections::HashMap;

pub(super) struct DocumentTree {
    parent_map: HashMap<ObjId, Option<ObjId>>,
}

impl DocumentTree {
    pub(super) fn new() -> Self {
        Self {
            parent_map: HashMap::new(),
        }
    }

    pub(super) fn insert(&mut self, obj_id: ObjId, parent_id: ObjId) {
        self.parent_map.insert(obj_id, Some(parent_id));
    }

    pub(super) fn remove(&mut self, obj_id: ObjId) {
        self.parent_map.remove(&obj_id);
    }

    pub(super) fn get_parent(&self, obj_id: ObjId) -> Option<ObjId> {
        self.parent_map.get(&obj_id).cloned().unwrap_or(None)
    }

    pub(super)fn is_ancestor_of(&self, ancestor_id: ObjId, descendant_id: ObjId) -> bool {
        let mut current_id = descendant_id;
        while let Some(parent_id) = self.get_parent(current_id) {
            if parent_id == ancestor_id {
                return true;
            }
            current_id = parent_id;
        } // reach root or garbage
        false
    }

    pub(super) fn update_parent(&mut self, obj_id: ObjId, new_parent_id: ObjId) {
        self.parent_map.insert(obj_id, Some(new_parent_id));
    }
}