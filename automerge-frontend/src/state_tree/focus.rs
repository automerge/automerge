use super::{
    DiffApplicationResult, MultiValue, StateTree, StateTreeChange, StateTreeComposite,
    StateTreeList, StateTreeMap, StateTreeTable, StateTreeValue,
};

#[derive(Clone)]
pub(crate) struct Focus(FocusInner);

impl Focus {
    pub(super) fn update(&mut self, diffapp: DiffApplicationResult<MultiValue>) -> StateTree {
        match &mut self.0 {
            FocusInner::Root(root) => root.update(diffapp).clone(),
            FocusInner::Map(mapfocus) => mapfocus.update(diffapp),
            FocusInner::Table(tablefocus) => tablefocus.update(diffapp),
            FocusInner::List(listfocus) => listfocus.update(diffapp),
        }
    }

    pub fn new_root(root_tree: StateTree, key: String) -> Focus {
        Focus(FocusInner::Root(RootFocus {
            root: root_tree,
            key,
        }))
    }

    pub(super) fn new_map(
        state_tree: StateTree,
        map: StateTreeMap,
        key: String,
        multivalue: MultiValue,
    ) -> Focus {
        Focus(FocusInner::Map(MapFocus {
            state_tree,
            key,
            map,
            multivalue,
        }))
    }

    pub(super) fn new_table(
        state_tree: StateTree,
        table: StateTreeTable,
        key: String,
        multivalue: MultiValue,
    ) -> Focus {
        Focus(FocusInner::Table(TableFocus {
            state_tree,
            key,
            table,
            multivalue,
        }))
    }

    pub(super) fn new_list(
        state_tree: StateTree,
        list: StateTreeList,
        index: usize,
        multivalue: MultiValue,
    ) -> Focus {
        Focus(FocusInner::List(ListFocus {
            state_tree,
            index,
            list,
            multivalue,
        }))
    }
}

#[derive(Clone)]
enum FocusInner {
    Root(RootFocus),
    Map(MapFocus),
    Table(TableFocus),
    List(ListFocus),
}

#[derive(Clone)]
struct RootFocus {
    root: StateTree,
    key: String,
}

impl RootFocus {
    fn update(&mut self, diffapp: DiffApplicationResult<MultiValue>) -> &mut StateTree {
        self.root.update(self.key.clone(), diffapp)
    }
}

#[derive(Clone)]
struct MapFocus {
    state_tree: StateTree,
    key: String,
    map: StateTreeMap,
    multivalue: MultiValue,
}

impl MapFocus {
    fn update(&mut self, diffapp: DiffApplicationResult<MultiValue>) -> StateTree {
        let new_diffapp = diffapp.and_then(|v| {
            let updated = StateTreeComposite::Map(StateTreeMap {
                object_id: self.map.object_id.clone(),
                props: self.map.props.update(self.key.clone(), v),
            });
            DiffApplicationResult::pure(
                self.multivalue
                    .update_default(StateTreeValue::Link(updated.object_id())),
            )
            .with_changes(StateTreeChange::single(self.map.object_id.clone(), updated))
        });
        self.state_tree.apply(new_diffapp.change)
    }
}

#[derive(Clone)]
struct TableFocus {
    state_tree: StateTree,
    key: String,
    table: StateTreeTable,
    multivalue: MultiValue,
}

impl TableFocus {
    fn update(&mut self, diffapp: DiffApplicationResult<MultiValue>) -> StateTree {
        let new_diffapp = diffapp.and_then(|v| {
            let updated = StateTreeComposite::Table(StateTreeTable {
                object_id: self.table.object_id.clone(),
                props: self.table.props.update(self.key.clone(), v),
            });
            DiffApplicationResult::pure(
                self.multivalue
                    .update_default(StateTreeValue::Link(updated.object_id())),
            )
            .with_changes(StateTreeChange::single(
                self.table.object_id.clone(),
                updated,
            ))
        });
        self.state_tree.apply(new_diffapp.change)
    }
}

#[derive(Clone)]
struct ListFocus {
    state_tree: StateTree,
    index: usize,
    list: StateTreeList,
    multivalue: MultiValue,
}

impl ListFocus {
    fn update(&mut self, diffapp: DiffApplicationResult<MultiValue>) -> StateTree {
        let new_diffapp = diffapp.and_then(|v| {
            let updated = StateTreeComposite::List(StateTreeList {
                object_id: self.list.object_id.clone(),
                elements: self.list.elements.update(self.index, v),
            });
            DiffApplicationResult::pure(
                self.multivalue
                    .update_default(StateTreeValue::Link(updated.object_id())),
            )
            .with_changes(StateTreeChange::single(
                self.list.object_id.clone(),
                updated,
            ))
        });
        self.state_tree.apply(new_diffapp.change)
    }
}
