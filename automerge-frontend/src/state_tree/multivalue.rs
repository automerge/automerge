use automerge_protocol as amp;

use super::{
    DiffableSequence, StateTreeChange, StateTreeComposite, StateTreeList, StateTreeMap,
    StateTreeTable, StateTreeText, StateTreeValue,
};
use crate::error;
use crate::value::Value;
use std::iter::Iterator;

pub(crate) struct NewValueRequest<'a, 'b, 'c, 'd> {
    pub(crate) actor: &'a amp::ActorID,
    pub(crate) start_op: u64,
    pub(crate) key: &'b amp::Key,
    pub(crate) value: &'c Value,
    pub(crate) parent_obj: &'d amp::ObjectID,
    pub(crate) insert: bool,
    pub(crate) pred: Vec<amp::OpID>,
}

/// A set of conflicting values for the same key, indexed by OpID
#[derive(Debug, Clone)]
pub(super) struct MultiValue {
    winning_value: (amp::OpID, StateTreeValue),
    conflicts: im::HashMap<amp::OpID, StateTreeValue>,
}

impl MultiValue {
    pub(super) fn new_from_statetree_value(opid: amp::OpID, value: StateTreeValue) -> MultiValue {
        MultiValue {
            winning_value: (opid, value),
            conflicts: im::HashMap::new(),
        }
    }

    pub fn new_from_diff(
        opid: amp::OpID,
        diff: &amp::Diff,
    ) -> Result<StateTreeChange<MultiValue>, error::InvalidPatch> {
        StateTreeValue::new_from_diff(diff)?.fallible_map(move |value| {
            Ok(MultiValue {
                winning_value: (opid, value),
                conflicts: im::HashMap::new(),
            })
        })
    }

    pub(super) fn new_from_value_2(req: NewValueRequest) -> NewValue<MultiValue> {
        Self::new_from_value(
            req.actor,
            req.start_op,
            req.parent_obj.clone(),
            req.key,
            req.value,
            req.insert,
            req.pred.into_iter().collect(),
        )
    }

    pub(super) fn new_from_value(
        actor: &amp::ActorID,
        start_op: u64,
        parent_id: amp::ObjectID,
        key: &amp::Key,
        value: &Value,
        insert: bool,
        pred: Vec<amp::OpID>,
    ) -> NewValue<MultiValue> {
        match value {
            Value::Map(props, amp::MapType::Map) => {
                let make_op_id = amp::OpID(start_op, actor.clone());
                let make_op = amp::Op {
                    action: amp::OpType::Make(amp::ObjType::map()),
                    obj: parent_id,
                    key: key.clone(),
                    insert,
                    pred,
                };
                let map = im::HashMap::new();
                let newvalue: NewValue<im::HashMap<String, MultiValue>> =
                    NewValue::init(map, make_op, start_op);
                props
                    .iter()
                    .fold(newvalue, |newvalue_so_far, (key, value)| {
                        let start_op = newvalue_so_far.max_op + 1;
                        newvalue_so_far.and_then(|m| {
                            MultiValue::new_from_value(
                                actor,
                                start_op,
                                make_op_id.clone().into(),
                                &key.into(),
                                value,
                                false,
                                Vec::new(),
                            )
                            .map(|v| m.update(key.to_string(), v))
                        })
                    })
                    .map(|map| {
                        MultiValue::new_from_statetree_value(
                            make_op_id.clone(),
                            StateTreeValue::Composite(StateTreeComposite::Map(StateTreeMap {
                                object_id: make_op_id.clone().into(),
                                props: map,
                            })),
                        )
                    })
            }
            Value::Map(props, amp::MapType::Table) => {
                let make_table_opid = amp::OpID::new(start_op, actor);
                let table = im::HashMap::new();
                let make_op = amp::Op {
                    action: amp::OpType::Make(amp::ObjType::table()),
                    obj: parent_id,
                    key: key.clone(),
                    insert,
                    pred,
                };
                let newvalue: NewValue<im::HashMap<String, MultiValue>> =
                    NewValue::init(table, make_op, start_op);
                props
                    .iter()
                    .fold(newvalue, |newvalue_so_far, (key, value)| {
                        let start_op = newvalue_so_far.max_op + 1;
                        newvalue_so_far.and_then(|t| {
                            MultiValue::new_from_value(
                                actor,
                                start_op,
                                make_table_opid.clone().into(),
                                &key.into(),
                                value,
                                false,
                                Vec::new(),
                            )
                            .map(|v| t.update(key.to_string(), v))
                        })
                    })
                    .map(|table| {
                        MultiValue::new_from_statetree_value(
                            make_table_opid.clone(),
                            StateTreeValue::Composite(StateTreeComposite::Table(StateTreeTable {
                                object_id: make_table_opid.clone().into(),
                                props: table,
                            })),
                        )
                    })
            }
            Value::Sequence(vals) => {
                let make_list_opid = amp::OpID::new(start_op, actor);
                let elems = DiffableSequence::new();
                let make_op = amp::Op {
                    action: amp::OpType::Make(amp::ObjType::list()),
                    obj: parent_id,
                    key: key.clone(),
                    insert,
                    pred,
                };
                let newvalue: NewValue<DiffableSequence<MultiValue>> =
                    NewValue::init(elems, make_op, start_op);
                vals.iter()
                    .fold(
                        (newvalue, amp::ElementID::Head),
                        |(newvalue_so_far, last_elemid), elem| {
                            let start_op = newvalue_so_far.max_op + 1;
                            let updated_newvalue = newvalue_so_far.and_then(|l| {
                                MultiValue::new_from_value(
                                    actor,
                                    start_op,
                                    make_list_opid.clone().into(),
                                    &last_elemid.clone().into(),
                                    elem,
                                    true,
                                    Vec::new(),
                                )
                                .map(|e| {
                                    let mut new_l = l.clone();
                                    new_l.push_back(e);
                                    new_l
                                })
                            });
                            (updated_newvalue, amp::OpID::new(start_op, actor).into())
                        },
                    )
                    .0
                    .map(|elems| {
                        MultiValue::new_from_statetree_value(
                            make_list_opid.clone(),
                            StateTreeValue::Composite(StateTreeComposite::List(StateTreeList {
                                object_id: make_list_opid.clone().into(),
                                elements: elems,
                            })),
                        )
                    })
            }
            Value::Text(chars) => {
                let make_text_opid = amp::OpID(start_op, actor.clone());
                let make_op = amp::Op {
                    action: amp::OpType::Make(amp::ObjType::text()),
                    obj: parent_id,
                    key: key.clone(),
                    insert,
                    pred,
                };
                let newvalue: NewValue<DiffableSequence<MultiChar>> =
                    NewValue::init(DiffableSequence::new(), make_op, start_op);
                chars
                    .iter()
                    .fold(
                        (newvalue, amp::ElementID::Head),
                        |(newvalue_so_far, last_elemid), c| {
                            let char_start_op = newvalue_so_far.max_op + 1;
                            let create_char_op = amp::OpID(char_start_op, actor.clone());
                            let updated_newvalue = newvalue_so_far.and_then(|newval_chars| {
                                NewValue::init(
                                    MultiChar::new_from_char(create_char_op.clone(), *c),
                                    amp::Op {
                                        action: amp::OpType::Set(amp::ScalarValue::Str(
                                            c.to_string(),
                                        )),
                                        obj: make_text_opid.clone().into(),
                                        key: last_elemid.clone().into(),
                                        insert: true,
                                        pred: Vec::new(),
                                    },
                                    char_start_op,
                                )
                                .map(|newchar| {
                                    let mut chars = newval_chars.clone();
                                    chars.push_back(newchar);
                                    chars
                                })
                            });
                            (
                                updated_newvalue,
                                amp::OpID::new(char_start_op, actor).into(),
                            )
                        },
                    )
                    .0
                    .map(|newchars| {
                        MultiValue::new_from_statetree_value(
                            make_text_opid.clone(),
                            StateTreeValue::Composite(StateTreeComposite::Text(StateTreeText {
                                object_id: make_text_opid.clone().into(),
                                chars: newchars,
                            })),
                        )
                    })
            }
            Value::Primitive(v) => {
                let make_op_id = amp::OpID(start_op, actor.clone());
                NewValue::init(
                    MultiValue::new_from_statetree_value(
                        make_op_id,
                        StateTreeValue::Leaf(v.clone()),
                    ),
                    amp::Op {
                        action: amp::OpType::Set(v.clone()),
                        obj: parent_id,
                        key: key.clone(),
                        insert,
                        pred,
                    },
                    start_op,
                )
            }
        }
    }

    pub(super) fn apply_diff(
        &self,
        opid: &amp::OpID,
        subdiff: &amp::Diff,
    ) -> Result<StateTreeChange<MultiValue>, error::InvalidPatch> {
        let current = self.tree_values();
        let update_for_opid = if let Some(existing_value) = current.get(opid) {
            match existing_value {
                StateTreeValue::Leaf(_) => StateTreeValue::new_from_diff(subdiff),
                StateTreeValue::Composite(composite) => composite
                    .apply_diff(subdiff)
                    .map(|value| value.map(StateTreeValue::Composite)),
            }
        } else {
            StateTreeValue::new_from_diff(subdiff)
        }?;
        update_for_opid.fallible_map(|update| {
            Self::multivalue_from_opids_and_values(current.update(opid.clone(), update))
        })
    }

    pub(super) fn apply_diff_iter<'a, 'b, I>(
        &'a self,
        diff: &mut I,
    ) -> Result<StateTreeChange<MultiValue>, error::InvalidPatch>
    where
        I: Iterator<Item = (&'b amp::OpID, &'b amp::Diff)>,
    {
        let init = Ok(StateTreeChange::pure(self.tree_values()));
        let updated = diff.fold(init, move |updated_so_far, (opid, subdiff)| {
            //let result_so_far = result_so_far?;
            updated_so_far?.fallible_and_then(|updated| {
                let update_for_opid = if let Some(existing_value) = updated.get(opid) {
                    match existing_value {
                        StateTreeValue::Leaf(_) => StateTreeValue::new_from_diff(subdiff),
                        StateTreeValue::Composite(composite) => composite
                            .apply_diff(subdiff)
                            .map(|value| value.map(StateTreeValue::Composite)),
                    }
                } else {
                    StateTreeValue::new_from_diff(subdiff)
                }?;
                Ok(update_for_opid.map(|u| updated.update(opid.clone(), u)))
            })
        })?;
        updated.fallible_map(Self::multivalue_from_opids_and_values)
    }

    pub(super) fn default_statetree_value(&self) -> StateTreeValue {
        self.winning_value.1.clone()
    }

    pub(super) fn default_value(&self) -> Value {
        self.winning_value.1.value()
    }

    pub(super) fn default_opid(&self) -> amp::OpID {
        self.winning_value.0.clone()
    }

    pub(super) fn update_default(&self, val: StateTreeValue) -> MultiValue {
        MultiValue {
            winning_value: (self.winning_value.0.clone(), val),
            conflicts: self.conflicts.clone(),
        }
    }

    fn tree_values(&self) -> im::HashMap<amp::OpID, StateTreeValue> {
        self.conflicts
            .update(self.winning_value.0.clone(), self.winning_value.1.clone())
    }

    pub(super) fn values(&self) -> std::collections::HashMap<amp::OpID, Value> {
        self.tree_values()
            .iter()
            .map(|(opid, v)| (opid.clone(), v.value()))
            .collect()
    }

    fn multivalue_from_opids_and_values<I>(
        opids_and_values: I,
    ) -> Result<MultiValue, error::InvalidPatch>
    where
        I: IntoIterator<Item = (amp::OpID, StateTreeValue)>,
    {
        let mut opids_and_values_vec: Vec<(amp::OpID, StateTreeValue)> =
            opids_and_values.into_iter().collect();
        opids_and_values_vec.sort_by(|(o1, _), (o2, _)| o1.cmp(o2));
        opids_and_values_vec.reverse();
        //updates_vec.sort_by_key(|(o, _)| o.clone());
        match opids_and_values_vec.split_first() {
            Some(((opid, value), rest)) => Ok(MultiValue {
                winning_value: (opid.clone(), value.clone()),
                conflicts: rest.into(),
            }),
            None => Err(error::InvalidPatch::DiffCreatedObjectWithNoValue),
        }
    }
}

pub(super) struct NewValue<T> {
    value: T,
    ops: Vec<amp::Op>,
    index_updates: im::HashMap<amp::ObjectID, StateTreeComposite>,
    max_op: u64,
}

impl<T> NewValue<T> {
    pub(super) fn state_tree_change(&self) -> StateTreeChange<T>
    where
        T: Clone,
    {
        StateTreeChange::pure(self.value.clone()).with_updates(Some(self.index_updates.clone()))
    }

    pub(super) fn ops(self) -> Vec<amp::Op> {
        self.ops
    }

    fn init(t: T, op: amp::Op, start_op: u64) -> NewValue<T> {
        NewValue {
            value: t,
            ops: vec![op],
            index_updates: im::HashMap::new(),
            max_op: start_op,
        }
    }

    fn and_then<F, G>(&self, f: F) -> NewValue<G>
    where
        T: Clone,
        F: Fn(T) -> NewValue<G>,
    {
        let newvalue = (f)(self.value.clone());
        let num_newops = newvalue.ops.len();
        let mut newops = self.ops.clone();
        newops.extend(newvalue.ops);
        NewValue {
            value: newvalue.value,
            ops: newops,
            index_updates: newvalue.index_updates.union(self.index_updates.clone()),
            max_op: self.max_op + (num_newops as u64),
        }
    }

    fn map<F, G>(self, f: F) -> NewValue<G>
    where
        F: Fn(T) -> G,
    {
        NewValue {
            value: f(self.value),
            ops: self.ops,
            index_updates: self.index_updates,
            max_op: self.max_op,
        }
    }
}

/// This struct exists to constrain the values of a text type to just containing
/// sequences of chars
#[derive(Debug, Clone)]
pub(super) struct MultiChar {
    winning_value: (amp::OpID, char),
    conflicts: im::HashMap<amp::OpID, char>,
}

impl MultiChar {
    pub(super) fn new_from_char(opid: amp::OpID, c: char) -> MultiChar {
        MultiChar {
            winning_value: (opid, c),
            conflicts: im::HashMap::new(),
        }
    }

    pub(super) fn new_from_diff(
        parent_object_id: &amp::ObjectID,
        opid: &amp::OpID,
        diff: &amp::Diff,
    ) -> Result<MultiChar, error::InvalidPatch> {
        let winning_value = match diff {
            amp::Diff::Value(amp::ScalarValue::Str(s)) => {
                if s.len() != 1 {
                    return Err(error::InvalidPatch::InsertNonTextInTextObject {
                        object_id: parent_object_id.clone(),
                        diff: diff.clone(),
                    });
                } else {
                    s.chars().next().unwrap()
                }
            }
            _ => {
                return Err(error::InvalidPatch::InsertNonTextInTextObject {
                    object_id: parent_object_id.clone(),
                    diff: diff.clone(),
                });
            }
        };
        Ok(MultiChar {
            winning_value: (opid.clone(), winning_value),
            conflicts: im::HashMap::new(),
        })
    }

    pub(super) fn apply_diff(
        &self,
        parent_object_id: &amp::ObjectID,
        opid: &amp::OpID,
        diff: &amp::Diff,
    ) -> Result<MultiChar, error::InvalidPatch> {
        let mut opids_and_values = self.values();
        match diff {
            amp::Diff::Value(amp::ScalarValue::Str(s)) => {
                if s.len() != 1 {
                    return Err(error::InvalidPatch::InsertNonTextInTextObject {
                        object_id: parent_object_id.clone(),
                        diff: diff.clone(),
                    });
                } else {
                    opids_and_values =
                        opids_and_values.update(opid.clone(), s.chars().next().unwrap());
                }
            }
            _ => {
                return Err(error::InvalidPatch::InsertNonTextInTextObject {
                    object_id: parent_object_id.clone(),
                    diff: diff.clone(),
                });
            }
        }
        Self::multichar_from_opids_and_values(opids_and_values)
    }

    pub(super) fn apply_diff_iter<'a, 'b, I>(
        &'a self,
        parent_object_id: &amp::ObjectID,
        diff: &mut I,
    ) -> Result<StateTreeChange<MultiChar>, error::InvalidPatch>
    where
        I: Iterator<Item = (&'b amp::OpID, &'b amp::Diff)>,
    {
        let init = Ok(StateTreeChange::pure(self.values()));
        let updated = diff.fold(init, move |updated_so_far, (opid, subdiff)| {
            updated_so_far?.fallible_map(|updated| match subdiff {
                amp::Diff::Value(amp::ScalarValue::Str(s)) => {
                    if s.len() != 1 {
                        Err(error::InvalidPatch::InsertNonTextInTextObject {
                            object_id: parent_object_id.clone(),
                            diff: subdiff.clone(),
                        })
                    } else {
                        let c = s.chars().next().unwrap();
                        Ok(updated.update(opid.clone(), c))
                    }
                }
                _ => {
                    Err(error::InvalidPatch::InsertNonTextInTextObject {
                        object_id: parent_object_id.clone(),
                        diff: subdiff.clone(),
                    })
                }
            })
        })?;
        updated.fallible_map(Self::multichar_from_opids_and_values)
    }

    pub(super) fn default_char(&self) -> char {
        self.winning_value.1
    }

    pub fn default_opid(&self) -> &amp::OpID {
        &self.winning_value.0
    }

    pub fn values(&self) -> im::HashMap<amp::OpID, char> {
        self.conflicts
            .update(self.winning_value.0.clone(), self.winning_value.1)
    }

    fn multichar_from_opids_and_values<I>(
        opids_and_values: I,
    ) -> Result<MultiChar, error::InvalidPatch>
    where
        I: IntoIterator<Item = (amp::OpID, char)>,
    {
        let mut opids_and_values_vec: Vec<(amp::OpID, char)> =
            opids_and_values.into_iter().collect();
        opids_and_values_vec.sort_by(|(o1, _), (o2, _)| o1.cmp(o2));
        opids_and_values_vec.reverse();
        //updates_vec.sort_by_key(|(o, _)| o.clone());
        match opids_and_values_vec.split_first() {
            Some(((opid, value), rest)) => Ok(MultiChar {
                winning_value: (opid.clone(), *value),
                conflicts: rest.into(),
            }),
            None => Err(error::InvalidPatch::DiffCreatedObjectWithNoValue),
        }
    }
}
