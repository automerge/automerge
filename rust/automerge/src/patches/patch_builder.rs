use core::fmt::Debug;
use std::sync::Arc;

use crate::marks::MarkSet;
use crate::{ObjId, Prop, ReadDoc, Value};

use super::{PatchAction, PatchWithAttribution};
use crate::{marks::Mark, sequence_tree::SequenceTree};

#[derive(Debug, Clone)]
pub(crate) struct PatchBuilder<'a, T: PartialEq> {
    patches: Vec<PatchWithAttribution<'a, T>>,
    last_mark_set: Option<Arc<MarkSet>>, // keep this around for a quick pointer equality test
}

impl<'a, T: PartialEq> Default for PatchBuilder<'a, T> {
    fn default() -> Self {
        Self {
            patches: vec![],
            last_mark_set: None,
        }
    }
}

impl<'a, T: PartialEq> PatchBuilder<'a, T> {
    pub(crate) fn get_path<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: &ObjId,
    ) -> Option<Vec<(ObjId, Prop)>> {
        match doc.parents(obj) {
            Ok(parents) => parents.visible_path(),
            Err(e) => {
                log!("error generating patch : {:?}", e);
                None
            }
        }
    }

    pub(crate) fn take_patches(&mut self) -> Vec<PatchWithAttribution<'a, T>> {
        std::mem::take(&mut self.patches)
    }

    pub(crate) fn insert<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        index: usize,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
        marks: Option<Arc<MarkSet>>,
    ) {
        let value = (tagged_value.0.to_owned(), tagged_value.1, conflict);
        if let Some(PatchAction::Insert {
            index: tail_index,
            values,
            ..
        }) = maybe_append(&mut self.patches, &obj)
        {
            let range = *tail_index..=*tail_index + values.len();
            if marks == self.last_mark_set && range.contains(&index) {
                values.insert(index - *tail_index, value);
                return;
            }
        }
        if let Some(path) = self.get_path(doc, &obj) {
            let mut values = SequenceTree::new();
            values.push(value);
            let action = PatchAction::Insert {
                index,
                values,
                marks: marks.as_deref().cloned(),
            };
            self.push(PatchWithAttribution {
                obj,
                path,
                action,
                attribute: None,
            });
            self.last_mark_set = marks;
        }
    }

    fn push(&mut self, patch: PatchWithAttribution<'a, T>) {
        self.patches.push(patch);
        self.last_mark_set = None;
    }

    pub(crate) fn splice_text<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        index: usize,
        value: &str,
        marks: Option<Arc<MarkSet>>,
        attribute: Option<&'a T>,
    ) {
        if let Some(PatchAction::SpliceText {
            index: tail_index,
            value: prev_value,
            ..
        }) = maybe_append2(&mut self.patches, &obj, attribute)
        {
            let range = *tail_index..=*tail_index + prev_value.len();
            if marks == self.last_mark_set && range.contains(&index) {
                let i = index - *tail_index;
                prev_value.splice(i, value);
                return;
            }
        }
        if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::SpliceText {
                index,
                value: value.into(),
                marks: marks.as_deref().cloned(),
            };
            //self.push(PatchWithAttribution { obj, path, action, attribute });
            self.push(PatchWithAttribution {
                obj,
                path,
                action,
                attribute,
            });
            self.last_mark_set = marks;
        }
    }

    pub(crate) fn delete_seq<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        index: usize,
        length: usize,
        value: String,
        attribute: Option<&'a T>,
    ) {
        match maybe_append2(&mut self.patches, &obj, attribute) {
            Some(PatchAction::SpliceText {
                index: tail_index,
                value,
                ..
            }) => {
                let range = *tail_index..*tail_index + value.len();
                if range.contains(&index) && range.contains(&(index + length - 1)) {
                    for _ in 0..length {
                        value.remove(index - *tail_index);
                    }
                    return;
                }
            }
            Some(PatchAction::Insert {
                index: tail_index,
                values,
                ..
            }) => {
                let range = *tail_index..*tail_index + values.len();
                if range.contains(&index) && range.contains(&(index + length - 1)) {
                    for _ in 0..length {
                        values.remove(index - *tail_index);
                    }
                    return;
                }
            }
            Some(PatchAction::DeleteSeq {
                index: tail_index,
                length: tail_length,
                value: tail_value,
                ..
            }) => {
                if index == *tail_index {
                    *tail_length += length;
                    tail_value.push_str(&value);
                    return;
                }
            }
            _ => {}
        }
        if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::DeleteSeq {
                index,
                length,
                value,
            };
            self.push(PatchWithAttribution {
                obj,
                path,
                action,
                attribute,
            })
        }
    }

    pub(crate) fn delete_map<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, key: &str) {
        if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::DeleteMap {
                key: key.to_owned(),
            };
            self.push(PatchWithAttribution {
                obj,
                path,
                action,
                attribute: None,
            })
        }
    }

    pub(crate) fn put<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        if let Some(path) = self.get_path(doc, &obj) {
            let value = (tagged_value.0.to_owned(), tagged_value.1);
            let action = match prop {
                Prop::Map(key) => PatchAction::PutMap {
                    key,
                    value,
                    conflict,
                },
                Prop::Seq(index) => PatchAction::PutSeq {
                    index,
                    value,
                    conflict,
                },
            };
            self.push(PatchWithAttribution {
                obj,
                path,
                action,
                attribute: None,
            })
        }
    }

    pub(crate) fn increment<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (i64, ObjId),
    ) {
        if let Some(path) = self.get_path(doc, &obj) {
            let value = tagged_value.0;
            let action = PatchAction::Increment { prop, value };
            self.push(PatchWithAttribution {
                obj,
                path,
                action,
                attribute: None,
            })
        }
    }

    pub(crate) fn mark<'x, 'y, R: ReadDoc, M: Iterator<Item = Mark<'y>>>(
        &mut self,
        doc: &'x R,
        obj: ObjId,
        mark: M,
    ) {
        if let Some(PatchAction::Mark { marks, .. }) = maybe_append(&mut self.patches, &obj) {
            for m in mark {
                marks.push(m.into_owned())
            }
            return;
        }
        if let Some(path) = self.get_path(doc, &obj) {
            let marks: Vec<_> = mark.map(|m| m.into_owned()).collect();
            if !marks.is_empty() {
                let action = PatchAction::Mark { marks };
                self.push(PatchWithAttribution {
                    obj,
                    path,
                    action,
                    attribute: None,
                });
            }
        }
    }

    pub(crate) fn flag_conflict<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, prop: Prop) {
        let conflict = match maybe_append(&mut self.patches, &obj) {
            Some(PatchAction::PutMap { key, conflict, .. })
                if Some(key.as_str()) == prop.as_str() =>
            {
                Some(conflict)
            }
            Some(PatchAction::PutSeq {
                index, conflict, ..
            }) if Some(*index) == prop.as_index() => Some(conflict),
            _ => None,
        };
        if let Some(conflict) = conflict {
            *conflict = true
        } else if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::Conflict { prop };
            self.push(PatchWithAttribution {
                obj,
                path,
                action,
                attribute: None,
            });
        }
    }
}

impl<'a, T: PartialEq> AsMut<PatchBuilder<'a, T>> for PatchBuilder<'a, T> {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

fn maybe_append<'a, T: PartialEq>(
    patches: &'a mut [PatchWithAttribution<'_, T>],
    obj: &ObjId,
) -> Option<&'a mut PatchAction> {
    match patches.last_mut() {
        Some(PatchWithAttribution {
            obj: tail_obj,
            action,
            ..
        }) if obj == tail_obj => Some(action),
        _ => None,
    }
}

fn maybe_append2<'a, T: PartialEq>(
    patches: &'a mut [PatchWithAttribution<'_, T>],
    obj: &ObjId,
    attr: Option<&T>,
) -> Option<&'a mut PatchAction> {
    match patches.last_mut() {
        Some(PatchWithAttribution {
            obj: tail_obj,
            attribute: tail_attr,
            action,
            ..
   //     }) if obj == tail_obj && _eq_ref(attr, *tail_attr) => Some(action),
        }) if obj == tail_obj && attr == *tail_attr => Some(action),
        _ => None,
    }
}

fn _eq_ref<T>(a: Option<&T>, b: Option<&T>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => std::ptr::eq(a, b),
        _ => false,
    }
}
