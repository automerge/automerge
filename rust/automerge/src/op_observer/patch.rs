#![allow(dead_code)]

use crate::{marks::Mark, ObjId, Prop, Value};
use core::fmt::Debug;

use crate::sequence_tree::SequenceTree;
use crate::text_value::TextValue;

#[derive(Debug, Clone, PartialEq)]
pub struct Patch {
    pub obj: ObjId,
    pub path: Vec<(ObjId, Prop)>,
    pub action: PatchAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PatchAction {
    PutMap {
        key: String,
        value: (Value<'static>, ObjId),
        expose: bool,
        conflict: bool,
    },
    PutSeq {
        index: usize,
        value: (Value<'static>, ObjId),
        expose: bool,
        conflict: bool,
    },
    Insert {
        index: usize,
        values: SequenceTree<(Value<'static>, ObjId)>,
        conflict: bool,
    },
    SpliceText {
        index: usize,
        value: TextValue,
    },
    Increment {
        prop: Prop,
        value: i64,
    },
    DeleteMap {
        key: String,
    },
    DeleteSeq {
        index: usize,
        length: usize,
    },
    Mark {
        marks: Vec<Mark<'static>>,
    },
}
