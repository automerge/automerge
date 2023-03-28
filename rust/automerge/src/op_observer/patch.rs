#![allow(dead_code)]

use crate::{marks::Mark, ObjId, ObserverContext, Prop, Value};
use core::fmt::Debug;

use crate::sequence_tree::SequenceTree;

#[derive(Debug, Clone, PartialEq)]
pub struct Patch<T: PartialEq + Clone + Debug> {
    pub obj: ObjId,
    pub path: Vec<(ObjId, Prop)>,
    pub ctx: ObserverContext,
    pub action: PatchAction<T>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PatchAction<T: PartialEq + Clone + Debug> {
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
        value: SequenceTree<T>,
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
    Unmark {
        name: String,
        start: usize,
        end: usize,
    },
}
