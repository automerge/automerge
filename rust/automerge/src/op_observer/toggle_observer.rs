#![allow(dead_code)]

use crate::ChangeHash;
use core::fmt::Debug;

use crate::{marks::Mark, ObjId, OpObserver, Prop, ReadDoc, Value};

use crate::op_observer::BranchableObserver;
use crate::op_observer::{HasPatches, ObserverContext, TextRepresentation};

#[derive(Debug, Clone)]
pub struct ToggleObserver<T> {
    enabled: bool,
    last_heads: Option<Vec<ChangeHash>>,
    observer: T,
}

impl<T: Default> Default for ToggleObserver<T> {
    fn default() -> Self {
        Self {
            enabled: false,
            last_heads: None,
            observer: T::default(),
        }
    }
}

impl<T: HasPatches> ToggleObserver<T> {
    pub fn new(observer: T) -> Self {
        ToggleObserver {
            enabled: false,
            last_heads: None,
            observer,
        }
    }

    pub fn take_patches(&mut self, heads: Vec<ChangeHash>) -> (T::Patches, Vec<ChangeHash>) {
        let old_heads = self.last_heads.replace(heads).unwrap_or_default();
        let patches = self.observer.take_patches();
        (patches, old_heads)
    }

    pub fn with_text_rep(mut self, text_rep: TextRepresentation) -> Self {
        self.observer = self.observer.with_text_rep(text_rep);
        self
    }

    pub fn set_text_rep(&mut self, text_rep: TextRepresentation) {
        self.observer.set_text_rep(text_rep)
    }

    pub fn enable(&mut self, enable: bool, heads: Vec<ChangeHash>) -> bool {
        if self.enabled && !enable {
            self.observer.take_patches();
            self.last_heads = Some(heads);
        }
        let old_enabled = self.enabled;
        self.enabled = enable;
        old_enabled
    }

    fn get_path<R: ReadDoc>(&mut self, doc: &R, obj: &ObjId) -> Option<Vec<(ObjId, Prop)>> {
        match doc.parents(obj) {
            Ok(parents) => parents.visible_path(),
            Err(e) => {
                log!("error generating patch : {:?}", e);
                None
            }
        }
    }
}

impl<T: OpObserver + HasPatches> OpObserver for ToggleObserver<T> {
    fn insert<R: ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        obj: ObjId,
        index: usize,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        if self.enabled {
            self.observer
                .insert(doc, ctx, obj, index, tagged_value, conflict)
        }
    }

    fn splice_text<R: ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        obj: ObjId,
        index: usize,
        value: &str,
    ) {
        if self.enabled {
            self.observer.splice_text(doc, ctx, obj, index, value)
        }
    }

    fn delete_seq<R: ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        obj: ObjId,
        index: usize,
        length: usize,
    ) {
        if self.enabled {
            self.observer.delete_seq(doc, ctx, obj, index, length)
        }
    }

    fn delete_map<R: ReadDoc>(&mut self, doc: &R, ctx: ObserverContext, obj: ObjId, key: &str) {
        if self.enabled {
            self.observer.delete_map(doc, ctx, obj, key)
        }
    }

    fn put<R: ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        if self.enabled {
            self.observer
                .put(doc, ctx, obj, prop, tagged_value, conflict)
        }
    }

    fn expose<R: ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        if self.enabled {
            self.observer
                .expose(doc, ctx, obj, prop, tagged_value, conflict)
        }
    }

    fn increment<R: ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        obj: ObjId,
        prop: Prop,
        tagged_value: (i64, ObjId),
    ) {
        if self.enabled {
            self.observer.increment(doc, ctx, obj, prop, tagged_value)
        }
    }

    fn mark<'a, R: ReadDoc, M: Iterator<Item = Mark<'a>>>(
        &mut self,
        doc: &'a R,
        ctx: ObserverContext,
        obj: ObjId,
        mark: M,
    ) {
        if self.enabled {
            self.observer.mark(doc, ctx, obj, mark)
        }
    }

    fn unmark<R: ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        obj: ObjId,
        name: &str,
        start: usize,
        end: usize,
    ) {
        if self.enabled {
            self.observer.unmark(doc, ctx, obj, name, start, end)
        }
    }

    fn text_as_seq(&self) -> bool {
        self.observer.get_text_rep() == TextRepresentation::Array
    }
}

impl<T: BranchableObserver> BranchableObserver for ToggleObserver<T> {
    fn merge(&mut self, other: &Self) {
        self.observer.merge(&other.observer)
    }

    fn branch(&self) -> Self {
        ToggleObserver {
            observer: self.observer.branch(),
            last_heads: None,
            enabled: self.enabled,
        }
    }
}
