use super::{
    ListDiff, ListDiffItem, ListRange, ListRangeItem, MapDiff, MapDiffItem, MapRange, MapRangeItem,
    Span, SpanDiff, SpanInternal, SpansDiff, SpansInternal,
};
use crate::clock::{Clock, ClockRange};
use crate::exid::ExId;
use crate::op_set2::op_set::{ObjIdIter, OpSet};
use crate::op_set2::types::ValueRef;
use crate::patches::PatchLog;
use crate::types::{ObjId, ObjMeta, ObjType, Prop};
use crate::Automerge;
use crate::TextEncoding;

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DocIter<'a> {
    op_set: Option<&'a OpSet>,
    obj_export: Arc<ExId>,
    inner: DocIterInternal<'a>,
}

impl<'a> DocIter<'a> {
    pub(crate) fn empty(encoding: TextEncoding) -> Self {
        Self {
            op_set: None,
            obj_export: Arc::new(ExId::Root),
            inner: DocIterInternal::empty(encoding),
        }
    }

    fn encoding(&self) -> TextEncoding {
        self.inner.span_iter.encoding()
    }

    fn clock(&self) -> Option<&Clock> {
        self.inner.span_iter.clock()
    }

    pub(crate) fn new(doc: &'a Automerge, obj: ObjMeta, clock: Option<Clock>) -> Self {
        let obj_export = Arc::new(doc.ops().id_to_exid(obj.id.0));
        let op_set = Some(doc.ops());
        Self {
            op_set,
            obj_export,
            inner: DocIterInternal::new(doc, obj, clock),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DocIterInternal<'a> {
    next_objs: BTreeMap<ObjId, IterType>,
    pub(crate) path_map: BTreeMap<ObjId, (Prop, ObjId)>,
    obj_id_iter: ObjIdIter<'a>,
    map_iter: MapRange<'a>,
    list_iter: ListRange<'a>,
    span_iter: SpansInternal<'a>,
    iter_type: IterType,
    obj: ObjId,
}

#[derive(Debug, Clone)]
pub(crate) struct DiffIter<'a> {
    next_objs: BTreeMap<ObjId, IterType>,
    path_map: BTreeMap<ObjId, (Prop, ObjId)>,
    obj_id_iter: ObjIdIter<'a>,
    map_iter: MapDiff<'a>,
    list_iter: ListDiff<'a>,
    span_iter: SpansDiff<'a>,
    iter_type: IterType,
    obj: ObjId,
}

impl<'a> DiffIter<'a> {
    pub(crate) fn log(
        doc: &'a Automerge,
        obj: ObjMeta,
        clock: ClockRange,
        log: &mut PatchLog,
    ) -> BTreeMap<ObjId, (Prop, ObjId)> {
        let encoding = doc.text_encoding();
        let mut iter = DiffIter::new(doc, obj, clock);
        for item in iter.by_ref() {
            item.log(log, encoding);
        }
        iter.path_map
    }

    pub(crate) fn new(doc: &'a Automerge, obj: ObjMeta, clock: ClockRange) -> Self {
        let op_set = doc.ops();
        let iter_type = IterType::new(obj.typ);
        let obj = obj.id;
        let mut obj_id_iter = op_set.obj_id_iter();
        let scope = obj_id_iter.seek_to_value(obj);
        let map_iter = MapDiff::new(op_set, scope.clone(), clock.clone());
        let list_iter = ListDiff::new(op_set, scope.clone(), clock.clone());
        let span_iter = SpansDiff::new(op_set, scope, clock, doc.text_encoding());
        let path_map = BTreeMap::new();
        let next_objs = BTreeMap::new();
        DiffIter {
            map_iter,
            list_iter,
            span_iter,
            obj,
            iter_type,
            obj_id_iter,
            next_objs,
            path_map,
        }
    }

    fn process_item(&mut self, item: DocDiffItem<'a>) -> Option<DocObjDiffItem<'a>> {
        if let Some((next_obj, next_typ)) = item.make_obj() {
            let prop = item.prop();
            self.next_objs.insert(next_obj, next_typ);
            self.path_map.insert(next_obj, (prop, self.obj));
        }
        Some(DocObjDiffItem {
            obj: self.obj,
            item,
        })
    }

    fn shift(&mut self, next_type: IterType, next_range: Range<usize>) -> Option<DocDiffItem<'a>> {
        match next_type {
            IterType::Map => Some(DocDiffItem::Map(self.map_iter.shift_next(next_range)?)),
            IterType::List => Some(DocDiffItem::List(self.list_iter.shift_next(next_range)?)),
            IterType::Text => Some(DocDiffItem::Text(self.span_iter.shift_next(next_range)?)),
        }
    }

    fn next_object(&mut self) -> Option<Option<DocObjDiffItem<'a>>> {
        let (next, next_type) = self.next_objs.pop_first()?;
        let next_range = self.obj_id_iter.seek_to_value(next);
        if next_range.is_empty() {
            Some(None)
        } else if let Some(item) = self.shift(next_type, next_range) {
            self.obj = next;
            self.iter_type = next_type;
            Some(self.process_item(item))
        } else {
            Some(None)
        }
    }

    fn next_prop(&mut self) -> Option<DocObjDiffItem<'a>> {
        match self.iter_type {
            IterType::Map => {
                let map = DocDiffItem::Map(self.map_iter.next()?);
                self.process_item(map)
            }
            IterType::List => {
                let list = DocDiffItem::List(self.list_iter.next()?);
                self.process_item(list)
            }
            IterType::Text => {
                let span = DocDiffItem::Text(self.span_iter.next()?);
                self.process_item(span)
            }
        }
    }
}

impl<'a> DocIterInternal<'a> {
    fn new(doc: &'a Automerge, obj: ObjMeta, clock: Option<Clock>) -> Self {
        let op_set = doc.ops();
        let iter_type = IterType::new(obj.typ);
        let obj = obj.id;
        let mut obj_id_iter = op_set.obj_id_iter();
        let scope = obj_id_iter.seek_to_value(obj);
        let map_iter = MapRange::new(op_set, scope.clone(), clock.clone());
        let list_iter = ListRange::new(op_set, scope.clone(), clock.clone(), ..);
        let span_iter = SpansInternal::new(op_set, scope, clock, doc.text_encoding());
        let path_map = BTreeMap::new();
        let next_objs = BTreeMap::new();
        DocIterInternal {
            map_iter,
            list_iter,
            span_iter,
            obj,
            iter_type,
            obj_id_iter,
            next_objs,
            path_map,
        }
    }

    fn empty(encoding: TextEncoding) -> Self {
        Self {
            next_objs: BTreeMap::default(),
            path_map: BTreeMap::default(),
            obj_id_iter: ObjIdIter::default(),
            map_iter: MapRange::default(),
            list_iter: ListRange::default(),
            span_iter: SpansInternal::empty(encoding),
            iter_type: IterType::Map,
            obj: ObjId::root(),
        }
    }

    fn process_item(&mut self, item: DocItemInternal<'a>) -> Option<DocObjItemInternal<'a>> {
        if let Some((next_obj, next_typ)) = item.make_obj() {
            let prop = item.prop();
            self.next_objs.insert(next_obj, next_typ);
            self.path_map.insert(next_obj, (prop, self.obj));
        }
        Some(DocObjItemInternal {
            obj: self.obj,
            item,
        })
    }

    fn shift(
        &mut self,
        next_type: IterType,
        next_range: Range<usize>,
    ) -> Option<DocItemInternal<'a>> {
        match next_type {
            IterType::Map => Some(DocItemInternal::Map(self.map_iter.shift_next(next_range)?)),
            IterType::List => Some(DocItemInternal::List(
                self.list_iter.shift_next(next_range)?,
            )),
            IterType::Text => Some(DocItemInternal::Text(
                self.span_iter.shift_next(next_range)?,
            )),
        }
    }

    fn next_object(&mut self) -> Option<Option<DocObjItemInternal<'a>>> {
        let (next, next_type) = self.next_objs.pop_first()?;
        let next_range = self.obj_id_iter.seek_to_value(next);
        if next_range.is_empty() {
            Some(None)
        } else if let Some(item) = self.shift(next_type, next_range) {
            self.obj = next;
            self.iter_type = next_type;
            Some(self.process_item(item))
        } else {
            Some(None)
        }
    }

    fn next_prop(&mut self) -> Option<DocObjItemInternal<'a>> {
        match self.iter_type {
            IterType::Map => {
                let map = DocItemInternal::Map(self.map_iter.next()?);
                self.process_item(map)
            }
            IterType::List => {
                let list = DocItemInternal::List(self.list_iter.next()?);
                self.process_item(list)
            }
            IterType::Text => {
                let span = DocItemInternal::Text(self.span_iter.next()?);
                self.process_item(span)
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum IterType {
    Map,
    List,
    Text,
}

impl IterType {
    fn new(obj_type: ObjType) -> Self {
        match obj_type {
            ObjType::Text => IterType::Text,
            ObjType::Map => IterType::Map,
            _ => IterType::List,
        }
    }
}

impl<'a> Iterator for DocIter<'a> {
    type Item = DocObjItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let DocObjItemInternal { obj, item } = self.inner.next()?;
        if *self.obj_export != obj {
            self.obj_export = Arc::new(self.op_set?.id_to_exid(self.inner.obj.0));
        }
        Some(DocObjItem {
            obj: self.obj_export.clone(),
            item: item.export(self.op_set?, self.clock(), self.encoding()),
        })
    }
}

impl<'a> Iterator for DiffIter<'a> {
    type Item = DocObjDiffItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.next_prop() {
            return Some(item);
        }
        loop {
            if let Some(item) = self.next_object()? {
                return Some(item);
            }
        }
    }
}

impl<'a> Iterator for DocIterInternal<'a> {
    type Item = DocObjItemInternal<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.next_prop() {
            return Some(item);
        }
        loop {
            if let Some(item) = self.next_object()? {
                return Some(item);
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct DocObjItem<'a> {
    pub obj: Arc<ExId>,
    pub item: DocItem<'a>,
}

#[derive(Debug, Clone)]
pub(crate) struct DocObjItemInternal<'a> {
    pub(crate) obj: ObjId,
    pub(crate) item: DocItemInternal<'a>,
}

#[derive(Debug, Clone)]
pub(crate) struct DocObjDiffItem<'a> {
    pub(crate) obj: ObjId,
    pub(crate) item: DocDiffItem<'a>,
}

impl DocObjDiffItem<'_> {
    pub(crate) fn log(self, log: &mut PatchLog, encoding: TextEncoding) {
        match self.item {
            DocDiffItem::Map(m) => m.log(self.obj, log, encoding),
            DocDiffItem::List(l) => l.log(self.obj, log, encoding),
            DocDiffItem::Text(t) => t.log(self.obj, log, encoding),
        }
    }
}

impl<'a> DocObjItem<'a> {
    pub fn key(&self) -> Option<&str> {
        if let DocItem::Map(MapRangeItem { key, .. }) = &self.item {
            Some(key)
        } else {
            None
        }
    }

    pub fn value(&self) -> Option<ValueRef<'a>> {
        match &self.item {
            DocItem::Map(MapRangeItem { value, .. }) => Some(value.clone()),
            DocItem::List(ListRangeItem { value, .. }) => Some(value.clone()),
            DocItem::Text(span) => Some(ValueRef::str(span.as_str()).into_owned()),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum DocItem<'a> {
    Map(MapRangeItem<'a>),
    List(ListRangeItem<'a>),
    Text(Span),
}

#[derive(Debug, Clone)]
pub(crate) enum DocItemInternal<'a> {
    Map(MapRangeItem<'a>),
    List(ListRangeItem<'a>),
    Text(SpanInternal),
}

#[derive(Debug, Clone)]
pub(crate) enum DocDiffItem<'a> {
    Map(MapDiffItem<'a>),
    List(ListDiffItem<'a>),
    Text(SpanDiff),
}

impl DocDiffItem<'_> {
    fn prop(&self) -> Prop {
        match self {
            Self::Map(m) => Prop::Map(m.key.to_string()),
            Self::List(l) => Prop::Seq(l.index),
            Self::Text(SpanDiff {
                span: SpanInternal::Obj(_, index, _),
                ..
            }) => Prop::Seq(*index),
            Self::Text(SpanDiff {
                span: SpanInternal::Text(_, index, _),
                ..
            }) => Prop::Seq(*index),
        }
    }

    fn make_obj(&self) -> Option<(ObjId, IterType)> {
        match self {
            Self::Map(MapDiffItem {
                value: ValueRef::Object(ot),
                diff,
                id,
                ..
            }) if diff.is_visible() => {
                let new_typ = IterType::new(*ot);
                let new_obj = ObjId(*id);
                Some((new_obj, new_typ))
            }
            Self::List(ListDiffItem {
                value: ValueRef::Object(ot),
                diff,
                id,
                ..
            }) if diff.is_visible() => {
                let new_typ = IterType::new(*ot);
                let new_obj = ObjId(*id);
                Some((new_obj, new_typ))
            }
            Self::Text(SpanDiff {
                span: SpanInternal::Obj(id, _, _),
                diff,
            }) if diff.is_visible() => {
                let new_typ = IterType::new(ObjType::Map);
                let new_obj = ObjId(*id);
                Some((new_obj, new_typ))
            }
            _ => None,
        }
    }
}

impl<'a> DocItemInternal<'a> {
    fn prop(&self) -> Prop {
        match self {
            Self::Map(m) => Prop::Map(m.key.to_string()),
            Self::List(l) => Prop::Seq(l.index),
            Self::Text(SpanInternal::Obj(_, index, _)) => Prop::Seq(*index),
            Self::Text(SpanInternal::Text(_, index, _)) => Prop::Seq(*index),
        }
    }

    fn export(
        self,
        op_set: &'a OpSet,
        clock: Option<&Clock>,
        encoding: TextEncoding,
    ) -> DocItem<'a> {
        match self {
            Self::Map(m) => DocItem::Map(m),
            Self::List(l) => DocItem::List(l),
            Self::Text(t) => DocItem::Text(t.export(op_set, clock, encoding)),
        }
    }

    fn make_obj(&self) -> Option<(ObjId, IterType)> {
        match self {
            DocItemInternal::Map(MapRangeItem {
                value: ValueRef::Object(ot),
                maybe_exid,
                ..
            }) => {
                let new_typ = IterType::new(*ot);
                let new_obj = ObjId(maybe_exid.id);
                Some((new_obj, new_typ))
            }
            DocItemInternal::List(ListRangeItem {
                value: ValueRef::Object(ot),
                maybe_exid,
                ..
            }) => {
                let new_typ = IterType::new(*ot);
                let new_obj = ObjId(maybe_exid.id);
                Some((new_obj, new_typ))
            }
            DocItemInternal::Text(SpanInternal::Obj(id, _, _)) => {
                let new_typ = IterType::new(ObjType::Map);
                let new_obj = ObjId(*id);
                Some((new_obj, new_typ))
            }
            _ => None,
        }
    }
}

impl<'a> DocItem<'a> {
    pub fn id(&self) -> Option<ExId> {
        match self {
            DocItem::Map(m) => Some(m.id()),
            DocItem::List(l) => Some(l.id()),
            _ => None,
        }
    }

    pub fn value(&self) -> Option<&ValueRef<'a>> {
        match self {
            DocItem::Map(MapRangeItem { value, .. }) => Some(value),
            DocItem::List(ListRangeItem { value, .. }) => Some(value),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::Transactable;
    use crate::{Automerge, ObjType, ReadDoc, ROOT};

    #[test]
    fn doc_iter() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.put(&ROOT, "key01", "value1").unwrap();
        tx.put(&ROOT, "key02", "value2").unwrap();
        let map1 = tx.put_object(&ROOT, "key03", ObjType::Map).unwrap();
        tx.put(&ROOT, "key04", "value4").unwrap();
        let _map2 = tx.put_object(&ROOT, "key05", ObjType::Map).unwrap();
        let map3 = tx.put_object(&ROOT, "key06", ObjType::Map).unwrap(); // deleted
        let map4 = tx.put_object(&ROOT, "key07", ObjType::Map).unwrap();
        let list1 = tx.put_object(&ROOT, "key08", ObjType::List).unwrap();
        let _map5 = tx.put_object(&ROOT, "key09", ObjType::Map).unwrap();
        let map6 = tx.put_object(&ROOT, "key10", ObjType::Map).unwrap();
        let list2 = tx.put_object(&ROOT, "key11", ObjType::List).unwrap();
        let _map7 = tx.put_object(&ROOT, "key12", ObjType::Map).unwrap();
        let text1 = tx.put_object(&ROOT, "key13", ObjType::Text).unwrap();
        tx.splice_text(&text1, 0, 0, "hello world").unwrap();
        tx.put(&map1, "m1key1", "m1value1").unwrap();
        tx.put(&map3, "m3key1", "m3value1").unwrap();
        tx.put(&map3, "m3key2", "m3value2").unwrap();
        tx.put(&map3, "m3key3", "m3value3").unwrap();
        tx.put(&map4, "m4key1", "m4value1").unwrap();
        tx.put(&map4, "m4key2", "m4value2").unwrap();
        tx.insert(&list1, 0, "l1e1").unwrap();
        tx.insert(&list1, 1, "l1e2").unwrap();
        tx.insert(&list1, 2, "l1e3").unwrap();
        tx.put(&map6, "m6key1", "m6value1").unwrap();
        tx.put(&map6, "m6key2", "m6value2").unwrap();
        tx.put(&map6, "m6key3", "m6value3").unwrap();
        tx.insert(&list2, 0, "l2e1").unwrap();
        tx.delete(&ROOT, "key06").unwrap();
        tx.delete(&map6, "m6key2").unwrap();
        tx.commit();

        doc.dump();
        let props: Vec<_> = doc.iter().collect();

        let values: Vec<_> = props
            .iter()
            .map(|p| (p.key().unwrap_or(""), p.value().unwrap()))
            .collect();

        let answers = vec![
            ("key01", "value1".into()),
            ("key02", "value2".into()),
            ("key03", ValueRef::Object(ObjType::Map)),
            ("key04", "value4".into()),
            ("key05", ValueRef::Object(ObjType::Map)),
            ("key07", ValueRef::Object(ObjType::Map)),
            ("key08", ValueRef::Object(ObjType::List)),
            ("key09", ValueRef::Object(ObjType::Map)),
            ("key10", ValueRef::Object(ObjType::Map)),
            ("key11", ValueRef::Object(ObjType::List)),
            ("key12", ValueRef::Object(ObjType::Map)),
            ("key13", ValueRef::Object(ObjType::Text)),
            ("m1key1", "m1value1".into()),
            ("m4key1", "m4value1".into()),
            ("m4key2", "m4value2".into()),
            ("", "l1e1".into()),
            ("", "l1e2".into()),
            ("", "l1e3".into()),
            ("m6key1", "m6value1".into()),
            ("m6key3", "m6value3".into()),
            ("", "l2e1".into()),
            ("", "hello world".into()),
        ];
        let max = std::cmp::max(answers.len(), values.len());
        for i in 0..max {
            assert_eq!(values.get(i), answers.get(i));
        }
    }
}
