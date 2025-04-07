use super::{ListRange, ListRangeItem, MapRange, MapRangeItem, Span, Spans};
use crate::clock::Clock;
use crate::exid::ExId;
use crate::op_set2::op_set::{ObjIdIter, OpSet};
use crate::op_set2::types::ValueRef;
use crate::patches::TextRepresentation;
use crate::types::{ObjId, ObjMeta, ObjType};
use crate::Automerge;

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DocIter<'a> {
    op_set: Option<&'a OpSet>,
    next_objs: BTreeMap<ObjId, IterType>,
    obj_id_iter: ObjIdIter<'a>,
    map_iter: MapRange<'a>,
    list_iter: ListRange<'a>,
    span_iter: Spans<'a>,
    iter_type: IterType,
    obj: ObjId,
    obj_export: Arc<ExId>,
    text_rep: TextRepresentation,
}

impl Default for DocIter<'_> {
    fn default() -> Self {
        Self {
            op_set: None,
            next_objs: BTreeMap::default(),
            obj_id_iter: ObjIdIter::default(),
            map_iter: MapRange::default(),
            list_iter: ListRange::default(),
            span_iter: Spans::default(),
            iter_type: IterType::Map,
            obj: ObjId::root(),
            obj_export: Arc::new(ExId::Root),
            text_rep: TextRepresentation::Array,
        }
    }
}

impl<'a> DocIter<'a> {
    pub(crate) fn new(
        doc: &'a Automerge,
        obj: ObjMeta,
        clock: Option<Clock>,
        text_rep: TextRepresentation,
    ) -> Self {
        let op_set = doc.ops();
        let next_objs = BTreeMap::new();
        let mut obj_id_iter = op_set.obj_id_iter();
        //let exid = op_set.id_to_exid(obj.id.0);
        let iter_type = IterType::new(text_rep, obj.typ);
        let obj = obj.id;
        let obj_export = Arc::new(op_set.id_to_exid(obj.0));
        let scope = obj_id_iter.seek_to_value(obj);
        let map_iter = MapRange::new(op_set, scope.clone(), clock.clone());
        let list_iter = ListRange::new(op_set, scope.clone(), clock.clone(), ..);
        let span_iter = Spans::new(op_set, scope, clock, doc.text_encoding());
        let op_set = Some(op_set);
        Self {
            map_iter,
            list_iter,
            span_iter,
            op_set,
            obj,
            obj_export,
            iter_type,
            obj_id_iter,
            next_objs,
            text_rep,
        }
    }
}

impl<'a> DocIter<'a> {
    fn process_item(&mut self, item: DocItem<'a>) -> Option<DocObjItem<'a>> {
        if let Some((next_obj, next_typ)) = item.make_obj(self.text_rep) {
            self.next_objs.insert(next_obj, next_typ);
        }
        Some(DocObjItem {
            obj: self.obj_export.clone(),
            item,
        })
    }

    fn shift(&mut self, next_type: IterType, next_range: Range<usize>) -> Option<DocItem<'a>> {
        match next_type {
            IterType::Map => Some(DocItem::Map(self.map_iter.shift_next(next_range)?)),
            IterType::List => Some(DocItem::List(self.list_iter.shift_next(next_range)?)),
            IterType::Text => Some(DocItem::Text(self.span_iter.shift_next(next_range)?)),
        }
    }

    fn next_object(&mut self) -> Option<Option<DocObjItem<'a>>> {
        let (next, next_type) = self.next_objs.pop_first()?;
        let next_range = self.obj_id_iter.seek_to_value(next);
        if next_range.is_empty() {
            Some(None)
        } else {
            if let Some(item) = self.shift(next_type, next_range) {
                self.obj = next;
                self.obj_export = Arc::new(self.op_set?.id_to_exid(next.0));
                self.iter_type = next_type;
                Some(self.process_item(item))
            } else {
                Some(None)
            }
        }
    }

    fn next_prop(&mut self) -> Option<DocObjItem<'a>> {
        match self.iter_type {
            IterType::Map => {
                let map = DocItem::Map(self.map_iter.next()?);
                self.process_item(map)
            }
            IterType::List => {
                let list = DocItem::List(self.list_iter.next()?);
                self.process_item(list)
            }
            IterType::Text => {
                let span = DocItem::Text(self.span_iter.next()?);
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
    fn new(text_rep: TextRepresentation, obj_type: ObjType) -> Self {
        match (obj_type, text_rep) {
            (ObjType::Text, TextRepresentation::String(_)) => IterType::Text,
            (ObjType::Map, _) => IterType::Map,
            _ => IterType::List,
        }
    }
}

impl<'a> Iterator for DocIter<'a> {
    type Item = DocObjItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.next_prop() {
            return Some(item);
        }
        // could rewrite this as an iterator
        loop {
            if let Some(item) = self.next_object()? {
                return Some(item);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct DocObjItem<'a> {
    pub obj: Arc<ExId>,
    pub item: DocItem<'a>,
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

#[derive(Debug, Clone)]
pub enum DocItem<'a> {
    Map(MapRangeItem<'a>),
    List(ListRangeItem<'a>),
    Text(Span),
}

impl<'a> DocItem<'a> {
    pub fn id(&self) -> Option<ExId> {
        match self {
            DocItem::Map(m) => Some(m.id()),
            DocItem::List(l) => Some(l.id()),
            _ => None,
        }
    }

    fn make_obj(&self, text_rep: TextRepresentation) -> Option<(ObjId, IterType)> {
        match self {
            DocItem::Map(MapRangeItem {
                value: ValueRef::Object(ot),
                maybe_exid,
                ..
            }) => {
                let new_typ = IterType::new(text_rep, *ot);
                let new_obj = ObjId(maybe_exid.id);
                Some((new_obj, new_typ))
            }
            DocItem::List(ListRangeItem {
                value: ValueRef::Object(ot),
                maybe_exid,
                ..
            }) => {
                let new_typ = IterType::new(text_rep, *ot);
                let new_obj = ObjId(maybe_exid.id);
                Some((new_obj, new_typ))
            }
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
