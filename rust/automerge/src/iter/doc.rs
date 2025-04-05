use super::{ListRange, ListRangeItem, MapRange, MapRangeItem, Span, SpansInternal};
use crate::clock::Clock;
use crate::exid::ExId;
use crate::op_set2::op_set::{ObjIdIter, OpSet};
use crate::patches::TextRepresentation;
use crate::types::{ObjId, ObjMeta, ObjType, TextEncoding};
use crate::value::Value;
use crate::Automerge;

use std::collections::BTreeMap;
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct DocIter<'a> {
    op_set: Option<&'a OpSet>,
    next_objs: BTreeMap<ObjId, IterType>,
    obj_id_iter: ObjIdIter<'a>,
    map_iter: MapRange<'a>,
    list_iter: ListRange<'a>,
    span_iter: SpansInternal<'a>,
    iter_type: IterType,
    obj: ObjId,
    encoding: TextRepresentation,
}

impl Default for DocIter<'_> {
    fn default() -> Self {
        Self {
            op_set: None,
            next_objs: BTreeMap::default(),
            obj_id_iter: ObjIdIter::default(),
            map_iter: MapRange::default(),
            list_iter: ListRange::default(),
            span_iter: SpansInternal::default(),
            iter_type: IterType::Map,
            obj: ObjId::root(),
            encoding: TextRepresentation::Array,
        }
    }
}

impl<'a> DocIter<'a> {
    pub(crate) fn new(
        doc: &'a Automerge,
        obj: ObjMeta,
        clock: Option<Clock>,
        encoding: TextRepresentation,
    ) -> Self {
        let op_set = doc.ops();
        let next_objs = BTreeMap::new();
        let mut obj_id_iter = op_set.obj_id_iter();
        //let exid = op_set.id_to_exid(obj.id.0);
        let iter_type = IterType::new(encoding, obj.typ);
        let obj = obj.id;
        let scope = obj_id_iter.seek_to_value(obj);
        let map_iter = MapRange::new(op_set, scope.clone(), clock.clone());
        let list_iter = ListRange::new(op_set, scope.clone(), clock.clone(), ..);
        let span_iter = SpansInternal::new(op_set, scope, clock, doc.text_encoding());
        let op_set = Some(op_set);
        Self {
            map_iter,
            list_iter,
            span_iter,
            op_set,
            obj,
            iter_type,
            obj_id_iter,
            next_objs,
            encoding,
        }
    }
}

impl<'a> DocIter<'a> {
    #[inline(never)]
    fn process_map_item(&mut self, item: Option<MapRangeItem<'a>>) -> Option<ObjItem<'a>> {
        let item = item?;
        if let Value::Object(ot) = &item.value {
            let next_obj = ObjId(item._id);
            let next_typ = IterType::new(self.encoding, *ot);
            self.next_objs.insert(next_obj, next_typ);
        }
        Some(ObjItem {
            obj: self.op_set?.id_to_exid(self.obj.0),
            item: DocItem::Map(item),
        })
    }

    #[inline(never)]
    fn process_list_item(&mut self, item: Option<ListRangeItem<'a>>) -> Option<ObjItem<'a>> {
        let item = item?;
        if let Value::Object(ot) = &item.value {
            let next_obj = ObjId(item._id);
            let next_typ = IterType::new(self.encoding, *ot);
            self.next_objs.insert(next_obj, next_typ);
        }
        Some(ObjItem {
            obj: self.op_set?.id_to_exid(self.obj.0),
            item: DocItem::List(item),
        })
    }

    #[inline(never)]
    fn shift(&mut self, next_type: IterType, next_range: Range<usize>) {
        match next_type {
            IterType::Map => self.map_iter.shift_range(next_range),
            IterType::List => self.list_iter.shift_range(next_range),
            IterType::Text(_) => self.span_iter.shift_range(next_range),
        }
    }

    #[inline(never)]
    fn next_object(&mut self) -> Option<()> {
        let (next, next_type) = self.next_objs.pop_first()?;
        let next_range = self.obj_id_iter.seek_to_value(next);
        self.shift(next_type, next_range);
        self.obj = next;
        self.iter_type = next_type;
        Some(())
    }

    #[inline(never)]
    fn next_prop(&mut self) -> Option<ObjItem<'a>> {
        match self.iter_type {
            IterType::Map => {
                let map_item = self.map_iter.next();
                self.process_map_item(map_item)
            }
            IterType::List => {
                let list_item = self.list_iter.next();
                self.process_list_item(list_item)
            }
            IterType::Text(enc) => {
                let span_item = self.span_iter.next()?;
                let op_set = self.op_set?;
                let obj = op_set.id_to_exid(self.obj.0);
                let clock = self.span_iter.clock.as_ref();
                let span = span_item.export(op_set, clock, enc);
                let item = DocItem::Text(span);
                Some(ObjItem { obj, item })
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum IterType {
    Map,
    List,
    Text(TextEncoding),
}

impl IterType {
    fn new(text_rep: TextRepresentation, obj_type: ObjType) -> Self {
        match (obj_type, text_rep) {
            (ObjType::Text, TextRepresentation::String(enc)) => IterType::Text(enc),
            (ObjType::Map, _) => IterType::Map,
            _ => IterType::List,
        }
    }
}

impl<'a> Iterator for DocIter<'a> {
    type Item = ObjItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.next_prop() {
                return Some(item);
            } else {
                self.next_object()?;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObjItem<'a> {
    pub obj: ExId,
    pub item: DocItem<'a>,
}

impl<'a> ObjItem<'a> {
    pub fn key(&self) -> Option<&str> {
        if let DocItem::Map(MapRangeItem { key, .. }) = &self.item {
            Some(key)
        } else {
            None
        }
    }

    #[inline(never)]
    pub fn value(&self) -> Option<Value<'a>> {
        match &self.item {
            DocItem::Map(MapRangeItem { value, .. }) => Some(value.clone().into()),
            DocItem::List(ListRangeItem { value, .. }) => Some(value.clone()),
            DocItem::Text(span) => Some(Value::str(span.as_str())),
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
    pub fn id(&self) -> Option<&ExId> {
        match self {
            DocItem::Map(MapRangeItem { id, .. }) => Some(id),
            DocItem::List(ListRangeItem { id, .. }) => Some(id),
            _ => None,
        }
    }

    pub fn value(&self) -> Option<Value<'a>> {
        match self {
            DocItem::Map(MapRangeItem { value, .. }) => Some(value.clone().into()),
            DocItem::List(ListRangeItem { value, .. }) => Some(value.clone()),
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
        let map2 = tx.put_object(&ROOT, "key05", ObjType::Map).unwrap();
        let map3 = tx.put_object(&ROOT, "key06", ObjType::Map).unwrap(); // deleted
        let map4 = tx.put_object(&ROOT, "key07", ObjType::Map).unwrap();
        let list1 = tx.put_object(&ROOT, "key08", ObjType::List).unwrap();
        let map5 = tx.put_object(&ROOT, "key09", ObjType::Map).unwrap();
        let map6 = tx.put_object(&ROOT, "key10", ObjType::Map).unwrap();
        let list2 = tx.put_object(&ROOT, "key11", ObjType::List).unwrap();
        let map7 = tx.put_object(&ROOT, "key12", ObjType::Map).unwrap();
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
        let keys: Vec<_> = props.iter().map(|p| p.key()).collect();

        let answers = vec![
            ("key01", "value1".into()),
            ("key02", "value2".into()),
            ("key03", Value::Object(ObjType::Map)),
            ("key04", "value4".into()),
            ("key05", Value::Object(ObjType::Map)),
            ("key07", Value::Object(ObjType::Map)),
            ("key08", Value::Object(ObjType::List)),
            ("key09", Value::Object(ObjType::Map)),
            ("key10", Value::Object(ObjType::Map)),
            ("key11", Value::Object(ObjType::List)),
            ("key12", Value::Object(ObjType::Map)),
            ("key13", Value::Object(ObjType::Text)),
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
