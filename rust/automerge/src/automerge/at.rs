use crate::exid::ExId;
use crate::marks::{Mark, MarkStateMachine};
use crate::query;
use crate::types::{ListEncoding, OpType};
use crate::value::ScalarValue;
use crate::{
    Automerge, AutomergeError, Change, ChangeHash, Keys, ListRange, MapRange, ObjType, Parents,
    Prop, ReadDoc, Value, Values,
};
use itertools::Itertools;
use std::ops::RangeBounds;

#[derive(Debug)]
pub struct At<'a, 'b> {
    pub(crate) doc: &'a Automerge,
    pub(crate) heads: &'b [ChangeHash],
}

impl<'a,'b> ReadDoc for At<'a,'b> {
    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'a> {
        self.doc.keys_at(obj, self.heads)
    }

    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'a>, AutomergeError> {
        // FIXME - need a parents_at()
        self.doc.parents(obj)
    }

    fn path_to_object<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<(ExId, Prop)>, AutomergeError> {
        // FIXME - need a path_to_object_at()
        self.doc.path_to_object(obj)
    }

    fn map_range<O: AsRef<ExId>, R: RangeBounds<String>>(
        &self,
        obj: O,
        range: R,
    ) -> MapRange<'a, R> {
        self.doc.map_range_at(obj, range, self.heads)
    }

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> ListRange<'a, R> {
        self.doc.list_range_at(obj, range, self.heads)
    }

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        self.doc.values_at(obj, self.heads)
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.doc.length_at(obj, self.heads)
    }

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        self.doc.object_type(obj)
    }

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark<'a>>, AutomergeError> {
        self.doc.marks_at(obj, self.heads)
    }

    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError> {
        self.doc.text_at(obj, self.heads)
    }

    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'a>, ExId)>, AutomergeError> {
        self.doc.get_at(obj, prop, self.heads)
    }

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'a>, ExId)>, AutomergeError> {
        self.doc.get_all_at(obj, prop, self.heads)
    }

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        todo!()
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<&Change> {
        todo!()
    }
}

impl Automerge {
    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_> {
        if let Ok((obj, _)) = self.exid_to_obj(obj.as_ref()) {
            let clock = self.clock_at(heads);
            if let Some(keys_at) = self.ops.keys_at(obj, clock) {
                return Keys::new(self).with_keys_at(keys_at);
            }
        }
        Keys::new(self)
    }

    fn map_range_at<O: AsRef<ExId>, R: RangeBounds<String>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'_, R> {
        if let Ok((obj, _)) = self.exid_to_obj(obj.as_ref()) {
            let clock = self.clock_at(heads);
            if let Some(iter_range) = self.ops.map_range_at(obj, range, clock) {
                return MapRange::new(self).with_map_range_at(iter_range);
            }
        }
        MapRange::new(self)
    }

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_, R> {
        if let Ok((obj, _)) = self.exid_to_obj(obj.as_ref()) {
            let clock = self.clock_at(heads);
            if let Some(iter_range) = self.ops.list_range_at(obj, range, clock) {
                return ListRange::new(self).with_list_range_at(iter_range);
            }
        }
        ListRange::new(self)
    }

    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_> {
        if let Ok((obj, obj_type)) = self.exid_to_obj(obj.as_ref()) {
            let clock = self.clock_at(heads);
            match obj_type {
                ObjType::Map | ObjType::Table => {
                    let iter_range = self.ops.map_range_at(obj, .., clock);
                    Values::new(self, iter_range)
                }
                ObjType::List | ObjType::Text => {
                    let iter_range = self.ops.list_range_at(obj, .., clock);
                    Values::new(self, iter_range)
                }
            }
        } else {
            Values::empty(self)
        }
    }

    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        if let Ok((inner_obj, obj_type)) = self.exid_to_obj(obj.as_ref()) {
            let clock = self.clock_at(heads);
            if obj_type == ObjType::Map || obj_type == ObjType::Table {
                self.keys_at(obj, heads).count()
            } else {
                let encoding = ListEncoding::new(obj_type, self.text_encoding);
                self.ops
                    .search(&inner_obj, query::LenAt::new(clock, encoding))
                    .len
            }
        } else {
            0
        }
    }

    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?.0;
        let clock = self.clock_at(heads);
        let query = self.ops.search(&obj, query::ListValsAt::new(clock));
        let mut buffer = String::new();
        for q in &query.ops {
            if let OpType::Put(ScalarValue::Str(s)) = &q.action {
                buffer.push_str(s);
            } else {
                buffer.push('\u{fffc}');
            }
        }
        Ok(buffer)
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark<'_>>, AutomergeError> {
        let (obj, obj_type) = self.exid_to_obj(obj.as_ref())?;
        let clock = self.clock_at(heads);
        let encoding = ListEncoding::new(obj_type, self.text_encoding);
        let ops_by_key = self.ops().iter_ops(&obj).group_by(|o| o.elemid_or_key());
        let mut window = query::VisWindow::default();
        let mut pos = 0;
        let mut marks = MarkStateMachine::default();

        Ok(ops_by_key
            .into_iter()
            .filter_map(|(_key, key_ops)| {
                key_ops
                    .filter(|o| window.visible_at(o, pos, &clock))
                    .last()
                    .and_then(|o| match &o.action {
                        OpType::Make(_) | OpType::Put(_) => {
                            pos += o.width(encoding);
                            None
                        }
                        OpType::MarkBegin(_, data) => marks.mark_begin(o.id, pos, data, self),
                        OpType::MarkEnd(_) => marks.mark_end(o.id, pos, self),
                        OpType::Increment(_) | OpType::Delete => None,
                    })
            })
            .collect())
    }

    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        Ok(self.get_all_at(obj, prop, heads)?.last().cloned())
    }

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let prop = prop.into();
        let obj = self.exid_to_obj(obj.as_ref())?.0;
        let clock = self.clock_at(heads);
        let result = match prop {
            Prop::Map(p) => {
                let prop = self.ops.m.props.lookup(&p);
                if let Some(p) = prop {
                    self.ops
                        .search(&obj, query::PropAt::new(p, clock))
                        .ops
                        .into_iter()
                        .map(|o| (o.clone_value(), self.id_to_exid(o.id)))
                        .collect()
                } else {
                    vec![]
                }
            }
            Prop::Seq(n) => {
                let obj_type = self.ops.object_type(&obj);
                let encoding = obj_type
                    .map(|o| ListEncoding::new(o, self.text_encoding))
                    .unwrap_or_default();
                self.ops
                    .search(&obj, query::NthAt::new(n, clock, encoding))
                    .ops
                    .into_iter()
                    .map(|o| (o.clone_value(), self.id_to_exid(o.id)))
                    .collect()
            }
        };
        Ok(result)
    }
}
