use super::known_ids;
use crate::automerge::Automerge;
use crate::error::UpdateObjectError;
use crate::exid::ExId;
use crate::hash_retention::Full;
use crate::hydrate;
use crate::iter::{DocIter, Keys, ListRange, MapRange, Span, Spans, Values};
use crate::marks::{ExpandMark, Mark, MarkSet, UpdateSpansConfig};
use crate::read::{ReadDoc, Stats};
use crate::tx::Transactable;
use crate::{
    AutomergeError, Change, ChangeHash, Cursor, CursorPosition, MoveCursor, ObjType, Parents, Prop,
    ScalarValue, TextEncoding, Value,
};

pub trait AuditWrapper {
    type Next: ReadDoc;

    fn next(&self) -> &Self::Next;

    fn next_mut(&mut self) -> &mut Self::Next;

    fn doc(&self) -> &Automerge<Full>;
}

/// Reads the audited API declares infallible return an empty result where
/// the next API errors.
impl<T: AuditWrapper> super::ReadDoc for T {
    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'_>, AutomergeError> {
        self.next().parents(obj)
    }

    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Parents<'_>, AutomergeError> {
        let ids = known_ids(self.doc(), heads);
        self.next().parents_at(obj, &ids)
    }

    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.next().keys(obj)
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_> {
        let ids = known_ids(self.doc(), heads);
        self.next().keys_at(obj, &ids).unwrap_or_default()
    }

    fn iter_at<O: AsRef<ExId>>(&self, obj: O, heads: Option<&[ChangeHash]>) -> DocIter<'_> {
        let ids = heads.map(|h| known_ids(self.doc(), h));
        let encoding = self.next().text_encoding();
        self.next()
            .iter_at(obj, ids.as_deref())
            .unwrap_or_else(|_| DocIter::empty(encoding))
    }

    fn map_range<'a, O: AsRef<ExId>, R: std::ops::RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
    ) -> MapRange<'a> {
        self.next().map_range(obj, range)
    }

    fn map_range_at<'a, O: AsRef<ExId>, R: std::ops::RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'a> {
        let ids = known_ids(self.doc(), heads);
        self.next()
            .map_range_at(obj, range, &ids)
            .unwrap_or_default()
    }

    fn list_range<O: AsRef<ExId>, R: std::ops::RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> ListRange<'_> {
        self.next().list_range(obj, range)
    }

    fn list_range_at<O: AsRef<ExId>, R: std::ops::RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_> {
        let ids = known_ids(self.doc(), heads);
        self.next()
            .list_range_at(obj, range, &ids)
            .unwrap_or_default()
    }

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        self.next().values(obj)
    }

    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_> {
        let ids = known_ids(self.doc(), heads);
        self.next().values_at(obj, &ids).unwrap_or_default()
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.next().length(obj)
    }

    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        let ids = known_ids(self.doc(), heads);
        self.next().length_at(obj, &ids).unwrap_or(0)
    }

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        self.next().object_type(obj)
    }

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark>, AutomergeError> {
        self.next().marks(obj)
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark>, AutomergeError> {
        let ids = known_ids(self.doc(), heads);
        self.next().marks_at(obj, &ids)
    }

    fn get_marks<O: AsRef<ExId>>(
        &self,
        obj: O,
        index: usize,
        heads: Option<&[ChangeHash]>,
    ) -> Result<MarkSet, AutomergeError> {
        let ids = heads.map(|h| known_ids(self.doc(), h));
        self.next().get_marks(obj, index, ids.as_deref())
    }

    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError> {
        self.next().text(obj)
    }

    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError> {
        let ids = known_ids(self.doc(), heads);
        self.next().text_at(obj, &ids)
    }

    fn spans<O: AsRef<ExId>>(&self, obj: O) -> Result<Spans<'_>, AutomergeError> {
        self.next().spans(obj)
    }

    fn spans_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Spans<'_>, AutomergeError> {
        let ids = known_ids(self.doc(), heads);
        self.next().spans_at(obj, &ids)
    }

    fn get_cursor<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
    ) -> Result<Cursor, AutomergeError> {
        let ids = at.map(|h| known_ids(self.doc(), h));
        self.next().get_cursor(obj, position, ids.as_deref())
    }

    fn get_cursor_moving<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
        move_cursor: MoveCursor,
    ) -> Result<Cursor, AutomergeError> {
        let ids = at.map(|h| known_ids(self.doc(), h));
        self.next()
            .get_cursor_moving(obj, position, ids.as_deref(), move_cursor)
    }

    fn get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        cursor: &Cursor,
        at: Option<&[ChangeHash]>,
    ) -> Result<usize, AutomergeError> {
        let ids = at.map(|h| known_ids(self.doc(), h));
        self.next().get_cursor_position(obj, cursor, ids.as_deref())
    }

    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.next().get(obj, prop)
    }

    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        let ids = known_ids(self.doc(), heads);
        self.next().get_at(obj, prop, &ids)
    }

    fn hydrate<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeHash]>,
    ) -> Result<hydrate::Value, AutomergeError> {
        let ids = heads.map(|h| known_ids(self.doc(), h));
        self.next().hydrate(obj, ids.as_deref())
    }

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.next().get_all(obj, prop)
    }

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let ids = known_ids(self.doc(), heads);
        self.next().get_all_at(obj, prop, &ids)
    }

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.doc()
            .missing_deps_with_queued(heads)
            .unwrap_or_default()
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<Change> {
        self.next().get_change_by_hash(hash).ok().flatten()
    }

    fn stats(&self) -> Stats {
        self.next().stats()
    }

    fn text_encoding(&self) -> TextEncoding {
        self.next().text_encoding()
    }
}

impl<T: AuditWrapper> super::Transactable for T
where
    T::Next: Transactable,
{
    fn pending_ops(&self) -> usize {
        self.next().pending_ops()
    }

    fn put<O: AsRef<ExId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.next_mut().put(obj, prop, value)
    }

    fn put_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        object: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.next_mut().put_object(obj, prop, object)
    }

    fn insert<O: AsRef<ExId>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.next_mut().insert(obj, index, value)
    }

    fn insert_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        index: usize,
        object: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.next_mut().insert_object(obj, index, object)
    }

    fn increment<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        self.next_mut().increment(obj, prop, value)
    }

    fn delete<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), AutomergeError> {
        self.next_mut().delete(obj, prop)
    }

    fn splice<O: AsRef<ExId>, V: Into<hydrate::Value>, I: IntoIterator<Item = V>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        vals: I,
    ) -> Result<(), AutomergeError> {
        self.next_mut().splice(obj, pos, del, vals)
    }

    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        text: &str,
    ) -> Result<(), AutomergeError> {
        self.next_mut().splice_text(obj, pos, del, text)
    }

    fn mark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        mark: Mark,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        self.next_mut().mark(obj, mark, expand)
    }

    fn unmark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        key: &str,
        start: usize,
        end: usize,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        self.next_mut().unmark(obj, key, start, end, expand)
    }

    fn split_block<O>(&mut self, obj: O, index: usize) -> Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>,
    {
        self.next_mut().split_block(obj, index)
    }

    fn join_block<O: AsRef<ExId>>(&mut self, text: O, index: usize) -> Result<(), AutomergeError> {
        self.next_mut().join_block(text, index)
    }

    fn replace_block<O>(&mut self, text: O, index: usize) -> Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>,
    {
        self.next_mut().replace_block(text, index)
    }

    fn update_spans<O: AsRef<ExId>, I: IntoIterator<Item = Span>>(
        &mut self,
        text: O,
        config: UpdateSpansConfig,
        new_text: I,
    ) -> Result<(), AutomergeError> {
        self.next_mut().update_spans(text, config, new_text)
    }

    fn base_heads(&self) -> Vec<ChangeHash> {
        self.next().base_heads()
    }

    fn update_text<S: AsRef<str>>(
        &mut self,
        obj: &ExId,
        new_text: S,
    ) -> Result<(), AutomergeError> {
        self.next_mut().update_text(obj, new_text)
    }

    fn update_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        new_value: &hydrate::Value,
    ) -> Result<(), UpdateObjectError> {
        self.next_mut().update_object(obj, new_value)
    }

    fn batch_create_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: &hydrate::Value,
        insert: bool,
    ) -> Result<ExId, AutomergeError> {
        self.next_mut()
            .batch_create_object(obj, prop, value, insert)
    }

    fn init_root_from_hydrate(&mut self, value: &hydrate::Map) -> Result<(), AutomergeError> {
        self.next_mut().init_root_from_hydrate(value)
    }
}
