use crate::exid::ExId;
use crate::iter::Span;
use crate::marks::{ExpandMark, Mark, UpdateSpansConfig};
use crate::{AutomergeError, ChangeHash, ObjType, Prop, ScalarValue};

use crate::automerge::Automerge;
use crate::hash_retention::Full;

pub trait AuditWrapper {
    type Next: crate::read::ReadDoc;

    fn next(&self) -> &Self::Next;

    fn next_mut(&mut self) -> &mut Self::Next;

    fn doc(&self) -> &Automerge<Full>;
}

/// Reads the audited API declares infallible return an empty result where
/// the next API errors.
impl<T: AuditWrapper> crate::audit::ReadDoc for T {
    fn parents<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
    ) -> ::std::result::Result<crate::Parents<'_>, crate::AutomergeError> {
        crate::read::ReadDoc::parents(self.next(), obj)
    }

    fn parents_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeHash],
    ) -> ::std::result::Result<crate::Parents<'_>, crate::AutomergeError> {
        let ids = crate::audit::known_ids(self.doc(), heads);
        crate::read::ReadDoc::parents_at(self.next(), obj, &ids)
    }

    fn keys<O: AsRef<crate::exid::ExId>>(&self, obj: O) -> crate::iter::Keys<'_> {
        crate::read::ReadDoc::keys(self.next(), obj)
    }

    fn keys_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeHash],
    ) -> crate::iter::Keys<'_> {
        let ids = crate::audit::known_ids(self.doc(), heads);
        crate::read::ReadDoc::keys_at(self.next(), obj, &ids).unwrap_or_default()
    }

    fn iter_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: Option<&[crate::ChangeHash]>,
    ) -> crate::iter::DocIter<'_> {
        let ids = heads.map(|h| crate::audit::known_ids(self.doc(), h));
        let encoding = crate::read::ReadDoc::text_encoding(self.next());
        crate::read::ReadDoc::iter_at(self.next(), obj, ids.as_deref())
            .unwrap_or_else(|_| crate::iter::DocIter::empty(encoding))
    }

    fn map_range<'a, O: AsRef<crate::exid::ExId>, R: std::ops::RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
    ) -> crate::iter::MapRange<'a> {
        crate::read::ReadDoc::map_range(self.next(), obj, range)
    }

    fn map_range_at<'a, O: AsRef<crate::exid::ExId>, R: std::ops::RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        heads: &[crate::ChangeHash],
    ) -> crate::iter::MapRange<'a> {
        let ids = crate::audit::known_ids(self.doc(), heads);
        crate::read::ReadDoc::map_range_at(self.next(), obj, range, &ids).unwrap_or_default()
    }

    fn list_range<O: AsRef<crate::exid::ExId>, R: std::ops::RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> crate::iter::ListRange<'_> {
        crate::read::ReadDoc::list_range(self.next(), obj, range)
    }

    fn list_range_at<O: AsRef<crate::exid::ExId>, R: std::ops::RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[crate::ChangeHash],
    ) -> crate::iter::ListRange<'_> {
        let ids = crate::audit::known_ids(self.doc(), heads);
        crate::read::ReadDoc::list_range_at(self.next(), obj, range, &ids).unwrap_or_default()
    }

    fn values<O: AsRef<crate::exid::ExId>>(&self, obj: O) -> crate::iter::Values<'_> {
        crate::read::ReadDoc::values(self.next(), obj)
    }

    fn values_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeHash],
    ) -> crate::iter::Values<'_> {
        let ids = crate::audit::known_ids(self.doc(), heads);
        crate::read::ReadDoc::values_at(self.next(), obj, &ids).unwrap_or_default()
    }

    fn length<O: AsRef<crate::exid::ExId>>(&self, obj: O) -> usize {
        crate::read::ReadDoc::length(self.next(), obj)
    }

    fn length_at<O: AsRef<crate::exid::ExId>>(&self, obj: O, heads: &[crate::ChangeHash]) -> usize {
        let ids = crate::audit::known_ids(self.doc(), heads);
        crate::read::ReadDoc::length_at(self.next(), obj, &ids).unwrap_or(0)
    }

    fn object_type<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
    ) -> ::std::result::Result<crate::ObjType, crate::AutomergeError> {
        crate::read::ReadDoc::object_type(self.next(), obj)
    }

    fn marks<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
    ) -> ::std::result::Result<Vec<crate::marks::Mark>, crate::AutomergeError> {
        crate::read::ReadDoc::marks(self.next(), obj)
    }

    fn marks_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeHash],
    ) -> ::std::result::Result<Vec<crate::marks::Mark>, crate::AutomergeError> {
        let ids = crate::audit::known_ids(self.doc(), heads);
        crate::read::ReadDoc::marks_at(self.next(), obj, &ids)
    }

    fn get_marks<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        index: usize,
        heads: Option<&[crate::ChangeHash]>,
    ) -> ::std::result::Result<crate::marks::MarkSet, crate::AutomergeError> {
        let ids = heads.map(|h| crate::audit::known_ids(self.doc(), h));
        crate::read::ReadDoc::get_marks(self.next(), obj, index, ids.as_deref())
    }

    fn text<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
    ) -> ::std::result::Result<String, crate::AutomergeError> {
        crate::read::ReadDoc::text(self.next(), obj)
    }

    fn text_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeHash],
    ) -> ::std::result::Result<String, crate::AutomergeError> {
        let ids = crate::audit::known_ids(self.doc(), heads);
        crate::read::ReadDoc::text_at(self.next(), obj, &ids)
    }

    fn spans<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
    ) -> ::std::result::Result<crate::iter::Spans<'_>, crate::AutomergeError> {
        crate::read::ReadDoc::spans(self.next(), obj)
    }

    fn spans_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeHash],
    ) -> ::std::result::Result<crate::iter::Spans<'_>, crate::AutomergeError> {
        let ids = crate::audit::known_ids(self.doc(), heads);
        crate::read::ReadDoc::spans_at(self.next(), obj, &ids)
    }

    fn get_cursor<O: AsRef<crate::exid::ExId>, I: Into<crate::CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[crate::ChangeHash]>,
    ) -> ::std::result::Result<crate::Cursor, crate::AutomergeError> {
        let ids = at.map(|h| crate::audit::known_ids(self.doc(), h));
        crate::read::ReadDoc::get_cursor(self.next(), obj, position, ids.as_deref())
    }

    fn get_cursor_moving<O: AsRef<crate::exid::ExId>, I: Into<crate::CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[crate::ChangeHash]>,
        move_cursor: crate::MoveCursor,
    ) -> ::std::result::Result<crate::Cursor, crate::AutomergeError> {
        let ids = at.map(|h| crate::audit::known_ids(self.doc(), h));
        crate::read::ReadDoc::get_cursor_moving(
            self.next(),
            obj,
            position,
            ids.as_deref(),
            move_cursor,
        )
    }

    fn get_cursor_position<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        cursor: &crate::Cursor,
        at: Option<&[crate::ChangeHash]>,
    ) -> ::std::result::Result<usize, crate::AutomergeError> {
        let ids = at.map(|h| crate::audit::known_ids(self.doc(), h));
        crate::read::ReadDoc::get_cursor_position(self.next(), obj, cursor, ids.as_deref())
    }

    fn get<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> ::std::result::Result<Option<(crate::Value<'_>, crate::exid::ExId)>, crate::AutomergeError>
    {
        crate::read::ReadDoc::get(self.next(), obj, prop)
    }

    fn get_at<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[crate::ChangeHash],
    ) -> ::std::result::Result<Option<(crate::Value<'_>, crate::exid::ExId)>, crate::AutomergeError>
    {
        let ids = crate::audit::known_ids(self.doc(), heads);
        crate::read::ReadDoc::get_at(self.next(), obj, prop, &ids)
    }

    fn hydrate<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: Option<&[crate::ChangeHash]>,
    ) -> ::std::result::Result<crate::hydrate::Value, crate::AutomergeError> {
        let ids = heads.map(|h| crate::audit::known_ids(self.doc(), h));
        crate::read::ReadDoc::hydrate(self.next(), obj, ids.as_deref())
    }

    fn get_all<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> ::std::result::Result<Vec<(crate::Value<'_>, crate::exid::ExId)>, crate::AutomergeError>
    {
        crate::read::ReadDoc::get_all(self.next(), obj, prop)
    }

    fn get_all_at<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[crate::ChangeHash],
    ) -> ::std::result::Result<Vec<(crate::Value<'_>, crate::exid::ExId)>, crate::AutomergeError>
    {
        let ids = crate::audit::known_ids(self.doc(), heads);
        crate::read::ReadDoc::get_all_at(self.next(), obj, prop, &ids)
    }

    fn get_missing_deps(&self, heads: &[crate::ChangeHash]) -> Vec<crate::ChangeHash> {
        self.doc()
            .missing_deps_with_queued(heads)
            .unwrap_or_default()
    }

    fn get_change_by_hash(&self, hash: &crate::ChangeHash) -> Option<crate::Change> {
        crate::read::ReadDoc::get_change_by_hash(self.next(), hash)
            .ok()
            .flatten()
    }

    fn stats(&self) -> crate::read::Stats {
        crate::read::ReadDoc::stats(self.next())
    }

    fn text_encoding(&self) -> crate::TextEncoding {
        crate::read::ReadDoc::text_encoding(self.next())
    }
}

impl<T: AuditWrapper> crate::audit::Transactable for T
where
    T::Next: crate::tx::Transactable,
{
    fn pending_ops(&self) -> usize {
        crate::tx::Transactable::pending_ops(self.next())
    }

    fn put<O: AsRef<ExId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::put(self.next_mut(), obj, prop, value)
    }

    fn put_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        object: ObjType,
    ) -> ::std::result::Result<ExId, AutomergeError> {
        crate::tx::Transactable::put_object(self.next_mut(), obj, prop, object)
    }

    fn insert<O: AsRef<ExId>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::insert(self.next_mut(), obj, index, value)
    }

    fn insert_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        index: usize,
        object: ObjType,
    ) -> ::std::result::Result<ExId, AutomergeError> {
        crate::tx::Transactable::insert_object(self.next_mut(), obj, index, object)
    }

    fn increment<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::increment(self.next_mut(), obj, prop, value)
    }

    fn delete<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::delete(self.next_mut(), obj, prop)
    }

    fn splice<O: AsRef<ExId>, V: Into<crate::hydrate::Value>, I: IntoIterator<Item = V>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        vals: I,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::splice(self.next_mut(), obj, pos, del, vals)
    }

    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        text: &str,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::splice_text(self.next_mut(), obj, pos, del, text)
    }

    fn mark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        mark: Mark,
        expand: ExpandMark,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::mark(self.next_mut(), obj, mark, expand)
    }

    fn unmark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        key: &str,
        start: usize,
        end: usize,
        expand: ExpandMark,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::unmark(self.next_mut(), obj, key, start, end, expand)
    }

    fn split_block<O>(
        &mut self,
        obj: O,
        index: usize,
    ) -> ::std::result::Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>,
    {
        crate::tx::Transactable::split_block(self.next_mut(), obj, index)
    }

    fn join_block<O: AsRef<ExId>>(
        &mut self,
        text: O,
        index: usize,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::join_block(self.next_mut(), text, index)
    }

    fn replace_block<O>(
        &mut self,
        text: O,
        index: usize,
    ) -> ::std::result::Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>,
    {
        crate::tx::Transactable::replace_block(self.next_mut(), text, index)
    }

    fn update_spans<O: AsRef<ExId>, I: IntoIterator<Item = Span>>(
        &mut self,
        text: O,
        config: UpdateSpansConfig,
        new_text: I,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::update_spans(self.next_mut(), text, config, new_text)
    }

    fn base_heads(&self) -> Vec<ChangeHash> {
        crate::tx::Transactable::base_heads(self.next())
    }

    fn update_text<S: AsRef<str>>(
        &mut self,
        obj: &ExId,
        new_text: S,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::update_text(self.next_mut(), obj, new_text)
    }

    fn update_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        new_value: &crate::hydrate::Value,
    ) -> ::std::result::Result<(), crate::error::UpdateObjectError> {
        crate::tx::Transactable::update_object(self.next_mut(), obj, new_value)
    }

    fn batch_create_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: &crate::hydrate::Value,
        insert: bool,
    ) -> ::std::result::Result<ExId, AutomergeError> {
        crate::tx::Transactable::batch_create_object(self.next_mut(), obj, prop, value, insert)
    }

    fn init_root_from_hydrate(
        &mut self,
        value: &crate::hydrate::Map,
    ) -> ::std::result::Result<(), AutomergeError> {
        crate::tx::Transactable::init_root_from_hydrate(self.next_mut(), value)
    }
}
