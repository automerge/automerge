use std::borrow::Cow;

use crate::automerge::view::ReadAt;
use crate::automerge::{Automerge, Parents};
use crate::cursor::{CursorPosition, MoveCursor};
use crate::error::UpdateObjectError;
use crate::exid::ExId;
use crate::hash_retention::HashRetention;
use crate::hydrate;
use crate::iter::{DocIter, Keys, ListRange, MapRange, Span, Spans, Values};
use crate::marks::{ExpandMark, Mark, MarkSet, UpdateSpansConfig};
use crate::read::{ReadDoc, Stats};
use crate::text_diff::{myers_block_diff, myers_diff};
use crate::tx::Transactable;
use crate::{
    AutomergeError, Change, ChangeHash, ChangeId, Cursor, ObjType, Prop, ScalarValue, TextEncoding,
    Value,
};

use super::TransactionInner;

pub trait TxDoc {
    type Retention: HashRetention;

    fn doc(&self) -> &Automerge<Self::Retention>;

    fn inner(&self) -> Option<&TransactionInner>;

    fn parts_mut(&mut self) -> (&mut TransactionInner, &mut Automerge<Self::Retention>);

    fn do_tx<F, O>(&mut self, f: F) -> O
    where
        F: FnOnce(&mut TransactionInner, &mut Automerge<Self::Retention>) -> O,
    {
        let (tx, doc) = self.parts_mut();
        f(tx, doc)
    }

    fn get_scope(&self, heads: Option<&[ChangeId]>) -> Result<ReadAt<'_>, AutomergeError> {
        let doc = self.doc();
        if let Some(h) = heads {
            // with a transaction in flight the current-heads shortcut is unsound
            return Ok(doc.read_scoped(Cow::Owned(doc.visible(h)?)));
        }
        Ok(match self.inner().and_then(|i| i.get_scope().as_ref()) {
            Some(scope) => doc.read_scoped(Cow::Borrowed(scope)),
            None => doc.read_current(),
        })
    }
}

pub trait ScopedRead {
    type Retention: HashRetention;

    fn doc(&self) -> &Automerge<Self::Retention>;

    fn get_scope(&self, heads: Option<&[ChangeId]>) -> Result<ReadAt<'_>, AutomergeError>;
}

impl<T: TxDoc> ScopedRead for T {
    type Retention = T::Retention;

    fn doc(&self) -> &Automerge<T::Retention> {
        TxDoc::doc(self)
    }

    fn get_scope(&self, heads: Option<&[ChangeId]>) -> Result<ReadAt<'_>, AutomergeError> {
        TxDoc::get_scope(self, heads)
    }
}

impl<T: ScopedRead> ReadDoc for T {
    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.doc().keys_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn keys_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<Keys<'_>, AutomergeError> {
        Ok(self
            .doc()
            .keys_for(obj.as_ref(), self.get_scope(Some(heads))?))
    }

    fn iter_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeId]>,
    ) -> Result<DocIter<'_>, AutomergeError> {
        Ok(self.doc().iter_for(obj.as_ref(), self.get_scope(heads)?))
    }

    fn map_range<'b, O: AsRef<ExId>, R: std::ops::RangeBounds<String> + 'b>(
        &'b self,
        obj: O,
        range: R,
    ) -> MapRange<'b> {
        self.doc().map_range_for(
            obj.as_ref(),
            range,
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn map_range_at<'b, O: AsRef<ExId>, R: std::ops::RangeBounds<String> + 'b>(
        &'b self,
        obj: O,
        range: R,
        heads: &[ChangeId],
    ) -> Result<MapRange<'b>, AutomergeError> {
        Ok(self
            .doc()
            .map_range_for(obj.as_ref(), range, self.get_scope(Some(heads))?))
    }

    fn list_range<O: AsRef<ExId>, R: std::ops::RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> ListRange<'_> {
        self.doc().list_range_for(
            obj.as_ref(),
            range,
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn list_range_at<O: AsRef<ExId>, R: std::ops::RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeId],
    ) -> Result<ListRange<'_>, AutomergeError> {
        Ok(self
            .doc()
            .list_range_for(obj.as_ref(), range, self.get_scope(Some(heads))?))
    }

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        self.doc().values_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn values_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<Values<'_>, AutomergeError> {
        Ok(self
            .doc()
            .values_for(obj.as_ref(), self.get_scope(Some(heads))?))
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.doc().length_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn length_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<usize, AutomergeError> {
        Ok(self
            .doc()
            .length_for(obj.as_ref(), self.get_scope(Some(heads))?))
    }

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        self.doc().object_type(obj)
    }

    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError> {
        self.doc().text_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<String, AutomergeError> {
        self.doc()
            .text_for(obj.as_ref(), self.get_scope(Some(heads))?)
    }

    fn spans<O: AsRef<ExId>>(&self, obj: O) -> Result<Spans<'_>, AutomergeError> {
        self.doc().spans_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn spans_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<Spans<'_>, AutomergeError> {
        self.doc()
            .spans_for(obj.as_ref(), self.get_scope(Some(heads))?)
    }

    fn get_cursor<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeId]>,
    ) -> Result<Cursor, AutomergeError> {
        self.doc().get_cursor_for(
            obj.as_ref(),
            position.into(),
            self.get_scope(at)?,
            MoveCursor::After,
        )
    }

    fn get_cursor_moving<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeId]>,
        move_cursor: MoveCursor,
    ) -> Result<Cursor, AutomergeError> {
        self.doc().get_cursor_for(
            obj.as_ref(),
            position.into(),
            self.get_scope(at)?,
            move_cursor,
        )
    }

    fn get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        address: &Cursor,
        at: Option<&[ChangeId]>,
    ) -> Result<usize, AutomergeError> {
        self.doc()
            .get_cursor_position_for(obj.as_ref(), address, self.get_scope(at)?)
    }

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark>, AutomergeError> {
        self.doc().marks_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<Vec<Mark>, AutomergeError> {
        self.doc()
            .marks_for(obj.as_ref(), self.get_scope(Some(heads))?)
    }

    fn hydrate<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeId]>,
    ) -> Result<hydrate::Value, AutomergeError> {
        self.doc()
            .hydrate_obj_for(obj.as_ref(), self.get_scope(heads)?)
    }

    fn get_marks<O: AsRef<ExId>>(
        &self,
        obj: O,
        index: usize,
        heads: Option<&[ChangeId]>,
    ) -> Result<MarkSet, AutomergeError> {
        self.doc()
            .get_marks_for(obj.as_ref(), index, self.get_scope(heads)?)
    }

    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.doc().get_for(
            obj.as_ref(),
            prop.into(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeId],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.doc()
            .get_for(obj.as_ref(), prop.into(), self.get_scope(Some(heads))?)
    }

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.doc().get_all_for(
            obj.as_ref(),
            prop.into(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeId],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.doc()
            .get_all_for(obj.as_ref(), prop.into(), self.get_scope(Some(heads))?)
    }

    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'_>, AutomergeError> {
        self.doc().parents_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<Parents<'_>, AutomergeError> {
        self.doc()
            .parents_for(obj.as_ref(), self.get_scope(Some(heads))?)
    }

    fn get_missing_deps(&self, heads: &[ChangeId]) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.doc().get_missing_deps(heads)
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Result<Option<Change>, AutomergeError> {
        self.doc().get_change_by_hash(hash)
    }

    fn stats(&self) -> Stats {
        self.doc().stats()
    }

    fn text_encoding(&self) -> TextEncoding {
        self.doc().text_encoding()
    }
}

impl<T: TxDoc> Transactable for T {
    fn pending_ops(&self) -> usize {
        self.inner().expect("an open transaction").pending_ops()
    }

    fn put<O: AsRef<ExId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc| tx.put(doc, obj.as_ref(), prop, value))
    }

    fn put_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.do_tx(|tx, doc| tx.put_object(doc, obj.as_ref(), prop, value))
    }

    fn insert<O: AsRef<ExId>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc| tx.insert(doc, obj.as_ref(), index, value))
    }

    fn insert_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        index: usize,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.do_tx(|tx, doc| tx.insert_object(doc, obj.as_ref(), index, value))
    }

    fn increment<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc| tx.increment(doc, obj.as_ref(), prop, value))
    }

    fn delete<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc| tx.delete(doc, obj.as_ref(), prop))
    }

    fn splice<O: AsRef<ExId>, V: Into<hydrate::Value>, I: IntoIterator<Item = V>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        vals: I,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc| tx.splice(doc, obj.as_ref(), pos, del, vals))?;
        Ok(())
    }

    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        text: &str,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc| tx.splice_text(doc, obj.as_ref(), pos, del, text))?;
        Ok(())
    }

    fn mark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        mark: Mark,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc| tx.mark(doc, obj.as_ref(), mark, expand))
    }

    fn unmark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        name: &str,
        start: usize,
        end: usize,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc| tx.unmark(doc, obj.as_ref(), name, start, end, expand))
    }

    fn split_block<O>(&mut self, obj: O, index: usize) -> Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>,
    {
        self.do_tx(|tx, doc| tx.split_block(doc, obj.as_ref(), index))
    }

    fn join_block<O>(&mut self, text: O, index: usize) -> Result<(), AutomergeError>
    where
        O: AsRef<ExId>,
    {
        self.do_tx(|tx, doc| tx.join_block(doc, text.as_ref(), index))
    }

    fn replace_block<O>(&mut self, text: O, index: usize) -> Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>,
    {
        self.do_tx(|tx, doc| tx.replace_block(doc, text.as_ref(), index))
    }

    fn base_heads(&self) -> Vec<ChangeHash> {
        self.inner().map(|d| d.get_deps()).unwrap_or_default()
    }

    fn update_text<S: AsRef<str>>(
        &mut self,
        obj: &ExId,
        new_text: S,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc| myers_diff(doc, tx, obj, new_text))
    }

    fn update_spans<O: AsRef<ExId>, I: IntoIterator<Item = Span>>(
        &mut self,
        text: O,
        config: UpdateSpansConfig,
        new_text: I,
    ) -> Result<(), AutomergeError> {
        self.do_tx(move |tx, doc| myers_block_diff(doc, tx, text.as_ref(), new_text, &config))
    }

    fn update_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        new_value: &hydrate::Value,
    ) -> Result<(), UpdateObjectError> {
        self.do_tx(move |tx, doc| tx.update_object(doc, obj.as_ref(), new_value))
    }

    fn batch_create_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: &hydrate::Value,
        insert: bool,
    ) -> Result<ExId, AutomergeError> {
        let prop = prop.into();
        self.do_tx(move |tx, doc| tx.batch_create_object(doc, obj.as_ref(), prop, value, insert))
    }

    fn init_root_from_hydrate(&mut self, value: &hydrate::Map) -> Result<(), AutomergeError> {
        self.do_tx(move |tx, doc| tx.batch_init_root_map(doc, value))?;
        Ok(())
    }
}
