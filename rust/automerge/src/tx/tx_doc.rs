use crate::automerge::Automerge;
use crate::exid::ExId;
use crate::hash_retention::HashRetention;

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

    fn get_scope(
        &self,
        heads: Option<&[crate::ChangeId]>,
    ) -> std::result::Result<Option<crate::types::Clock>, crate::AutomergeError> {
        if let Some(h) = heads {
            // with a transaction in flight the current-heads shortcut is unsound
            let nodes = self.doc().nodes_for_change_ids(h)?;
            Ok(Some(self.doc().change_graph.clock_for_nodes(nodes)))
        } else {
            Ok(self.inner().and_then(|i| i.get_scope().clone()))
        }
    }
}

impl<T: TxDoc> crate::read::ReadDoc for T {
    fn keys<O: AsRef<crate::exid::ExId>>(&self, obj: O) -> crate::iter::Keys<'_> {
        self.doc().keys_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn keys_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeId],
    ) -> Result<crate::iter::Keys<'_>, crate::AutomergeError> {
        Ok(self
            .doc()
            .keys_for(obj.as_ref(), self.get_scope(Some(heads))?))
    }

    fn iter_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: Option<&[crate::ChangeId]>,
    ) -> Result<crate::iter::DocIter<'_>, crate::AutomergeError> {
        Ok(self.doc().iter_for(obj.as_ref(), self.get_scope(heads)?))
    }

    fn map_range<'b, O: AsRef<crate::exid::ExId>, R: std::ops::RangeBounds<String> + 'b>(
        &'b self,
        obj: O,
        range: R,
    ) -> crate::iter::MapRange<'b> {
        self.doc().map_range_for(
            obj.as_ref(),
            range,
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn map_range_at<'b, O: AsRef<crate::exid::ExId>, R: std::ops::RangeBounds<String> + 'b>(
        &'b self,
        obj: O,
        range: R,
        heads: &[crate::ChangeId],
    ) -> Result<crate::iter::MapRange<'b>, crate::AutomergeError> {
        Ok(self
            .doc()
            .map_range_for(obj.as_ref(), range, self.get_scope(Some(heads))?))
    }

    fn list_range<O: AsRef<crate::exid::ExId>, R: std::ops::RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> crate::iter::ListRange<'_> {
        self.doc().list_range_for(
            obj.as_ref(),
            range,
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn list_range_at<O: AsRef<crate::exid::ExId>, R: std::ops::RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[crate::ChangeId],
    ) -> Result<crate::iter::ListRange<'_>, crate::AutomergeError> {
        Ok(self
            .doc()
            .list_range_for(obj.as_ref(), range, self.get_scope(Some(heads))?))
    }

    fn values<O: AsRef<crate::exid::ExId>>(&self, obj: O) -> crate::iter::Values<'_> {
        self.doc().values_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn values_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeId],
    ) -> Result<crate::iter::Values<'_>, crate::AutomergeError> {
        Ok(self
            .doc()
            .values_for(obj.as_ref(), self.get_scope(Some(heads))?))
    }

    fn length<O: AsRef<crate::exid::ExId>>(&self, obj: O) -> usize {
        self.doc().length_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn length_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeId],
    ) -> Result<usize, crate::AutomergeError> {
        Ok(self
            .doc()
            .length_for(obj.as_ref(), self.get_scope(Some(heads))?))
    }

    fn object_type<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
    ) -> Result<crate::ObjType, crate::AutomergeError> {
        self.doc().object_type(obj)
    }

    fn text<O: AsRef<crate::exid::ExId>>(&self, obj: O) -> Result<String, crate::AutomergeError> {
        self.doc().text_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn text_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeId],
    ) -> Result<String, crate::AutomergeError> {
        self.doc()
            .text_for(obj.as_ref(), self.get_scope(Some(heads))?)
    }

    fn spans<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
    ) -> Result<crate::iter::Spans<'_>, crate::AutomergeError> {
        self.doc().spans_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn spans_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeId],
    ) -> Result<crate::iter::Spans<'_>, crate::AutomergeError> {
        self.doc()
            .spans_for(obj.as_ref(), self.get_scope(Some(heads))?)
    }

    fn get_cursor<O: AsRef<crate::exid::ExId>, I: Into<crate::cursor::CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[crate::ChangeId]>,
    ) -> Result<crate::Cursor, crate::AutomergeError> {
        self.doc().get_cursor_for(
            obj.as_ref(),
            position.into(),
            self.get_scope(at)?,
            crate::cursor::MoveCursor::After,
        )
    }

    fn get_cursor_moving<O: AsRef<crate::exid::ExId>, I: Into<crate::cursor::CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[crate::ChangeId]>,
        move_cursor: crate::cursor::MoveCursor,
    ) -> Result<crate::Cursor, crate::AutomergeError> {
        self.doc().get_cursor_for(
            obj.as_ref(),
            position.into(),
            self.get_scope(at)?,
            move_cursor,
        )
    }

    fn get_cursor_position<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        address: &crate::Cursor,
        at: Option<&[crate::ChangeId]>,
    ) -> Result<usize, crate::AutomergeError> {
        self.doc()
            .get_cursor_position_for(obj.as_ref(), address, self.get_scope(at)?)
    }

    fn marks<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
    ) -> Result<Vec<crate::marks::Mark>, crate::AutomergeError> {
        self.doc().marks_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn marks_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeId],
    ) -> Result<Vec<crate::marks::Mark>, crate::AutomergeError> {
        self.doc()
            .marks_for(obj.as_ref(), self.get_scope(Some(heads))?)
    }

    fn hydrate<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: Option<&[crate::ChangeId]>,
    ) -> Result<crate::hydrate::Value, crate::AutomergeError> {
        self.doc().hydrate_obj(obj.as_ref(), heads)
    }

    fn get_marks<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        index: usize,
        heads: Option<&[crate::ChangeId]>,
    ) -> Result<crate::marks::MarkSet, crate::AutomergeError> {
        self.doc()
            .get_marks_for(obj.as_ref(), index, self.get_scope(heads)?)
    }

    fn get<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(crate::Value<'_>, crate::exid::ExId)>, crate::AutomergeError> {
        self.doc().get_for(
            obj.as_ref(),
            prop.into(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn get_at<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[crate::ChangeId],
    ) -> Result<Option<(crate::Value<'_>, crate::exid::ExId)>, crate::AutomergeError> {
        self.doc()
            .get_for(obj.as_ref(), prop.into(), self.get_scope(Some(heads))?)
    }

    fn get_all<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(crate::Value<'_>, crate::exid::ExId)>, crate::AutomergeError> {
        self.doc().get_all_for(
            obj.as_ref(),
            prop.into(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn get_all_at<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[crate::ChangeId],
    ) -> Result<Vec<(crate::Value<'_>, crate::exid::ExId)>, crate::AutomergeError> {
        self.doc()
            .get_all_for(obj.as_ref(), prop.into(), self.get_scope(Some(heads))?)
    }

    fn parents<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
    ) -> Result<crate::automerge::Parents<'_>, crate::AutomergeError> {
        self.doc().parents_for(
            obj.as_ref(),
            self.get_scope(None).expect("no heads to resolve"),
        )
    }

    fn parents_at<O: AsRef<crate::exid::ExId>>(
        &self,
        obj: O,
        heads: &[crate::ChangeId],
    ) -> Result<crate::automerge::Parents<'_>, crate::AutomergeError> {
        self.doc()
            .parents_for(obj.as_ref(), self.get_scope(Some(heads))?)
    }

    fn get_missing_deps(
        &self,
        heads: &[crate::ChangeId],
    ) -> Result<Vec<crate::ChangeHash>, crate::AutomergeError> {
        self.doc().get_missing_deps(heads)
    }

    fn get_change_by_hash(
        &self,
        hash: &crate::ChangeHash,
    ) -> Result<Option<crate::Change>, crate::AutomergeError> {
        self.doc().get_change_by_hash(hash)
    }

    fn stats(&self) -> crate::read::Stats {
        self.doc().stats()
    }

    fn text_encoding(&self) -> crate::TextEncoding {
        self.doc().text_encoding()
    }
}

impl<T: TxDoc> crate::tx::Transactable for T {
    fn pending_ops(&self) -> usize {
        self.inner().expect("an open transaction").pending_ops()
    }

    fn put<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>, V: Into<crate::ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<(), crate::AutomergeError> {
        self.do_tx(|tx, doc| tx.put(doc, obj.as_ref(), prop, value))
    }

    fn put_object<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: crate::ObjType,
    ) -> Result<crate::exid::ExId, crate::AutomergeError> {
        self.do_tx(|tx, doc| tx.put_object(doc, obj.as_ref(), prop, value))
    }

    fn insert<O: AsRef<crate::exid::ExId>, V: Into<crate::ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), crate::AutomergeError> {
        self.do_tx(|tx, doc| tx.insert(doc, obj.as_ref(), index, value))
    }

    fn insert_object<O: AsRef<crate::exid::ExId>>(
        &mut self,
        obj: O,
        index: usize,
        value: crate::ObjType,
    ) -> Result<crate::exid::ExId, crate::AutomergeError> {
        self.do_tx(|tx, doc| tx.insert_object(doc, obj.as_ref(), index, value))
    }

    fn increment<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), crate::AutomergeError> {
        self.do_tx(|tx, doc| tx.increment(doc, obj.as_ref(), prop, value))
    }

    fn delete<O: AsRef<crate::exid::ExId>, P: Into<crate::Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), crate::AutomergeError> {
        self.do_tx(|tx, doc| tx.delete(doc, obj.as_ref(), prop))
    }

    fn splice<O: AsRef<ExId>, V: Into<crate::hydrate::Value>, I: IntoIterator<Item = V>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        vals: I,
    ) -> Result<(), crate::AutomergeError> {
        self.do_tx(|tx, doc| tx.splice(doc, obj.as_ref(), pos, del, vals))?;
        Ok(())
    }

    fn splice_text<O: AsRef<crate::exid::ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        text: &str,
    ) -> Result<(), crate::AutomergeError> {
        self.do_tx(|tx, doc| tx.splice_text(doc, obj.as_ref(), pos, del, text))?;
        Ok(())
    }

    fn mark<O: AsRef<crate::exid::ExId>>(
        &mut self,
        obj: O,
        mark: crate::marks::Mark,
        expand: crate::marks::ExpandMark,
    ) -> Result<(), crate::AutomergeError> {
        self.do_tx(|tx, doc| tx.mark(doc, obj.as_ref(), mark, expand))
    }

    fn unmark<O: AsRef<crate::exid::ExId>>(
        &mut self,
        obj: O,
        name: &str,
        start: usize,
        end: usize,
        expand: crate::marks::ExpandMark,
    ) -> Result<(), crate::AutomergeError> {
        self.do_tx(|tx, doc| tx.unmark(doc, obj.as_ref(), name, start, end, expand))
    }

    fn split_block<O>(
        &mut self,
        obj: O,
        index: usize,
    ) -> Result<crate::exid::ExId, crate::AutomergeError>
    where
        O: AsRef<crate::exid::ExId>,
    {
        self.do_tx(|tx, doc| tx.split_block(doc, obj.as_ref(), index))
    }

    fn join_block<O>(&mut self, text: O, index: usize) -> Result<(), crate::AutomergeError>
    where
        O: AsRef<crate::exid::ExId>,
    {
        self.do_tx(|tx, doc| tx.join_block(doc, text.as_ref(), index))
    }

    fn replace_block<O>(
        &mut self,
        text: O,
        index: usize,
    ) -> Result<crate::exid::ExId, crate::AutomergeError>
    where
        O: AsRef<crate::exid::ExId>,
    {
        self.do_tx(|tx, doc| tx.replace_block(doc, text.as_ref(), index))
    }

    fn base_heads(&self) -> Vec<crate::ChangeHash> {
        self.inner().map(|d| d.get_deps()).unwrap_or_default()
    }

    fn update_text<S: AsRef<str>>(
        &mut self,
        obj: &crate::exid::ExId,
        new_text: S,
    ) -> Result<(), crate::AutomergeError> {
        self.do_tx(|tx, doc| crate::text_diff::myers_diff(doc, tx, obj, new_text))
    }

    fn update_spans<O: AsRef<crate::exid::ExId>, I: IntoIterator<Item = crate::iter::Span>>(
        &mut self,
        text: O,
        config: crate::marks::UpdateSpansConfig,
        new_text: I,
    ) -> Result<(), crate::AutomergeError> {
        self.do_tx(move |tx, doc| {
            crate::text_diff::myers_block_diff(doc, tx, text.as_ref(), new_text, &config)
        })
    }

    fn update_object<O: AsRef<crate::exid::ExId>>(
        &mut self,
        obj: O,
        new_value: &crate::hydrate::Value,
    ) -> Result<(), crate::error::UpdateObjectError> {
        self.do_tx(move |tx, doc| tx.update_object(doc, obj.as_ref(), new_value))
    }

    fn batch_create_object<O: AsRef<ExId>, P: Into<crate::Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: &crate::hydrate::Value,
        insert: bool,
    ) -> Result<ExId, crate::AutomergeError> {
        let prop = prop.into();
        self.do_tx(move |tx, doc| tx.batch_create_object(doc, obj.as_ref(), prop, value, insert))
    }

    fn init_root_from_hydrate(
        &mut self,
        value: &crate::hydrate::Map,
    ) -> Result<(), crate::AutomergeError> {
        self.do_tx(move |tx, doc| tx.batch_init_root_map(doc, value))?;
        Ok(())
    }
}
