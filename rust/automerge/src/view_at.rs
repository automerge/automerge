use std::ops::RangeBounds;

use crate::clock::Clock;
use crate::cursor::{CursorPosition, MoveCursor};
use crate::error::{AutomergeError, ViewAtError};
use crate::exid::ExId;
use crate::iter::{DocIter, Keys, ListRange, MapRange, Spans, Values};
use crate::marks::{Mark, MarkSet};
use crate::op_set2::Parents;
use crate::Automerge;
use crate::Change;
use crate::ChangeHash;
use crate::Cursor;
use crate::ObjType;
use crate::Prop;
use crate::ReadDoc;
use crate::TextEncoding;
use crate::Value;
use crate::{hydrate, ROOT};

/// A view of an [`Automerge`] document at a specific point in history.
///
/// This type implements [`ReadDoc`], allowing you to use all the normal read
/// methods.
///
/// Create a view using [`ReadDoc::view_at`]:
///
/// ```
/// use automerge::{AutoCommit, ReadDoc, ROOT};
/// use automerge::transaction::Transactable;
///
/// let mut doc = AutoCommit::new();
/// doc.put(&ROOT, "key", "value1").unwrap();
/// let heads1 = doc.get_heads();
///
/// doc.put(&ROOT, "key", "value2").unwrap();
///
/// // View the document at the earlier point in history
/// let view = doc.view_at(&heads1).unwrap();
/// let (value, _) = view.get(&ROOT, "key").unwrap().unwrap();
/// // value is "value1", not "value2"
/// ```
#[derive(Debug, Clone)]
pub struct AutomergeAt<'a> {
    pub(crate) doc: &'a Automerge,
    pub(crate) clock: Clock,
    pub(crate) heads: Vec<ChangeHash>,
}

impl<'a> AutomergeAt<'a> {
    /// Create a new view of the document at the given heads.
    ///
    /// Returns an error if any of the heads do not exist in the document.
    pub(crate) fn new(doc: &'a Automerge, heads: &[ChangeHash]) -> Result<Self, ViewAtError> {
        // Validate all heads exist
        for hash in heads {
            if !doc.has_change(hash) {
                return Err(ViewAtError { missing: *hash });
            }
        }

        let clock = doc.clock_at(heads);
        Ok(Self {
            doc,
            clock,
            heads: heads.to_vec(),
        })
    }

    /// Get a reference to the underlying document.
    pub fn doc(&self) -> &Automerge {
        self.doc
    }

    /// Get the heads this view is at.
    pub fn heads(&self) -> &[ChangeHash] {
        &self.heads
    }
}

impl ReadDoc for AutomergeAt<'_> {
    type ViewAt<'a>
        = AutomergeAt<'a>
    where
        Self: 'a;

    fn view_at(&self, heads: &[ChangeHash]) -> Result<Self::ViewAt<'_>, ViewAtError> {
        AutomergeAt::new(self.doc, heads)
    }

    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'_>, AutomergeError> {
        self.doc.parents_for(obj.as_ref(), Some(self.clock.clone()))
    }

    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Parents<'_>, AutomergeError> {
        let clock = self.doc.clock_at(heads);
        self.doc.parents_for(obj.as_ref(), Some(clock))
    }

    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.doc.keys_for(obj.as_ref(), Some(self.clock.clone()))
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_> {
        let clock = self.doc.clock_at(heads);
        self.doc.keys_for(obj.as_ref(), Some(clock))
    }

    fn iter_at<O: AsRef<ExId>>(&self, obj: O, heads: Option<&[ChangeHash]>) -> DocIter<'_> {
        let clock = heads
            .map(|h| self.doc.clock_at(h))
            .unwrap_or_else(|| self.clock.clone());
        self.doc.iter_for(obj.as_ref(), Some(clock))
    }

    fn iter(&self) -> DocIter<'_> {
        self.doc.iter_for(&ROOT, Some(self.clock.clone()))
    }

    fn map_range<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
    ) -> MapRange<'a> {
        self.doc
            .map_range_for(obj.as_ref(), range, Some(self.clock.clone()))
    }

    fn map_range_at<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'a> {
        let clock = self.doc.clock_at(heads);
        self.doc.map_range_for(obj.as_ref(), range, Some(clock))
    }

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(&self, obj: O, range: R) -> ListRange<'_> {
        self.doc
            .list_range_for(obj.as_ref(), range, Some(self.clock.clone()))
    }

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_> {
        let clock = self.doc.clock_at(heads);
        self.doc.list_range_for(obj.as_ref(), range, Some(clock))
    }

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        self.doc.values_for(obj.as_ref(), Some(self.clock.clone()))
    }

    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_> {
        let clock = self.doc.clock_at(heads);
        self.doc.values_for(obj.as_ref(), Some(clock))
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.doc.length_for(obj.as_ref(), Some(self.clock.clone()))
    }

    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        let clock = self.doc.clock_at(heads);
        self.doc.length_for(obj.as_ref(), Some(clock))
    }

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        self.doc.object_type(obj)
    }

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark>, AutomergeError> {
        self.doc.marks_for(obj.as_ref(), Some(self.clock.clone()))
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark>, AutomergeError> {
        let clock = self.doc.clock_at(heads);
        self.doc.marks_for(obj.as_ref(), Some(clock))
    }

    fn get_marks<O: AsRef<ExId>>(
        &self,
        obj: O,
        index: usize,
        heads: Option<&[ChangeHash]>,
    ) -> Result<MarkSet, AutomergeError> {
        let clock = heads
            .map(|h| self.doc.clock_at(h))
            .unwrap_or_else(|| self.clock.clone());
        self.doc.get_marks_for(obj.as_ref(), index, Some(clock))
    }

    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError> {
        self.doc.text_for(obj.as_ref(), Some(self.clock.clone()))
    }

    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError> {
        let clock = self.doc.clock_at(heads);
        self.doc.text_for(obj.as_ref(), Some(clock))
    }

    fn spans<O: AsRef<ExId>>(&self, obj: O) -> Result<Spans<'_>, AutomergeError> {
        self.doc.spans_for(obj.as_ref(), Some(self.clock.clone()))
    }

    fn spans_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Spans<'_>, AutomergeError> {
        let clock = self.doc.clock_at(heads);
        self.doc.spans_for(obj.as_ref(), Some(clock))
    }

    fn get_cursor<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
    ) -> Result<Cursor, AutomergeError> {
        let clock = at
            .map(|h| self.doc.clock_at(h))
            .unwrap_or_else(|| self.clock.clone());
        self.doc.get_cursor_for(
            obj.as_ref(),
            position.into(),
            Some(clock),
            MoveCursor::After,
        )
    }

    fn get_cursor_moving<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
        move_cursor: MoveCursor,
    ) -> Result<Cursor, AutomergeError> {
        let clock = at
            .map(|h| self.doc.clock_at(h))
            .unwrap_or_else(|| self.clock.clone());
        self.doc
            .get_cursor_for(obj.as_ref(), position.into(), Some(clock), move_cursor)
    }

    fn get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        cursor: &Cursor,
        at: Option<&[ChangeHash]>,
    ) -> Result<usize, AutomergeError> {
        let clock = at
            .map(|h| self.doc.clock_at(h))
            .unwrap_or_else(|| self.clock.clone());
        self.doc
            .get_cursor_position_for(obj.as_ref(), cursor, Some(clock))
    }

    fn hydrate<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeHash]>,
    ) -> Result<hydrate::Value, AutomergeError> {
        let heads_to_use = heads.unwrap_or(&self.heads);
        ReadDoc::hydrate(self.doc, obj, Some(heads_to_use))
    }

    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.doc
            .get_for(obj.as_ref(), prop.into(), Some(self.clock.clone()))
    }

    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        let clock = self.doc.clock_at(heads);
        self.doc.get_for(obj.as_ref(), prop.into(), Some(clock))
    }

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.doc
            .get_all_for(obj.as_ref(), prop.into(), Some(self.clock.clone()))
    }

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let clock = self.doc.clock_at(heads);
        self.doc.get_all_for(obj.as_ref(), prop.into(), Some(clock))
    }

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.doc.get_missing_deps(heads)
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<Change> {
        self.doc.get_change_by_hash(hash)
    }

    fn stats(&self) -> crate::read::Stats {
        self.doc.stats()
    }

    fn text_encoding(&self) -> TextEncoding {
        self.doc.text_encoding()
    }
}
