//! The document behind the wasm `Automerge`, in audit mode or not.
//!
//! In audit mode history is named by change hash, as it always has been.
//! Outside it history is named by change id (`"<seq>@<actor>"`) and saves
//! use the change set format.

use automerge as am;
use automerge::marks::{Mark, MarkSet};
use automerge::next::transaction::Transactable as _;
use automerge::next::ReadDoc as _;
use automerge::transaction::Transactable as _;
use automerge::{
    AutomergeError, Change, ChangeHash, ChangeId, ChangeMetadata, Cursor, CursorPosition,
    MoveCursor, ObjId, Parents, Patch, Prop, ReadDoc as _, Value,
};
use js_sys::Array;
use wasm_bindgen::{JsCast, JsValue};

use crate::interop::{error::BadChangeHashes, JS};

/// Runs `$body` against whichever document `$doc` holds, for calls that
/// have the same shape in both modes.
macro_rules! with_doc {
    ($doc:expr, $d:ident => $body:expr) => {
        match $doc {
            $crate::doc::Doc::Audit($d) => $body,
            $crate::doc::Doc::Next($d) => $body,
        }
    };
}
pub(crate) use with_doc;

#[derive(Debug, Clone)]
pub(crate) enum Doc {
    Audit(am::AutoCommit),
    Next(am::next::AutoCommit),
}

/// Heads in the form the document's mode names history by. Only
/// [`Doc::parse_heads`] makes them, so they always match the document.
pub(crate) enum Heads {
    Hashes(Vec<ChangeHash>),
    Ids(Vec<ChangeId>),
}

const MODE_MISMATCH: &str = "heads are parsed for the document's mode";

impl Heads {
    fn hashes(&self) -> &[ChangeHash] {
        match self {
            Heads::Hashes(h) => h,
            Heads::Ids(_) => unreachable!("{MODE_MISMATCH}"),
        }
    }

    fn ids(&self) -> &[ChangeId] {
        match self {
            Heads::Ids(h) => h,
            Heads::Hashes(_) => unreachable!("{MODE_MISMATCH}"),
        }
    }
}

/// A change named by hash or by id; either form is accepted in either mode.
pub(crate) enum ChangeRef {
    Hash(ChangeHash),
    Id(ChangeId),
}

impl ChangeRef {
    pub(crate) fn parse(value: &JsValue) -> Result<Self, String> {
        let s = value
            .as_string()
            .ok_or_else(|| "change must be a hash or change id string".to_string())?;
        if s.contains('@') {
            s.parse().map(ChangeRef::Id).map_err(|e| format!("{e}"))
        } else {
            s.parse().map(ChangeRef::Hash).map_err(|e| format!("{e}"))
        }
    }
}

pub(crate) fn change_ids_from_js(ids: &Array) -> Result<Vec<ChangeId>, BadChangeHashes> {
    ids.iter()
        .enumerate()
        .map(|(i, v)| {
            v.as_string()
                .ok_or_else(|| BadChangeHashes::NotChangeId(i, "not a string".to_string()))?
                .parse::<ChangeId>()
                .map_err(|e| BadChangeHashes::NotChangeId(i, e.to_string()))
        })
        .collect()
}

fn ids_to_js(ids: &[ChangeId]) -> Array {
    ids.iter()
        .map(|id| JsValue::from_str(&id.to_string()))
        .collect()
}

fn hashes_to_js(hashes: &[ChangeHash]) -> Array {
    hashes
        .iter()
        .map(|h| JsValue::from_str(&h.to_string()))
        .collect()
}

impl Doc {
    pub(crate) fn audit_mode(&self) -> bool {
        matches!(self, Doc::Audit(_))
    }

    /// Fails if a hash the document already held does not match the change
    /// it names, leaving the document as it was.
    pub(crate) fn set_audit_mode(&mut self, enable: bool) -> Result<(), AutomergeError> {
        if enable == self.audit_mode() {
            return Ok(());
        }
        let placeholder = Doc::Next(am::next::AutoCommit::new());
        match std::mem::replace(self, placeholder) {
            Doc::Audit(doc) => *self = Doc::Next(doc.into_next()),
            Doc::Next(doc) => match doc.into_audit() {
                Ok(doc) => *self = Doc::Audit(doc),
                Err(e) => {
                    *self = Doc::Next(*e.doc);
                    return Err(e.error);
                }
            },
        }
        Ok(())
    }

    /// `None` for `undefined` or `null`.
    pub(crate) fn parse_heads(&self, heads: JsValue) -> Result<Option<Heads>, BadChangeHashes> {
        if heads.is_undefined() || heads.is_null() {
            return Ok(None);
        }
        let Ok(heads) = heads.dyn_into::<Array>() else {
            return Err(BadChangeHashes::NotArray);
        };
        match self {
            Doc::Audit(_) => {
                let hashes = heads
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        ChangeHash::try_from(JS(v)).map_err(|e| BadChangeHashes::BadElem(i, e))
                    })
                    .collect::<Result<_, _>>()?;
                Ok(Some(Heads::Hashes(hashes)))
            }
            Doc::Next(_) => Ok(Some(Heads::Ids(change_ids_from_js(&heads)?))),
        }
    }

    pub(crate) fn heads(&mut self) -> Array {
        match self {
            Doc::Audit(d) => hashes_to_js(&d.get_heads()),
            Doc::Next(d) => ids_to_js(&d.get_heads()),
        }
    }

    pub(crate) fn commit_with(&mut self, options: am::transaction::CommitOptions) -> JsValue {
        match self {
            Doc::Audit(d) => d.commit_with(options).map(|h| h.to_string()),
            Doc::Next(d) => d.commit_with(options).map(|id| id.to_string()),
        }
        .map(JsValue::from)
        .unwrap_or(JsValue::NULL)
    }

    pub(crate) fn empty_change(&mut self, options: am::transaction::CommitOptions) -> JsValue {
        match self {
            Doc::Audit(d) => d.empty_change(options).to_string(),
            Doc::Next(d) => d.empty_change(options).to_string(),
        }
        .into()
    }

    /// Documents in different modes merge through a saved copy of `other`.
    pub(crate) fn merge(&mut self, other: &mut Doc) -> Result<Array, AutomergeError> {
        match (&mut *self, other) {
            (Doc::Audit(d), Doc::Audit(o)) => Ok(hashes_to_js(&d.merge(o)?)),
            (Doc::Next(d), Doc::Next(o)) => Ok(ids_to_js(&d.merge(o)?)),
            (_, other) => {
                let bytes = with_doc!(other, o => o.save());
                self.load_incremental(&bytes)?;
                Ok(self.heads())
            }
        }
    }

    pub(crate) fn fork(&mut self, heads: Option<&Heads>) -> Result<Doc, AutomergeError> {
        Ok(match (self, heads) {
            (Doc::Audit(d), None) => Doc::Audit(d.fork()),
            (Doc::Audit(d), Some(h)) => Doc::Audit(d.fork_at(h.hashes())?),
            (Doc::Next(d), None) => Doc::Next(d.fork()),
            (Doc::Next(d), Some(h)) => Doc::Next(d.fork_at(h.ids())?),
        })
    }

    pub(crate) fn anonymize(&mut self) -> Result<Doc, am::AnonymizeError> {
        Ok(match self {
            Doc::Audit(d) => Doc::Audit(d.anonymize()?),
            Doc::Next(d) => Doc::Next(d.anonymize()?),
        })
    }

    pub(crate) fn keys(
        &self,
        obj: &ObjId,
        heads: Option<&Heads>,
    ) -> Result<Vec<String>, AutomergeError> {
        Ok(match (self, heads) {
            (Doc::Audit(d), None) => d.keys(obj).collect(),
            (Doc::Audit(d), Some(h)) => d.keys_at(obj, h.hashes()).collect(),
            (Doc::Next(d), None) => d.keys(obj).collect(),
            (Doc::Next(d), Some(h)) => d.keys_at(obj, h.ids())?.collect(),
        })
    }

    pub(crate) fn text(
        &self,
        obj: &ObjId,
        heads: Option<&Heads>,
    ) -> Result<String, AutomergeError> {
        match (self, heads) {
            (Doc::Audit(d), None) => d.text(obj),
            (Doc::Audit(d), Some(h)) => d.text_at(obj, h.hashes()),
            (Doc::Next(d), None) => d.text(obj),
            (Doc::Next(d), Some(h)) => d.text_at(obj, h.ids()),
        }
    }

    pub(crate) fn spans(
        &self,
        obj: &ObjId,
        heads: Option<&Heads>,
    ) -> Result<am::iter::Spans<'_>, AutomergeError> {
        match (self, heads) {
            (Doc::Audit(d), None) => d.spans(obj),
            (Doc::Audit(d), Some(h)) => d.spans_at(obj, h.hashes()),
            (Doc::Next(d), None) => d.spans(obj),
            (Doc::Next(d), Some(h)) => d.spans_at(obj, h.ids()),
        }
    }

    pub(crate) fn get(
        &self,
        obj: &ObjId,
        prop: Prop,
        heads: Option<&Heads>,
    ) -> Result<Option<(Value<'_>, ObjId)>, AutomergeError> {
        match (self, heads) {
            (Doc::Audit(d), None) => d.get(obj, prop),
            (Doc::Audit(d), Some(h)) => d.get_at(obj, prop, h.hashes()),
            (Doc::Next(d), None) => d.get(obj, prop),
            (Doc::Next(d), Some(h)) => d.get_at(obj, prop, h.ids()),
        }
    }

    pub(crate) fn get_all(
        &self,
        obj: &ObjId,
        prop: Prop,
        heads: Option<&Heads>,
    ) -> Result<Vec<(Value<'_>, ObjId)>, AutomergeError> {
        match (self, heads) {
            (Doc::Audit(d), None) => d.get_all(obj, prop),
            (Doc::Audit(d), Some(h)) => d.get_all_at(obj, prop, h.hashes()),
            (Doc::Next(d), None) => d.get_all(obj, prop),
            (Doc::Next(d), Some(h)) => d.get_all_at(obj, prop, h.ids()),
        }
    }

    pub(crate) fn parents(
        &self,
        obj: &ObjId,
        heads: Option<&Heads>,
    ) -> Result<Parents<'_>, AutomergeError> {
        match (self, heads) {
            (Doc::Audit(d), None) => d.parents(obj),
            (Doc::Audit(d), Some(h)) => d.parents_at(obj, h.hashes()),
            (Doc::Next(d), None) => d.parents(obj),
            (Doc::Next(d), Some(h)) => d.parents_at(obj, h.ids()),
        }
    }

    pub(crate) fn length(
        &self,
        obj: &ObjId,
        heads: Option<&Heads>,
    ) -> Result<usize, AutomergeError> {
        Ok(match (self, heads) {
            (Doc::Audit(d), None) => d.length(obj),
            (Doc::Audit(d), Some(h)) => d.length_at(obj, h.hashes()),
            (Doc::Next(d), None) => d.length(obj),
            (Doc::Next(d), Some(h)) => d.length_at(obj, h.ids())?,
        })
    }

    pub(crate) fn iter(
        &self,
        obj: &ObjId,
        heads: Option<&Heads>,
    ) -> Result<am::iter::DocIter<'_>, AutomergeError> {
        match self {
            Doc::Audit(d) => Ok(d.iter_at(obj, heads.map(Heads::hashes))),
            Doc::Next(d) => d.iter_at(obj, heads.map(Heads::ids)),
        }
    }

    pub(crate) fn hydrate(
        &self,
        obj: &ObjId,
        heads: Option<&Heads>,
    ) -> Result<am::hydrate::Value, AutomergeError> {
        match self {
            Doc::Audit(d) => d.hydrate(obj, heads.map(Heads::hashes)),
            Doc::Next(d) => d.hydrate(obj, heads.map(Heads::ids)),
        }
    }

    pub(crate) fn marks(
        &self,
        obj: &ObjId,
        heads: Option<&Heads>,
    ) -> Result<Vec<Mark>, AutomergeError> {
        match (self, heads) {
            (Doc::Audit(d), None) => d.marks(obj),
            (Doc::Audit(d), Some(h)) => d.marks_at(obj, h.hashes()),
            (Doc::Next(d), None) => d.marks(obj),
            (Doc::Next(d), Some(h)) => d.marks_at(obj, h.ids()),
        }
    }

    pub(crate) fn get_marks(
        &self,
        obj: &ObjId,
        index: usize,
        heads: Option<&Heads>,
    ) -> Result<MarkSet, AutomergeError> {
        match self {
            Doc::Audit(d) => d.get_marks(obj, index, heads.map(Heads::hashes)),
            Doc::Next(d) => d.get_marks(obj, index, heads.map(Heads::ids)),
        }
    }

    pub(crate) fn get_cursor(
        &self,
        obj: &ObjId,
        position: CursorPosition,
        heads: Option<&Heads>,
        move_cursor: Option<MoveCursor>,
    ) -> Result<Cursor, AutomergeError> {
        match (self, move_cursor) {
            (Doc::Audit(d), None) => d.get_cursor(obj, position, heads.map(Heads::hashes)),
            (Doc::Audit(d), Some(m)) => {
                d.get_cursor_moving(obj, position, heads.map(Heads::hashes), m)
            }
            (Doc::Next(d), None) => d.get_cursor(obj, position, heads.map(Heads::ids)),
            (Doc::Next(d), Some(m)) => d.get_cursor_moving(obj, position, heads.map(Heads::ids), m),
        }
    }

    pub(crate) fn get_cursor_position(
        &self,
        obj: &ObjId,
        cursor: &Cursor,
        heads: Option<&Heads>,
    ) -> Result<usize, AutomergeError> {
        match self {
            Doc::Audit(d) => d.get_cursor_position(obj, cursor, heads.map(Heads::hashes)),
            Doc::Next(d) => d.get_cursor_position(obj, cursor, heads.map(Heads::ids)),
        }
    }

    pub(crate) fn diff(
        &mut self,
        before: &Heads,
        after: &Heads,
    ) -> Result<Vec<Patch>, AutomergeError> {
        match self {
            Doc::Audit(d) => Ok(d.diff(before.hashes(), after.hashes())),
            Doc::Next(d) => d.diff(before.ids(), after.ids()),
        }
    }

    pub(crate) fn diff_obj(
        &mut self,
        obj: &ObjId,
        before: &Heads,
        after: &Heads,
        recursive: bool,
    ) -> Result<Vec<Patch>, AutomergeError> {
        match self {
            Doc::Audit(d) => d.diff_obj(obj, before.hashes(), after.hashes(), recursive),
            Doc::Next(d) => d.diff_obj(obj, before.ids(), after.ids(), recursive),
        }
    }

    pub(crate) fn isolate(&mut self, heads: &Heads) -> Result<(), AutomergeError> {
        match self {
            Doc::Audit(d) => {
                d.isolate(heads.hashes());
                Ok(())
            }
            Doc::Next(d) => d.isolate(heads.ids()),
        }
    }

    pub(crate) fn mask_author(&mut self, author: am::Author<'static>, heads: Option<&Heads>) {
        match self {
            Doc::Audit(d) => d.mask_author(author, heads.map_or(&[], Heads::hashes)),
            Doc::Next(d) => d.mask_author(author, heads.map_or(&[], Heads::ids)),
        }
    }

    pub(crate) fn save_after(&mut self, heads: Option<&Heads>) -> Result<Vec<u8>, AutomergeError> {
        match self {
            Doc::Audit(d) => Ok(d.save_after(heads.map_or(&[], Heads::hashes))),
            Doc::Next(d) => d.save_after(heads.map_or(&[], Heads::ids)),
        }
    }

    pub(crate) fn save_and_verify(&mut self) -> Result<Vec<u8>, AutomergeError> {
        with_doc!(self, d => d.save_and_verify())
    }

    pub(crate) fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        with_doc!(self, d => d.load_incremental(data))
    }

    pub(crate) fn apply_changes(&mut self, changes: Vec<Change>) -> Result<(), AutomergeError> {
        with_doc!(self, d => d.apply_changes(changes))
    }

    pub(crate) fn get_changes(
        &mut self,
        have_deps: Option<&Heads>,
    ) -> Result<Vec<Change>, AutomergeError> {
        match self {
            Doc::Audit(d) => Ok(d.get_changes(have_deps.map_or(&[], Heads::hashes))),
            Doc::Next(d) => d.get_changes(have_deps.map_or(&[], Heads::ids)),
        }
    }

    pub(crate) fn get_changes_meta(
        &mut self,
        have_deps: Option<&Heads>,
    ) -> Result<Vec<ChangeMetadata<'_>>, AutomergeError> {
        match self {
            Doc::Audit(d) => Ok(d.get_changes_meta(have_deps.map_or(&[], Heads::hashes))),
            Doc::Next(d) => d.get_changes_meta(have_deps.map_or(&[], Heads::ids)),
        }
    }

    /// Fails for documents in different modes.
    pub(crate) fn get_changes_added(
        &mut self,
        other: &mut Doc,
    ) -> Result<Vec<Change>, AutomergeError> {
        match (self, other) {
            (Doc::Audit(d), Doc::Audit(o)) => Ok(d.get_changes_added(o)),
            (Doc::Next(d), Doc::Next(o)) => d.document().get_changes_added_legacy(o.document()),
            _ => Err(AutomergeError::AuditModeRequired),
        }
    }

    pub(crate) fn get_change_by_hash(
        &mut self,
        hash: &ChangeHash,
    ) -> Result<Option<Change>, AutomergeError> {
        match self {
            Doc::Audit(d) => Ok(d.get_change_by_hash(hash)),
            Doc::Next(d) => d.get_change_by_hash(hash),
        }
    }

    pub(crate) fn get_change_meta_by_hash(
        &mut self,
        hash: &ChangeHash,
    ) -> Result<Option<ChangeMetadata<'_>>, AutomergeError> {
        match self {
            Doc::Audit(d) => Ok(d.get_change_meta_by_hash(hash)),
            Doc::Next(d) => d.get_change_meta_by_hash(hash),
        }
    }

    pub(crate) fn get_change(
        &mut self,
        change: &ChangeRef,
    ) -> Result<Option<Change>, AutomergeError> {
        match (self, change) {
            (Doc::Audit(d), ChangeRef::Hash(h)) => Ok(d.get_change_by_hash(h)),
            (Doc::Audit(d), ChangeRef::Id(id)) => Ok(d.get_change_by_id(id)),
            (Doc::Next(d), ChangeRef::Hash(h)) => d.get_change_by_hash(h),
            (Doc::Next(d), ChangeRef::Id(id)) => d.get_change_by_id(id),
        }
    }

    /// A hash in audit mode and a change id outside it; never rebuilds a
    /// change.
    pub(crate) fn has_change(&mut self, change: &JsValue) -> Result<bool, String> {
        match self {
            Doc::Audit(d) => {
                let hash = ChangeHash::try_from(JS(change.clone())).map_err(|e| e.to_string())?;
                Ok(d.has_change(&hash))
            }
            Doc::Next(d) => {
                let id = change
                    .as_string()
                    .ok_or_else(|| "change id was not a string".to_string())?
                    .parse::<ChangeId>()
                    .map_err(|e| e.to_string())?;
                Ok(d.has_change_id(&id))
            }
        }
    }

    pub(crate) fn get_last_local_change(&mut self) -> Result<Option<Change>, AutomergeError> {
        match self {
            Doc::Audit(d) => Ok(d.get_last_local_change()),
            Doc::Next(d) => d.get_last_local_change_legacy(),
        }
    }

    pub(crate) fn get_missing_deps(
        &mut self,
        heads: Option<&Heads>,
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        match self {
            Doc::Audit(d) => Ok(d.get_missing_deps(heads.map_or(&[], Heads::hashes))),
            Doc::Next(d) => d.get_missing_deps(heads.map_or(&[], Heads::ids)),
        }
    }

    /// Sync exchanges change hashes, so it needs audit mode.
    pub(crate) fn audited(&mut self) -> Result<&mut am::AutoCommit, AutomergeError> {
        match self {
            Doc::Audit(d) => Ok(d),
            Doc::Next(_) => Err(AutomergeError::AuditModeRequired),
        }
    }
}

/// Calls that look the same in both modes, so `lib.rs` can use them as it
/// would a single document.
impl Doc {
    pub(crate) fn pending_ops(&self) -> usize {
        with_doc!(self, d => d.pending_ops())
    }

    pub(crate) fn put<O: AsRef<ObjId>, P: Into<Prop>, V: Into<am::ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<(), AutomergeError> {
        with_doc!(self, d => d.put(obj, prop, value))
    }

    pub(crate) fn put_object<O: AsRef<ObjId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        object: am::ObjType,
    ) -> Result<ObjId, AutomergeError> {
        with_doc!(self, d => d.put_object(obj, prop, object))
    }

    pub(crate) fn insert<O: AsRef<ObjId>, V: Into<am::ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        with_doc!(self, d => d.insert(obj, index, value))
    }

    pub(crate) fn insert_object<O: AsRef<ObjId>>(
        &mut self,
        obj: O,
        index: usize,
        object: am::ObjType,
    ) -> Result<ObjId, AutomergeError> {
        with_doc!(self, d => d.insert_object(obj, index, object))
    }

    pub(crate) fn increment<O: AsRef<ObjId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        with_doc!(self, d => d.increment(obj, prop, value))
    }

    pub(crate) fn delete<O: AsRef<ObjId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), AutomergeError> {
        with_doc!(self, d => d.delete(obj, prop))
    }

    pub(crate) fn splice<O, V, I>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        vals: I,
    ) -> Result<(), AutomergeError>
    where
        O: AsRef<ObjId>,
        V: Into<am::hydrate::Value>,
        I: IntoIterator<Item = V>,
    {
        with_doc!(self, d => d.splice(obj, pos, del, vals))
    }

    pub(crate) fn splice_text<O: AsRef<ObjId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        text: &str,
    ) -> Result<(), AutomergeError> {
        with_doc!(self, d => d.splice_text(obj, pos, del, text))
    }

    pub(crate) fn mark<O: AsRef<ObjId>>(
        &mut self,
        obj: O,
        mark: Mark,
        expand: am::marks::ExpandMark,
    ) -> Result<(), AutomergeError> {
        with_doc!(self, d => d.mark(obj, mark, expand))
    }

    pub(crate) fn split_block<O: AsRef<ObjId>>(
        &mut self,
        obj: O,
        index: usize,
    ) -> Result<ObjId, AutomergeError> {
        with_doc!(self, d => d.split_block(obj, index))
    }

    pub(crate) fn join_block<O: AsRef<ObjId>>(
        &mut self,
        text: O,
        index: usize,
    ) -> Result<(), AutomergeError> {
        with_doc!(self, d => d.join_block(text, index))
    }

    pub(crate) fn replace_block<O: AsRef<ObjId>>(
        &mut self,
        text: O,
        index: usize,
    ) -> Result<ObjId, AutomergeError> {
        with_doc!(self, d => d.replace_block(text, index))
    }

    pub(crate) fn update_spans<O: AsRef<ObjId>, I: IntoIterator<Item = am::iter::Span>>(
        &mut self,
        text: O,
        config: am::marks::UpdateSpansConfig,
        new_text: I,
    ) -> Result<(), AutomergeError> {
        with_doc!(self, d => d.update_spans(text, config, new_text))
    }

    pub(crate) fn update_text<S: AsRef<str>>(
        &mut self,
        obj: &ObjId,
        new_text: S,
    ) -> Result<(), AutomergeError> {
        with_doc!(self, d => d.update_text(obj, new_text))
    }

    pub(crate) fn update_object<O: AsRef<ObjId>>(
        &mut self,
        obj: O,
        new_value: &am::hydrate::Value,
    ) -> Result<(), am::error::UpdateObjectError> {
        with_doc!(self, d => d.update_object(obj, new_value))
    }

    pub(crate) fn batch_create_object<O: AsRef<ObjId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: &am::hydrate::Value,
        insert: bool,
    ) -> Result<ObjId, AutomergeError> {
        with_doc!(self, d => d.batch_create_object(obj, prop, value, insert))
    }

    pub(crate) fn init_root_from_hydrate(
        &mut self,
        value: &am::hydrate::Map,
    ) -> Result<(), AutomergeError> {
        with_doc!(self, d => d.init_root_from_hydrate(value))
    }

    pub(crate) fn rollback(&mut self) -> usize {
        with_doc!(self, d => d.rollback())
    }

    pub(crate) fn object_type<O: AsRef<ObjId>>(
        &self,
        obj: O,
    ) -> Result<am::ObjType, AutomergeError> {
        with_doc!(self, d => d.object_type(obj))
    }

    pub(crate) fn set_actor(&mut self, actor: am::ActorId) {
        with_doc!(self, d => { d.set_actor(actor); })
    }

    pub(crate) fn get_actor(&self) -> &am::ActorId {
        with_doc!(self, d => d.get_actor())
    }

    pub(crate) fn import(&self, s: &str) -> Result<(ObjId, am::ObjType), AutomergeError> {
        with_doc!(self, d => d.import(s))
    }

    pub(crate) fn diff_cursor_is_empty(&self) -> bool {
        with_doc!(self, d => d.diff_cursor().is_empty())
    }

    pub(crate) fn diff_incremental(&mut self) -> Vec<Patch> {
        with_doc!(self, d => d.diff_incremental())
    }

    pub(crate) fn update_diff_cursor(&mut self) {
        with_doc!(self, d => d.update_diff_cursor())
    }

    pub(crate) fn reset_diff_cursor(&mut self) {
        with_doc!(self, d => d.reset_diff_cursor())
    }

    pub(crate) fn integrate(&mut self) {
        with_doc!(self, d => d.integrate())
    }

    pub(crate) fn save(&mut self) -> Vec<u8> {
        with_doc!(self, d => d.save())
    }

    pub(crate) fn save_incremental(&mut self) -> Vec<u8> {
        with_doc!(self, d => d.save_incremental())
    }

    pub(crate) fn save_nocompress(&mut self) -> Vec<u8> {
        with_doc!(self, d => d.save_nocompress())
    }

    pub(crate) fn dump(&mut self) {
        with_doc!(self, d => d.dump())
    }

    pub(crate) fn stats(&self) -> am::Stats {
        with_doc!(self, d => d.stats())
    }
}
