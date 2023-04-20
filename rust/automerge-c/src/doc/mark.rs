use am::marks::{ExpandMark, Mark};
use automerge as am;
use automerge::transaction::Transactable;
use automerge::ReadDoc;

use crate::{
    byte_span::{to_str, AMbyteSpan},
    item::{AMitem, Item},
    items::AMitems,
    obj::{to_obj_id, AMobjId},
    result::{to_result, AMresult},
};

use super::{
    utils::{to_doc, to_doc_mut},
    AMdoc,
};

/// \ingroup enumerations
/// \enum AMmarkExpand
/// \installed_headerfile
/// \brief How to expand marks as the underlying text is edited.
#[derive(PartialEq, Eq)]
#[repr(u8)]
#[allow(dead_code)]
pub enum AMmarkExpand {
    /// Mark will include new text inserted at the start index
    Before = 1,
    /// Mark will include new text inserted at the end index
    After = 2,
    /// Mark will include new text inserted on either side
    Both = 3,
    /// Mark will not include text inserted at start or end
    None = 0,
}

impl From<AMmarkExpand> for ExpandMark {
    fn from(val: AMmarkExpand) -> Self {
        use AMmarkExpand::*;

        match val {
            Before => ExpandMark::Before,
            After => ExpandMark::After,
            Both => ExpandMark::Both,
            _ => ExpandMark::None,
        }
    }
}

pub struct AMmark<'a>(Mark<'a>);

impl<'a> AMmark<'a> {
    pub fn new(mark: Mark<'a>) -> Self {
        Self(mark)
    }
}

impl<'a> AsRef<Mark<'a>> for AMmark<'a> {
    fn as_ref(&self) -> &Mark<'a> {
        &self.0
    }
}

/// \memberof AMmark
/// \brief Gets the name of the mark
///
/// \param[in] mark A pointer to an `AMmark` struct.
/// \return An `AMbyteSpan` struct for a UTF-8 string.
/// \pre \p mark `!= NULL`
/// \warning The returned `AMbyteSpan` is only valid until the
///          result which returned the `AMmark` is freed.
/// \internal
///
/// # Safety
/// mark must be a valid pointer to an AMmark
#[no_mangle]
pub unsafe extern "C" fn AMmarkName(mark: *const AMmark) -> AMbyteSpan {
    if let Some(mark) = mark.as_ref() {
        if let Ok(name) = mark.as_ref().name().as_bytes().try_into() {
            return name;
        }
    }
    AMbyteSpan::default()
}

/// \memberof AMmark
/// \brief Gets the value of the mark
///
/// \param[in] mark A pointer to an `AMmark` struct.
/// \return A pointer to an `AMresult` struct with an `AMitem` struct.
/// \pre \p mark `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// mark must be a valid pointer to an AMmark
#[no_mangle]
pub unsafe extern "C" fn AMmarkValue(mark: *const AMmark) -> *mut AMresult {
    match mark.as_ref() {
        Some(mark) => to_result(mark.as_ref().value()),
        None => AMresult::error("invalid mark").into(),
    }
}

/// \memberof AMmark
/// \brief Gets the current start offset of the mark
///
/// \param[in] mark A pointer to an `AMmark` struct.
/// \return The offset at which the mark starts
/// \pre \p mark `!= NULL`
/// \internal
///
/// # Safety
/// mark must be a valid pointer to an AMmark
#[no_mangle]
pub unsafe extern "C" fn AMmarkStart(mark: *const AMmark) -> usize {
    match mark.as_ref() {
        Some(mark) => mark.as_ref().start,
        None => usize::MIN,
    }
}

/// \memberof AMmark
/// \brief Gets the current end offset of the mark
///
/// \param[in] mark A pointer to an `AMmark` struct.
/// \return The offset at which the mark starts
/// \pre \p mark `!= NULL`
/// \internal
///
/// # Safety
/// mark must be a valid pointer to an AMmark
#[no_mangle]
pub unsafe extern "C" fn AMmarkEnd(mark: *const AMmark) -> usize {
    match mark.as_ref() {
        Some(mark) => mark.as_ref().end,
        None => usize::MAX,
    }
}

/// \memberof AMdoc
/// \brief Gets the marks associated with this object
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct.
/// \param[in] heads A pointer to an `AMitems` struct.
/// \return An `AMresult` struct containing items of type `AM_VAL_TYPE_MARK`
/// \pre \p doc `!= NULL`
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an `AMdoc` struct
#[no_mangle]
pub unsafe extern "C" fn AMmarks(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    match heads.as_ref() {
        None => to_result(doc.marks(obj_id)),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => to_result(doc.marks_at(obj_id, &heads)),
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

/// \memberof AMdoc
/// \brief Creates a new mark
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct.
/// \param[in] start The start offset
/// \param[in] end The end offset
/// \param[in] expand How to handle future edits at the mark boundary
/// \param[in] name An `AMbyteSpan` struct containing valid utf-8 for the marks' name
/// \param[in] value An `AMitem` struct for the marks' value.
/// \return An `AMresult` struct containing no items.
/// \pre \p doc `!= NULL`
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an `AMdoc` struct
#[no_mangle]
pub unsafe extern "C" fn AMmarkCreate(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    start: usize,
    end: usize,
    expand: AMmarkExpand,
    name: AMbyteSpan,
    value: *const AMitem,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let name = to_str!(name);

    let item: &Item = value.as_ref().unwrap().as_ref();

    match <&am::ScalarValue>::try_from(item) {
        Ok(v) => {
            let mark = Mark::new(name.into(), v.clone(), start, end);
            to_result(doc.mark(obj_id, mark, expand.into()))
        }
        Err(e) => AMresult::Error(e.to_string()).into(),
    }
}

/// \memberof AMdoc
/// \brief Clears a mark
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct.
/// \param[in] start The start offset
/// \param[in] end The end offset
/// \param[in] expand How to handle future edits at the mark boundary
/// \param[in] name An `AMbyteSpan` struct containing valid utf-8 for the mark to clear
/// \return An `AMresult` struct containing no items.
/// \pre \p doc `!= NULL`
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an `AMdoc` struct
#[no_mangle]
pub unsafe extern "C" fn AMmarkClear(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    start: usize,
    end: usize,
    expand: AMmarkExpand,
    name: AMbyteSpan,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let name = to_str!(name);

    to_result(doc.unmark(obj_id, name, start, end, expand.into()))
}
