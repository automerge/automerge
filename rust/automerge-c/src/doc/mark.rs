use automerge as am;

use std::any::type_name;

use am::marks::{ExpandMark, Mark};
use am::transaction::Transactable;
use am::ReadDoc;

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

macro_rules! to_expand_mark {
    ($mark_expand:expr) => {{
        let result: Result<ExpandMark, am::AutomergeError> = (&$mark_expand).try_into();
        match result {
            Ok(expand_mark) => expand_mark,
            Err(e) => return AMresult::error(&e.to_string()).into(),
        }
    }};
}

pub(crate) use to_expand_mark;

/// \ingroup enumerations
/// \enum AMmarkExpand
/// \installed_headerfile
/// \brief A mark's expansion mode for when bordering text is inserted.
#[derive(Eq, PartialEq)]
#[repr(C)]
pub enum AMmarkExpand {
    /// Include text inserted at the end offset.
    After = 3,
    /// Include text inserted at the start offset.
    Before = 2,
    /// Include text inserted at either offset.
    Both = 4,
    /// The default tag, not a mark expansion mode signifier.
    Default = 0,
    /// Exclude text inserted at either offset.
    None = 1,
}

impl Default for AMmarkExpand {
    fn default() -> Self {
        Self::Default
    }
}

impl TryFrom<&AMmarkExpand> for ExpandMark {
    type Error = am::AutomergeError;

    fn try_from(mark_expand: &AMmarkExpand) -> Result<Self, Self::Error> {
        use am::AutomergeError::InvalidValueType;
        use AMmarkExpand::*;

        match mark_expand {
            After => Ok(Self::After),
            Before => Ok(Self::Before),
            Both => Ok(Self::Both),
            None => Ok(Self::None),
            _ => Err(InvalidValueType {
                expected: type_name::<Self>().to_string(),
                unexpected: type_name::<u8>().to_string(),
            }),
        }
    }
}

/// \struct AMmark
/// \installed_headerfile
/// \brief An association of out-of-bound information with a list object or text
///        object.
pub struct AMmark(Mark);

impl AMmark {
    pub fn new(mark: Mark) -> Self {
        Self(mark)
    }
}

impl AsRef<Mark> for AMmark {
    fn as_ref(&self) -> &Mark {
        &self.0
    }
}

/// \memberof AMmark
/// \brief Gets the name of a mark.
///
/// \param[in] mark A pointer to an `AMmark` struct.
/// \return A UTF-8 string view as an `AMbyteSpan` struct.
/// \pre \p mark `!= NULL`
/// \post `(`\p mark `== NULL) -> (AMbyteSpan){NULL, 0}`
/// \internal
///
/// # Safety
/// mark must be a valid pointer to an AMmark
#[no_mangle]
pub unsafe extern "C" fn AMmarkName(mark: *const AMmark) -> AMbyteSpan {
    if let Some(mark) = mark.as_ref() {
        return mark.as_ref().name().as_bytes().into();
    }
    Default::default()
}

/// \memberof AMmark
/// \brief Gets the value of a mark.
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
/// \brief Gets the start offset of a mark.
///
/// \param[in] mark A pointer to an `AMmark` struct.
/// \return The offset at which the mark starts.
/// \pre \p mark `!= NULL`
/// \post `(`\p mark `== NULL) -> 0`
/// \internal
///
/// # Safety
/// mark must be a valid pointer to an AMmark
#[no_mangle]
pub unsafe extern "C" fn AMmarkStart(mark: *const AMmark) -> usize {
    if let Some(mark) = mark.as_ref() {
        return mark.as_ref().start;
    }
    0
}

/// \memberof AMmark
/// \brief Gets the end offset of a mark.
///
/// \param[in] mark A pointer to an `AMmark` struct.
/// \return The offset at which the mark ends.
/// \pre \p mark `!= NULL`
/// \post `(`\p mark `== NULL) -> SIZE_MAX`
/// \internal
///
/// # Safety
/// mark must be a valid pointer to an AMmark
#[no_mangle]
pub unsafe extern "C" fn AMmarkEnd(mark: *const AMmark) -> usize {
    if let Some(mark) = mark.as_ref() {
        return mark.as_ref().end;
    }
    usize::MAX
}

/// \memberof AMdoc
/// \brief Gets the marks associated with an object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select a historical object or `NULL` to select the
///                  current object.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_MARK` items.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMitems or std::ptr::null()
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
/// \brief Creates a mark.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] start The start offset of the mark.
/// \param[in] end The end offset of the mark.
/// \param[in] expand The mode of expanding the mark to include text inserted at
///                   one of its offsets.
/// \param[in] name A UTF-8 string view as an `AMbyteSpan`struct.
/// \param[in] value A pointer to an `AMitem` struct with an `AM_VAL_TYPE_MARK`.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p start `<` \p end
/// \pre \p name.src `!= NULL`
/// \pre \p name.count `<= sizeof(`\p name.src `)`
/// \pre \p value `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// name.src must be a byte array of length >= name.count
/// value must be a valid pointer to an AMitem
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
    let expand = to_expand_mark!(expand);
    let name = to_str!(name);
    let item: &Item = value.as_ref().unwrap().as_ref();
    match <&am::ScalarValue>::try_from(item) {
        Ok(v) => {
            let mark = Mark::new(name.into(), v.clone(), start, end);
            to_result(doc.mark(obj_id, mark, expand))
        }
        Err(e) => AMresult::Error(e.to_string()).into(),
    }
}

/// \memberof AMdoc
/// \brief Clears a mark.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] start The start offset of the mark.
/// \param[in] end The end offset of the mark.
/// \param[in] expand The mode of expanding the mark to include text inserted at
///                   one of its offsets.
/// \param[in] name A UTF-8 string view s an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p start `<` \p end
/// \pre \p name.src `!= NULL`
/// \pre \p name.count `<= sizeof(`\p name.src `)`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// name.src must be a byte array of length >= name.count
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
    let expand = to_expand_mark!(expand);
    let name = to_str!(name);
    to_result(doc.unmark(obj_id, name, start, end, expand))
}
