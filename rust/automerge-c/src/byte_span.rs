use automerge as am;
use std::convert::TryFrom;
use std::os::raw::c_char;

use libc::strlen;
use smol_str::SmolStr;

macro_rules! to_str {
    ($byte_span:expr) => {{
        let result: Result<&str, am::AutomergeError> = (&$byte_span).try_into();
        match result {
            Ok(s) => s,
            Err(e) => return AMresult::error(&e.to_string()).into(),
        }
    }};
}

pub(crate) use to_str;

/// \struct AMbyteSpan
/// \installed_headerfile
/// \brief A view onto an array of bytes.
#[repr(C)]
pub struct AMbyteSpan {
    /// A pointer to the first byte of an array of bytes.
    /// \warning \p src is only valid until the array of bytes to which it
    ///          points is freed.
    /// \attention <b>NEVER CALL `free()` ON \p src if the `AMbyteSpan` came
    ///            from within an `AMitem` struct</b>; \p src will be freed
    ///            when `AMfree()` is called on the `AMresult` from which the
    ///            `AMitem` struct was retrieved.
    pub src: *const u8,
    /// The count of bytes in the array.
    pub count: usize,
}

impl AMbyteSpan {
    pub fn is_null(&self) -> bool {
        self.src.is_null()
    }
}

impl Default for AMbyteSpan {
    fn default() -> Self {
        Self {
            src: std::ptr::null(),
            count: 0,
        }
    }
}

impl PartialEq for AMbyteSpan {
    fn eq(&self, other: &Self) -> bool {
        if self.count != other.count {
            return false;
        } else if self.src == other.src {
            return true;
        }
        <&[u8]>::from(self) == <&[u8]>::from(other)
    }
}

impl Eq for AMbyteSpan {}

impl From<&am::ActorId> for AMbyteSpan {
    fn from(actor: &am::ActorId) -> Self {
        let slice = actor.to_bytes();
        Self {
            src: slice.as_ptr(),
            count: slice.len(),
        }
    }
}

impl From<&mut am::ActorId> for AMbyteSpan {
    fn from(actor: &mut am::ActorId) -> Self {
        actor.as_ref().into()
    }
}

impl From<&am::ChangeHash> for AMbyteSpan {
    fn from(change_hash: &am::ChangeHash) -> Self {
        Self {
            src: change_hash.0.as_ptr(),
            count: change_hash.0.len(),
        }
    }
}

impl From<*const c_char> for AMbyteSpan {
    fn from(cs: *const c_char) -> Self {
        if !cs.is_null() {
            Self {
                src: cs as *const u8,
                count: unsafe { strlen(cs) },
            }
        } else {
            Self::default()
        }
    }
}

impl From<&SmolStr> for AMbyteSpan {
    fn from(smol_str: &SmolStr) -> Self {
        smol_str.as_bytes().into()
    }
}

impl From<&[u8]> for AMbyteSpan {
    fn from(slice: &[u8]) -> Self {
        Self {
            src: slice.as_ptr(),
            count: slice.len(),
        }
    }
}

impl From<&AMbyteSpan> for &[u8] {
    fn from(byte_span: &AMbyteSpan) -> Self {
        unsafe { std::slice::from_raw_parts(byte_span.src, byte_span.count) }
    }
}

impl From<&AMbyteSpan> for Vec<u8> {
    fn from(byte_span: &AMbyteSpan) -> Self {
        <&[u8]>::from(byte_span).to_vec()
    }
}

impl TryFrom<&AMbyteSpan> for am::ChangeHash {
    type Error = am::AutomergeError;

    fn try_from(byte_span: &AMbyteSpan) -> Result<Self, Self::Error> {
        use am::AutomergeError::InvalidChangeHashBytes;

        let slice: &[u8] = byte_span.into();
        match slice.try_into() {
            Ok(change_hash) => Ok(change_hash),
            Err(e) => Err(InvalidChangeHashBytes(e)),
        }
    }
}

impl TryFrom<&AMbyteSpan> for &str {
    type Error = am::AutomergeError;

    fn try_from(byte_span: &AMbyteSpan) -> Result<Self, Self::Error> {
        use am::AutomergeError::InvalidCharacter;

        let slice = byte_span.into();
        match std::str::from_utf8(slice) {
            Ok(str_) => Ok(str_),
            Err(e) => Err(InvalidCharacter(e.valid_up_to())),
        }
    }
}

/// \brief Creates a view onto an array of bytes.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The count of bytes to view from the array pointed to by
///                  \p src.
/// \return An `AMbyteSpan` struct.
/// \pre \p count `<= sizeof(`\p src `)`
/// \internal
///
/// #Safety
/// src must be a byte array of length `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMbytes(src: *const u8, count: usize) -> AMbyteSpan {
    AMbyteSpan { src, count }
}

/// \brief Creates a string view from a C string.
///
/// \param[in] c_str A UTF-8 C string.
/// \return An `AMbyteSpan` struct for a UTF-8 string.
/// \internal
///
/// #Safety
/// c_str must be a null-terminated array of `c_char`
#[no_mangle]
pub unsafe extern "C" fn AMstr(c_str: *const c_char) -> AMbyteSpan {
    c_str.into()
}
