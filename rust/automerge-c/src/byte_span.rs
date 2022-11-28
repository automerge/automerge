use automerge as am;
use libc::strlen;
use std::convert::TryFrom;
use std::os::raw::c_char;

macro_rules! to_str {
    ($span:expr) => {{
        let result: Result<&str, am::AutomergeError> = (&$span).try_into();
        match result {
            Ok(s) => s,
            Err(e) => return AMresult::err(&e.to_string()).into(),
        }
    }};
}

pub(crate) use to_str;

/// \struct AMbyteSpan
/// \installed_headerfile
/// \brief A view onto a contiguous sequence of bytes.
#[repr(C)]
pub struct AMbyteSpan {
    /// A pointer to an array of bytes.
    /// \attention <b>NEVER CALL `free()` ON \p src!</b>
    /// \warning \p src is only valid until the `AMfree()` function is called
    ///          on the `AMresult` struct that stores the array of bytes to
    ///          which it points.
    pub src: *const u8,
    /// The number of bytes in the array.
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
        let slice = unsafe { std::slice::from_raw_parts(self.src, self.count) };
        let other_slice = unsafe { std::slice::from_raw_parts(other.src, other.count) };
        slice == other_slice
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
        let slice = actor.to_bytes();
        Self {
            src: slice.as_ptr(),
            count: slice.len(),
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

impl From<&am::ChangeHash> for AMbyteSpan {
    fn from(change_hash: &am::ChangeHash) -> Self {
        Self {
            src: change_hash.0.as_ptr(),
            count: change_hash.0.len(),
        }
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

impl TryFrom<&AMbyteSpan> for &str {
    type Error = am::AutomergeError;

    fn try_from(span: &AMbyteSpan) -> Result<Self, Self::Error> {
        use am::AutomergeError::InvalidCharacter;

        let slice = unsafe { std::slice::from_raw_parts(span.src, span.count) };
        match std::str::from_utf8(slice) {
            Ok(str_) => Ok(str_),
            Err(e) => Err(InvalidCharacter(e.valid_up_to())),
        }
    }
}
