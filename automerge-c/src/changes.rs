use automerge as am;
use std::ffi::{c_void, CString};

/// \struct AMchange
/// \brief A group of operations performed by an actor.
pub struct AMchange {
    body: am::Change,
    c_message: Option<CString>,
}

impl AMchange {
    pub fn new(change: am::Change) -> Self {
        let c_message = match change.message() {
            Some(c_message) => CString::new(c_message).ok(),
            None => None,
        };
        Self {
            body: change,
            c_message,
        }
    }

    pub fn c_message(&self) -> Option<&CString> {
        self.c_message.as_ref()
    }
}

impl AsRef<am::Change> for AMchange {
    fn as_ref(&self) -> &am::Change {
        &self.body
    }
}

/// \struct AMchanges
/// \brief A bidirectional iterator over a sequence of `AMchange` structs.
#[repr(C)]
pub struct AMchanges {
    len: usize,
    offset: isize,
    ptr: *const c_void,
}

impl AsRef<[AMchange]> for AMchanges {
    fn as_ref(&self) -> &[AMchange] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const AMchange, self.len) }
    }
}

impl AMchanges {
    pub fn new(changes: &[AMchange]) -> Self {
        Self {
            len: changes.len(),
            offset: 0,
            ptr: changes.as_ptr() as *const c_void,
        }
    }

    pub fn advance(&mut self, n: isize) {
        let len = self.len as isize;
        if n != 0 && self.offset >= -len && self.offset < len {
            // It's being advanced and it hasn't stopped.
            self.offset = std::cmp::max(-(len + 1), std::cmp::min(self.offset + n, len));
        };
    }

    pub fn next(&mut self, n: isize) -> Option<&AMchange> {
        let len = self.len as isize;
        if self.offset < -len || self.offset == len {
            // It's stopped.
            None
        } else {
            let slice =
                unsafe { std::slice::from_raw_parts(self.ptr as *const AMchange, self.len) };
            let index = (self.offset + if self.offset < 0 { len } else { 0 }) as usize;
            let element = Some(&slice[index]);
            self.advance(n);
            element
        }
    }

    pub fn prev(&mut self, n: isize) -> Option<&AMchange> {
        self.advance(n);
        let len = self.len as isize;
        if self.offset < -len || self.offset == len {
            // It's stopped.
            None
        } else {
            let slice =
                unsafe { std::slice::from_raw_parts(self.ptr as *const AMchange, self.len) };
            let index = (self.offset + if self.offset < 0 { len } else { 0 }) as usize;
            Some(&slice[index])
        }
    }
}

/// \memberof AMchanges
/// \brief Advances/rewinds an `AMchanges` struct by at most \p |n|
/// positions.
///
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
/// number of positions to advance/rewind.
/// \pre \p changes must be a valid address.
/// \internal
///
/// #Safety
/// changes must be a pointer to a valid AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMadvanceChanges(changes: *mut AMchanges, n: isize) {
    if let Some(changes) = changes.as_mut() {
        changes.advance(n);
    };
}

/// \memberof AMchanges
/// \brief Gets the size of an `AMchanges` struct.
///
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \return The count of values in \p changes.
/// \pre \p changes must be a valid address.
/// \internal
///
/// #Safety
/// changes must be a pointer to a valid AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMchangesSize(changes: *const AMchanges) -> usize {
    if let Some(changes) = changes.as_ref() {
        changes.len
    } else {
        0
    }
}

/// \memberof AMchanges
/// \brief Gets the `AMchange` struct at the current position of an
/// `AMchanges`struct and then advances/rewinds it by at most \p |n|
/// positions.
///
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
/// number of positions to advance/rewind.
/// \return A pointer to an `AMchange` struct that's invalid when \p changes was
/// previously advanced/rewound past its forward/backward limit.
/// \pre \p changes must be a valid address.
/// \internal
///
/// #Safety
/// changes must be a pointer to a valid AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMnextChange(changes: *mut AMchanges, n: isize) -> *const AMchange {
    if let Some(changes) = changes.as_mut() {
        if let Some(change) = changes.next(n) {
            return change;
        }
    }
    std::ptr::null()
}

/// \memberof AMchanges
/// \brief Advances/rewinds an `AMchanges` struct by at most \p |n|
/// positions and then gets the `AMchange` struct at its current position.
///
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
/// number of positions to advance/rewind.
/// \return A pointer to an `AMchange` struct that's invalid when \p changes is
/// presently advanced/rewound past its forward/backward limit.
/// \pre \p changes must be a valid address.
/// \internal
///
/// #Safety
/// changes must be a pointer to a valid AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMprevChange(changes: *mut AMchanges, n: isize) -> *const AMchange {
    if let Some(changes) = changes.as_mut() {
        if let Some(change) = changes.prev(n) {
            return change;
        }
    }
    std::ptr::null()
}
