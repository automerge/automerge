#![feature(vec_into_raw_parts)]

extern crate libc;
extern crate errno;
extern crate serde;
extern crate automerge_backend;

use automerge_protocol::{ChangeRequest, Patch, ActorID};
use std::ffi::{CStr, CString};
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::os::raw::{ c_void, c_char };
use serde::ser::Serialize;
use errno::{set_errno,Errno};

fn to_json<T: Serialize>(data: T) -> *const c_char {
    let json = serde_json::to_string(&data).unwrap();
    let json = CString::new(json).unwrap();
    json.into_raw()
}

#[derive(Clone)]
pub struct Backend(automerge_backend::Backend);

impl Deref for Backend {
    type Target = automerge_backend::Backend;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe fn from_buf_raw<T>(ptr: *const T, elts: usize) -> Vec<T> {
    let mut dst = Vec::with_capacity(elts);
    dst.set_len(elts);
    ptr::copy(ptr, dst.as_mut_ptr(), elts);
    dst
}

impl DerefMut for Backend {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[repr(C)]
pub struct Value {
    pub datatype: usize,
    pub data: *mut c_void,
}

#[repr(C)]
pub struct Document {
    pub len: usize,
    pub cap: usize,
    pub ptr: *mut u8,
}

#[repr(C)]
pub struct Change {
    pub len: usize,
    pub cap: usize,
    pub ptr: *mut u8,
}

#[repr(C)]
pub struct Changes {
    pub len: usize,
    pub cap: usize,
    pub ptr: *mut Change,
}

impl From<*mut Backend> for Backend {
    fn from(b: *mut Backend) -> Self {
       unsafe {
           *Box::from_raw(b)
       }
    }
}
impl From<Backend> for *mut Backend {
    fn from(b: Backend) -> Self {
        Box::into_raw(Box::new(b))
    }
}

impl From<Vec<u8>> for Change {
    fn from(v: Vec<u8>) -> Self {
        let (ptr,len,cap) = v.into_raw_parts();
        Change {
            ptr, len, cap
        }
    }
}

impl From<Vec<Change>> for Changes {
    fn from(v: Vec<Change>) -> Self {
        let (ptr,len,cap) = v.into_raw_parts();
        Changes {
            ptr, len, cap
        }
    }
}

impl From<Changes> for *const Changes {
    fn from(c: Changes) -> Self {
        Box::into_raw(Box::new(c))
    }
}

impl From<Vec<u8>> for Document {
    fn from(v: Vec<u8>) -> Self {
        let (ptr,len,cap) = v.into_raw_parts();
        Document {
            ptr, len, cap
        }
    }
}

impl From<Document> for *const Document {
    fn from(c: Document) -> Self {
        Box::into_raw(Box::new(c))
    }
}

/*
  init => automerge_init
  clone => automerge_clone
  free => automerge_free
  save => automerge_save
  load => automerge_load
  applyLocalChange => automerge_apply_local_change
  getPatch => automerge_get_patch
  applyChanges => automerge_apply_changes
  loadChanges => automerge_load_changes
  getChangesForActor => automerge_get_changes_for_actor
  getChanges => automerge_get_changes
  getMissingDeps => automerge_get_missing_deps
  getUndoStack => ..
  getRedoStack => ..
*/

#[no_mangle]
pub extern "C" fn automerge_init() -> *mut Backend {
    Backend(automerge_backend::Backend::init()).into()
}

#[no_mangle]
pub unsafe extern "C" fn automerge_free(backend: *mut Backend) {
    let backend : Backend = backend.into();
    drop(backend)
}

#[no_mangle]
pub unsafe extern "C" fn automerge_free_string(string: *const c_char) {
    let string : CString = CString::from_raw(std::mem::transmute(string));
    drop(string);
}

#[no_mangle]
pub unsafe extern "C" fn automerge_free_changes(changes: *mut Changes) {
    let changes = Box::from_raw(changes);
    let changes : Vec<Change> = Vec::from_raw_parts(changes.ptr, changes.len, changes.cap);
    let data : Vec<Vec<u8>> = changes.iter().map(|ch| Vec::from_raw_parts(ch.ptr, ch.len, ch.cap)).collect();
    drop(data);
    drop(changes);
}

#[no_mangle]
pub unsafe extern "C" fn automerge_free_document(doc: *mut Document) {
    let doc = Box::from_raw(doc);
    let doc : Vec<u8> = Vec::from_raw_parts(doc.ptr, doc.len, doc.cap);
    drop(doc)
}

#[no_mangle]
pub unsafe extern "C" fn automerge_free_change(change: *mut Change) {
    let change = Box::from_raw(change);
    let change : Vec<u8> = Vec::from_raw_parts(change.ptr, change.len, change.cap);
    drop(change)
}

#[no_mangle]
pub unsafe extern "C" fn automerge_apply_local_change(backend: *mut Backend, request: *const c_char) -> *const c_char {
    let request : &CStr = CStr::from_ptr(request);
    let request = request.to_string_lossy();
    let request : ChangeRequest = serde_json::from_str(&request).unwrap();
    let patch : Patch = (*backend).apply_local_change(request).unwrap();
    to_json(patch)
}

#[no_mangle]
pub unsafe extern "C" fn automerge_apply_changes(backend: *mut Backend, num_changes: usize, changes: *const Change) -> *const c_char {
    let mut c : Vec<Vec<u8>> = Vec::new();
    for i in 0..num_changes {
        let change = changes.offset(i as isize);
        let bytes = from_buf_raw((*change).ptr, (*change).len);
        c.push(bytes)
    }
    if let Ok(patch) = (*backend).apply_changes_binary(c) {
        to_json(patch)
    } else {
        set_errno(Errno(1));
        ptr::null()
    }
}

#[no_mangle]
pub unsafe extern "C" fn automerge_get_patch(backend: *mut Backend) -> *const c_char {
    if let Ok(patch) = (*backend).get_patch() {
        to_json(patch)
    } else {
        set_errno(Errno(1));
        ptr::null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn automerge_load_changes(backend: *mut Backend, num_changes: usize, changes: *const Change) {
    let mut c : Vec<Vec<u8>> = Vec::new();
    for i in 0..num_changes {
        let change = changes.offset(i as isize);
        let bytes = from_buf_raw((*change).ptr, (*change).len);
        c.push(bytes)
    }
    if let Err(_) = (*backend).load_changes_binary(c) {
        set_errno(Errno(1));
    }
}

#[no_mangle]
pub unsafe extern "C" fn automerge_clone(backend: *mut Backend) -> *mut Backend {
    (*backend).clone().into()
}

#[no_mangle]
pub unsafe extern "C" fn automerge_save(backend: *mut Backend) -> *const Document {
    let backend : Backend = backend.into();
    if let Ok(data) = backend.save() {
        let change : Document = data.into();
        change.into()
    } else {
        set_errno(Errno(1));
        ptr::null()
    }
}

#[no_mangle]
pub unsafe extern "C" fn automerge_load(len: usize, binary: *const u8) -> *mut Backend {
    let bytes = from_buf_raw(binary, len);
    if let Ok(backend) = automerge_backend::Backend::load(bytes) {
        Backend(backend).into()
    } else {
        set_errno(Errno(1));
        ptr::null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn automerge_get_changes_for_actor(backend: *mut Backend, actor: *const c_char) -> *const Changes {
    let actor : &CStr = CStr::from_ptr(actor);
    let actor = actor.to_string_lossy();
    let actor : ActorID = actor.as_ref().into();
    if let Ok(mut changes) = (*backend).get_changes_for_actor_id(&actor) {
        let changes : Vec<Change> = changes.drain(..).map(|c| c.into()).collect();
        let changes : Changes = changes.into();
        changes.into()
    } else {
        set_errno(Errno(1));
        ptr::null()
    }
}

#[no_mangle]
pub unsafe extern "C" fn automerge_get_changes(backend: *mut Backend, len: usize, binary: *const u8) -> *const Changes {
    let mut have_deps = Vec::new();
    for i in 0..len {
        have_deps.push(from_buf_raw(binary.offset(i as isize * 32),32).as_slice().into())
    }
    if let Ok(mut changes) = (*backend).get_changes(&have_deps) {
        let changes : Vec<Change> = changes.drain(..).map(|c| c.into()).collect();
        let changes : Changes = changes.into();
        changes.into()
    } else {
        set_errno(Errno(1));
        ptr::null()
    }
}

#[no_mangle]
pub unsafe extern "C" fn automerge_get_missing_deps(backend: *mut Backend) -> *const c_char {
    let missing = (*backend).get_missing_deps();
    to_json(missing)
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
