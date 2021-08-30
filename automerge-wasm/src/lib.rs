//#![feature(set_stdio)]

//use automerge::{Automerge};
//use js_sys::Array;
//use serde::{de::DeserializeOwned, Serialize};
use wasm_bindgen::prelude::*;

extern crate web_sys;
#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
          web_sys::console::log_1(&format!( $( $t )* ).into());
    };
}

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
#[derive(Debug)]
pub struct Automerge(automerge::Automerge);

#[wasm_bindgen]
impl Automerge {
    pub fn new() -> Self { Automerge(automerge::Automerge::new()) }
    pub fn clone(&self) -> Self { Automerge(self.0.clone()) }
    pub fn free(self) { }
}

#[wasm_bindgen]
pub fn init() -> Result<Automerge, JsValue> {
    Ok(Automerge::new())
}

