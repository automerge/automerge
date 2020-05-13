#![allow(dead_code, unused_imports)]

use std::time::*;
use wasm_bindgen::prelude::*;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0) as i64
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = Date, js_name = now)]
    fn date_now() -> f64;
}
#[cfg(target_arch = "wasm32")]
pub(crate) fn unix_timestamp() -> i64 {
    // returns millis - we resolve to seconds
    (date_now() / 1000.0) as i64
}
