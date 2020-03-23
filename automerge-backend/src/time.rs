#![allow(dead_code, unused_imports)]

use wasm_bindgen::prelude::*;
pub use std::time::*;

#[cfg(not(target_arch = "wasm32"))]
pub fn unix_timestamp() -> u128 {
  std::time::SystemTime::now()
    .duration_since(std::time::SystemTime::UNIX_EPOCH)
    .map(|d| d.as_millis()).unwrap_or(0)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = Date, js_name = now)]
    fn date_now() -> f64;
}
#[cfg(target_arch = "wasm32")]
pub fn unix_timestamp() -> u128 {
  date_now() as u128
}
