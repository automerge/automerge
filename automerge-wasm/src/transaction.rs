use js_sys::Array;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Debug)]
pub struct Transaction(pub(crate) automerge::Transaction<'static>);

#[wasm_bindgen]
impl Transaction {
    #[wasm_bindgen(js_name = pendingOps)]
    pub fn pending_ops(&self) -> JsValue {
        (self.0.pending_ops() as u32).into()
    }

    pub fn commit(&mut self, message: JsValue, time: JsValue) -> Array {
        let message = message.as_string();
        let time = time.as_f64().map(|v| v as i64);
        let heads = self.0.commit(message, time);
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
            .collect();
        heads
    }

    pub fn rollback(&mut self) -> JsValue {
        self.0.rollback().into()
    }
}
