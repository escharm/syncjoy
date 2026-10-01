use std::sync::{Arc, Mutex};

use wasm_bindgen::JsValue;

#[derive(Clone, Default)]
pub(crate) struct Keepalive(Arc<Mutex<Vec<JsValue>>>);

impl Keepalive {
    pub(crate) fn keep(&self, kept: JsValue) {
        self.0.lock().unwrap().push(kept);
    }
}
