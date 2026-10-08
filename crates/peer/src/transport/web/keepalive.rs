use std::sync::{Arc, Mutex};

use wasm_bindgen::JsValue;

use crate::lock;

#[derive(Clone, Default)]
pub(crate) struct Keepalive(Arc<Mutex<Vec<JsValue>>>);

impl Keepalive {
    pub(crate) fn keep(&self, kept: JsValue) {
        lock(&self.0).push(kept);
    }
}
