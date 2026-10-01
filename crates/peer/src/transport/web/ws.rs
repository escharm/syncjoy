use std::cell::RefCell;
use std::rc::Rc;

use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::spawn_local;
use web_sys::WebSocket;

use super::keepalive::Keepalive;

pub(crate) struct SignalingSender {
    inner: UnboundedSender<String>,
    #[allow(dead_code)]
    keepalive: Keepalive,
}

impl SignalingSender {
    pub(crate) fn send(&self, text: String) -> Result<(), SendError<String>> {
        self.inner.send(text)
    }
}

type Inbound = Rc<RefCell<Option<UnboundedSender<String>>>>;

pub(crate) async fn connect_signaling(
    url: String,
) -> Result<(SignalingSender, UnboundedReceiver<String>), String> {
    let ws = WebSocket::new(&url).map_err(|e| format!("{e:?}"))?;
    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

    let (in_tx, in_rx) = unbounded_channel::<String>();
    let (out_tx, mut out_rx) = unbounded_channel::<String>();
    let (opened_tx, mut opened_rx) = unbounded_channel::<Result<(), String>>();
    let inbound: Inbound = Rc::new(RefCell::new(Some(in_tx)));
    let keepalive = Keepalive::default();

    let on_message = {
        let inbound = Rc::clone(&inbound);
        Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
            let text = event.data().as_string();
            if let (Some(text), Some(in_tx)) = (text, inbound.borrow().as_ref().cloned()) {
                let _ = in_tx.send(text);
            }
        }) as Box<dyn FnMut(web_sys::MessageEvent)>)
    };
    ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    keepalive.keep(on_message.into_js_value());

    let on_open = {
        let opened_tx = opened_tx.clone();
        Closure::wrap(Box::new(move |_: JsValue| {
            let _ = opened_tx.send(Ok(()));
        }) as Box<dyn FnMut(JsValue)>)
    };
    ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

    let on_disconnected = {
        let ws = ws.clone();
        let inbound = Rc::clone(&inbound);
        Closure::wrap(Box::new(move |event: JsValue| {
            *inbound.borrow_mut() = None;
            let _ = opened_tx.send(Err(format!("{event:?}")));
            let _ = ws.close();
        }) as Box<dyn FnMut(JsValue)>)
    };
    ws.set_onerror(Some(on_disconnected.as_ref().unchecked_ref()));
    ws.set_onclose(Some(on_disconnected.as_ref().unchecked_ref()));
    keepalive.keep(on_disconnected.into_js_value());

    let opened = match opened_rx.recv().await {
        Some(opened) => opened,
        None => Err("signaling socket lost before open".to_owned()),
    };
    ws.set_onopen(None);
    drop(on_open);

    if let Err(reason) = opened {
        ws.set_onmessage(None);
        ws.set_onerror(None);
        ws.set_onclose(None);
        return Err(reason);
    }

    spawn_local({
        let ws = ws.clone();
        let keepalive = keepalive.clone();
        async move {
            while let Some(text) = out_rx.recv().await {
                if ws.send_with_str(&text).is_err() {
                    break;
                }
            }
            let _ = ws.close();
            drop(keepalive);
        }
    });

    Ok((
        SignalingSender {
            inner: out_tx,
            keepalive,
        },
        in_rx,
    ))
}
