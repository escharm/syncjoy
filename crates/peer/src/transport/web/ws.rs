use wasm_bindgen::closure::Closure;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;
use web_sys::WebSocket;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub(crate) async fn connect_signaling(
    url: String,
) -> Result<(UnboundedSender<String>, UnboundedReceiver<String>), String> {
    let ws = WebSocket::new(&url).map_err(|e| format!("{e:?}"))?;
    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

    let (in_tx, in_rx) = unbounded_channel::<String>();
    let (out_tx, mut out_rx) = unbounded_channel::<String>();

    let on_message = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
        if let Some(text) = event.data().as_string() {
            let _ = in_tx.send(text);
        }
    }) as Box<dyn FnMut(web_sys::MessageEvent)>);
    ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    on_message.forget();

    spawn_local(async move {
        while let Some(text) = out_rx.recv().await {
            let _ = ws.send_with_str(&text);
        }
    });

    Ok((out_tx, in_rx))
}
