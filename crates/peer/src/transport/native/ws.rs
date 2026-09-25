use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use super::super::spawn;

pub(crate) async fn connect_signaling(
    url: String,
) -> Result<(UnboundedSender<String>, UnboundedReceiver<String>), String> {
    let (ws, _) = connect_async(url).await.map_err(|e| e.to_string())?;
    let (mut sink, mut stream) = ws.split();

    let (out_tx, mut out_rx) = unbounded_channel::<String>();
    let (in_tx, in_rx) = unbounded_channel::<String>();
    spawn(async move {
        loop {
            tokio::select! {
                outgoing = out_rx.recv() => match outgoing {
                    Some(text) => {
                        if sink.send(Message::Text(text)).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                },
                incoming = stream.next() => match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if in_tx.send(text).is_err() {
                            break;
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                },
            }
        }
    });
    Ok((out_tx, in_rx))
}
