use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use syncjoy_relay::{session, Relay};
use tokio::net::TcpListener;
use tokio::sync::mpsc::unbounded_channel;

pub async fn serve(relay: Relay) -> Result<String, Box<dyn std::error::Error>> {
    let app = Router::new()
        .route("/syncjoy/v1/peer", get(upgrade))
        .with_state(relay);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("ws://{addr}"))
}

async fn upgrade(
    ws: WebSocketUpgrade,
    State(relay): State<Relay>,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| bridge(socket, relay))
}

async fn bridge(socket: WebSocket, relay: Relay) {
    let (mut sink, mut stream): (SplitSink<WebSocket, Message>, SplitStream<WebSocket>) =
        socket.split();
    let (in_tx, in_rx) = unbounded_channel::<String>();
    let (out_tx, mut out_rx) = unbounded_channel::<String>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                incoming = stream.next() => match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if in_tx.send(text.as_str().to_owned()).is_err() {
                            break;
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                },
                outgoing = out_rx.recv() => match outgoing {
                    Some(text) => {
                        if sink.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                },
            }
        }
    });

    session::run(relay, in_rx, out_tx).await;
}
