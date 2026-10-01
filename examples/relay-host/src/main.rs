use syncjoy_relay::{Relay, TurnConfig};

fn install_panic_logger() {
    std::panic::set_hook(Box::new(|info| {
        eprintln!("{}", panic_record(info));
    }));
}

/// One self-sufficient record: message, location, thread, backtrace.
/// Printed unconditionally on every panic, no RUST_BACKTRACE needed.
fn panic_record(info: &std::panic::PanicHookInfo<'_>) -> String {
    let thread = std::thread::current();
    let thread_name = thread.name().unwrap_or("<unnamed>");
    let location = info
        .location()
        .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
        .unwrap_or_else(|| "unknown".to_owned());
    let message = info
        .payload_as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| "<non-string panic payload>".to_owned());
    format!(
        "panic in thread {thread_name} at {location}: {message}\nstack backtrace:\n{}",
        std::backtrace::Backtrace::force_capture()
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    install_panic_logger();
    let mut relay = match (
        std::env::var("SYNCJOY_TURN_URL"),
        std::env::var("SYNCJOY_TURN_SECRET"),
    ) {
        (Ok(urls), Ok(secret)) => Relay::with_turn(TurnConfig {
            urls: urls
                .split(',')
                .map(|url| url.trim().to_owned())
                .filter(|url| !url.is_empty())
                .collect(),
            secret,
        }),
        _ => Relay::new(),
    };
    if let Ok(expected) = std::env::var("SYNCJOY_PEER_TOKEN") {
        relay = relay.with_guard(move |_, token| token == Some(expected.as_str()));
        println!("registration guard on: hello token required");
    }

    let addr = std::env::var("SYNCJOY_LISTEN").unwrap_or_else(|_| "0.0.0.0:7700".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("relay listening on {}", listener.local_addr()?);
    axum::serve(listener, router(relay)).await?;
    Ok(())
}

fn router(relay: Relay) -> axum::Router {
    axum::Router::new()
        .route("/syncjoy/healthz", axum::routing::get(health))
        .route(
            "/syncjoy/v1/peer",
            axum::routing::get(upgrade).with_state(relay),
        )
}

async fn health() -> &'static str {
    "ok"
}

async fn upgrade(
    ws: axum::extract::WebSocketUpgrade,
    axum::extract::State(relay): axum::extract::State<Relay>,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| bridge(socket, relay))
}

async fn bridge(socket: axum::extract::ws::WebSocket, relay: Relay) {
    use axum::extract::ws::{Message, WebSocket};
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::mpsc::unbounded_channel;

    let (mut sink, mut stream): (
        futures_util::stream::SplitSink<WebSocket, Message>,
        futures_util::stream::SplitStream<WebSocket>,
    ) = socket.split();
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

    syncjoy_relay::session::run(relay, in_rx, out_tx).await;
}
