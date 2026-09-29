mod chat;
mod screen;
mod ui;

use std::sync::Arc;
use std::time::Duration;

use chat::{CloseNotice, Receiver, Submission};
use clap::Parser;
use ratatui::crossterm::event::{self, KeyCode, KeyEventKind, KeyModifiers};
use screen::Screen;
use syncjoy_peer::{Event as PeerEvent, IncomingHandler, Peer, PeerError, PeerLink};
use syncjoy_protocol::PeerId;
use tokio::sync::Mutex;

/// Run one peer against a syncjoy relay.
#[derive(Parser)]
#[command(name = "peer-demo")]
struct Args {
    /// This peer's identity.
    #[arg(long)]
    id: String,

    /// The peer this demo talks to.
    #[arg(long)]
    to: String,

    /// Reject incoming connection requests (this peer only dials out).
    #[arg(long)]
    reject_incoming: bool,

    /// Relay websocket url.
    #[arg(long, env = "SYNCJOY_RELAY", default_value = "ws://127.0.0.1:7700")]
    relay: String,
}

struct RejectAll;

#[async_trait::async_trait]
impl IncomingHandler for RejectAll {
    async fn incoming(&self, _from: &PeerId) -> bool {
        false
    }
}

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
    let args = Args::parse();
    let peer = Peer::builder().id(args.id).build();
    let mut relay_link = peer.connect_relay(&args.relay).await?;
    let id = peer.id().clone();
    let to = PeerId::new(args.to);
    let screen = Arc::new(Screen::default());

    if args.reject_incoming {
        relay_link.on_incoming(RejectAll);
    }

    let handle = relay_link.handle();
    let peer_link_slot = Arc::new(Mutex::new(None::<PeerLink>));

    let consumer_screen = Arc::clone(&screen);
    let consumer_slot = Arc::clone(&peer_link_slot);
    tokio::spawn(async move {
        while let Some(PeerEvent::Channel(mut incoming)) = relay_link.next_event().await {
            incoming
                .on_msg(Receiver {
                    screen: Arc::clone(&consumer_screen),
                })
                .await;
            incoming.on_close(CloseNotice {
                screen: Arc::clone(&consumer_screen),
            });
            consumer_screen.set_connected(true);
            consumer_screen.log(format!("channel open: {}", incoming.peer()));
            *consumer_slot.lock().await = Some(incoming);
        }
    });

    screen.log(format!("ctrl-n connects to {to}"));
    screen.log(if args.reject_incoming {
        "incoming: rejected (dial out to connect)".to_owned()
    } else {
        "incoming: auto-accepted".to_owned()
    });
    screen.log("enter sends · /quit or ctrl-c exits".into());

    let mut terminal = ratatui::init();
    loop {
        terminal.draw(|frame| ui::draw(frame, &screen, &id, &to, &args.relay))?;

        if !event::poll(Duration::from_millis(100))? {
            continue;
        }
        let input_event = event::read()?;
        let event::Event::Key(key) = &input_event else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char('c') if ctrl => break,
            KeyCode::Char('n') if ctrl => {
                if screen.is_connected() {
                    screen.log(format!("already connected to {to}"));
                    continue;
                }
                screen.log(format!("connecting to {to}"));
                let slot = Arc::clone(&peer_link_slot);
                let dial_handle = handle.clone();
                let dial_screen = Arc::clone(&screen);
                let dial_to = to.clone();
                tokio::spawn(async move {
                    if dial_screen.is_connected() {
                        return;
                    }
                    match dial_handle.connect_peer(dial_to.clone()).await {
                        Ok(mut channel) => {
                            channel
                                .on_msg(Receiver {
                                    screen: Arc::clone(&dial_screen),
                                })
                                .await;
                            channel.on_close(CloseNotice {
                                screen: Arc::clone(&dial_screen),
                            });
                            dial_screen.set_connected(true);
                            dial_screen.log(format!("connected to {dial_to}"));
                            *slot.lock().await = Some(channel);
                        }
                        Err(error) => {
                            let occupied = slot.lock().await.is_some();
                            if !occupied {
                                dial_screen.log(format!("connect to {dial_to} failed: {error}"));
                            }
                        }
                    }
                });
            }
            KeyCode::Enter => match Submission::parse(&screen.take_line()) {
                Submission::Quit => break,
                Submission::Skip => {}
                Submission::Unknown(command) => screen.log(format!("unknown command: /{command}")),
                Submission::Send(text) => {
                    let slot = Arc::clone(&peer_link_slot);
                    let send_screen = Arc::clone(&screen);
                    let send_id = id.clone();
                    let send_to = to.clone();
                    tokio::spawn(async move {
                        let guard = slot.lock().await;
                        let Some(channel) = guard.as_ref() else {
                            send_screen.log(format!("not connected: ctrl-n dials {send_to}"));
                            return;
                        };
                        match channel.send(text.as_bytes()).await {
                            Ok(()) => send_screen.log(format!("[{send_id}] {text}")),
                            Err(PeerError::Rejected(reason)) => {
                                send_screen.log(format!("send failed: {reason}"))
                            }
                            Err(error) => {
                                send_screen.log(format!("link failed: {error}, ctrl-n redials"));
                                send_screen.set_connected(false);
                                drop(guard);
                                *slot.lock().await = None;
                            }
                        }
                    });
                }
            },
            KeyCode::Char(_) if !ctrl => {
                if !screen.block_over_limit() {
                    screen.edit(&input_event);
                }
            }
            _ => screen.edit(&input_event),
        }
    }
    ratatui::restore();
    Ok(())
}
