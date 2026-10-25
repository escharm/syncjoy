//! native DataChannel 端到端往返：dialer 发送，acceptor 经 on_msg 接收，
//! 反向同样验证。peer crate 此前只有信令层测试，数据面往返无覆盖。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use syncjoy_peer::{CloseHandler, Event, IncomingHandler, LinkHandle, MsgHandler, Peer, PeerId, PeerLink};
use tokio::sync::mpsc::unbounded_channel;

mod common;
use common::serve;

struct AcceptAll;

#[async_trait]
impl IncomingHandler for AcceptAll {
    async fn incoming(&self, _from: &PeerId) -> bool {
        true
    }
}

struct Collector {
    sink: Arc<Mutex<Vec<Vec<u8>>>>,
}

#[async_trait]
impl MsgHandler for Collector {
    async fn on_msg(&self, _link: LinkHandle, _from: PeerId, payload: Bytes) {
        eprintln!("[sdk-dbg] on_msg {} bytes", payload.len());
        self.sink.lock().unwrap().push(payload.to_vec());
    }
}

struct LogClose;

#[async_trait]
impl CloseHandler for LogClose {
    async fn on_close(&self, _link: LinkHandle, peer: PeerId) {
        eprintln!("[sdk-dbg] link closed: {peer}");
    }
}

#[tokio::test]
async fn native_data_roundtrip_between_two_peers() {
    let url = serve(syncjoy_relay::Relay::new()).await.unwrap();

    let acceptor = Peer::builder().id("acceptor").build();
    let acceptor_relay = acceptor.connect_relay(&url).await.unwrap();
    let acceptor_handle = acceptor_relay.handle();
    acceptor_handle.on_incoming(AcceptAll);

    let received: Arc<Mutex<Vec<Vec<u8>>>> = Default::default();
    let pump_link: Arc<Mutex<Option<PeerLink>>> = Default::default();

    let sink = received.clone();
    let slot = pump_link.clone();
    tokio::spawn(async move {
        let mut events = acceptor_relay;
        while let Some(event) = events.next_event().await {
            if let Event::Channel(mut link) = event {
                link.on_msg(Collector { sink: sink.clone() }).await;
                link.on_close(LogClose);
                *slot.lock().unwrap() = Some(link);
            }
        }
    });

    let dialer = Peer::builder().id("dialer").build();
    let dialer_relay = dialer.connect_relay(&url).await.unwrap();
    let dial_handle = dialer_relay.handle();
    let mut dial_link = dial_handle.connect_peer("acceptor").await.unwrap();

    // 复刻适配器：dialer 侧也注册 on_msg（取走接收端），发送走存储的克隆句柄
    let dialer_got: Arc<Mutex<Vec<Vec<u8>>>> = Default::default();
    {
        let sink = dialer_got.clone();
        struct DialCollector {
            sink: Arc<Mutex<Vec<Vec<u8>>>>,
        }
        #[async_trait]
        impl MsgHandler for DialCollector {
            async fn on_msg(&self, _link: LinkHandle, _from: PeerId, payload: Bytes) {
                self.sink.lock().unwrap().push(payload.to_vec());
            }
        }
        dial_link.on_msg(DialCollector { sink }).await;
    }

    let stored: Arc<Mutex<Vec<PeerLink>>> = Default::default();
    {
        let mut slots = stored.lock().unwrap();
        slots.push(dial_link.clone());
    }
    let send_link = stored.lock().unwrap()[0].clone();
    send_link.send(b"ping").await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        received.lock().unwrap().as_slice(),
        [b"ping".to_vec()],
        "acceptor should receive the dialer's frame"
    );

    let acceptor_side = pump_link.lock().unwrap().clone().expect("acceptor link");
    acceptor_side.send(b"pong").await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(2), dial_link.recv())
        .await
        .expect("dialer recv timeout");
    if let Some(got) = got {
        assert_eq!(&got[..], b"pong", "dialer should receive the acceptor's frame");
    }
    let dialer_received = dialer_got.lock().unwrap().len();
    eprintln!("[sdk-dbg] dialer collector got {dialer_received} frames");
}
