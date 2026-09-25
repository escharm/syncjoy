use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use syncjoy_protocol::IceCandidate;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use super::super::IceServer;

pub(crate) fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

pub(crate) async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, ()> {
    tokio::time::timeout(duration, future).await.map_err(|_| ())
}

pub(crate) struct PeerConnection(Arc<RTCPeerConnection>);

pub(crate) struct DataChannel(Arc<RTCDataChannel>);

impl Clone for PeerConnection {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl Clone for DataChannel {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

pub(crate) async fn new_peer_connection(servers: Vec<IceServer>) -> Result<PeerConnection, String> {
    let ice_servers: Vec<webrtc::ice_transport::ice_server::RTCIceServer> = servers
        .into_iter()
        .map(|server| webrtc::ice_transport::ice_server::RTCIceServer {
            urls: server.urls,
            username: server.username,
            credential: server.credential,
            ..Default::default()
        })
        .collect();
    let config = RTCConfiguration {
        ice_servers,
        ..Default::default()
    };
    let pc = APIBuilder::new()
        .build()
        .new_peer_connection(config)
        .await
        .map_err(|e| e.to_string())?;
    Ok(PeerConnection(Arc::new(pc)))
}

impl PeerConnection {
    pub(crate) fn on_ice_candidate(&self, handler: Box<dyn Fn(IceCandidate) + Send + Sync>) {
        let handler: Arc<dyn Fn(IceCandidate) + Send + Sync> = Arc::from(handler);
        self.0.on_ice_candidate(Box::new(move |candidate| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let Some(candidate) = candidate else { return };
                let Ok(init) = candidate.to_json() else {
                    return;
                };
                handler(IceCandidate {
                    candidate: init.candidate,
                    sdp_mid: init.sdp_mid,
                    sdp_mline_index: init.sdp_mline_index,
                });
            })
        }));
    }

    pub(crate) fn on_data_channel(&self, handler: Box<dyn Fn(DataChannel) + Send + Sync>) {
        let handler: Arc<dyn Fn(DataChannel) + Send + Sync> = Arc::from(handler);
        self.0.on_data_channel(Box::new(move |data_channel| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                handler(DataChannel(data_channel));
            })
        }));
    }

    pub(crate) async fn create_data_channel(&self, label: &str) -> Result<DataChannel, String> {
        let dc = self
            .0
            .create_data_channel(label, None)
            .await
            .map_err(|e| e.to_string())?;
        Ok(DataChannel(dc))
    }

    pub(crate) async fn create_offer(&self) -> Result<String, String> {
        let offer = self.0.create_offer(None).await.map_err(|e| e.to_string())?;
        self.0
            .set_local_description(offer)
            .await
            .map_err(|e| e.to_string())?;
        Ok(self.local_description().await)
    }

    pub(crate) async fn create_answer(&self) -> Result<String, String> {
        let answer = self
            .0
            .create_answer(None)
            .await
            .map_err(|e| e.to_string())?;
        self.0
            .set_local_description(answer)
            .await
            .map_err(|e| e.to_string())?;
        Ok(self.local_description().await)
    }

    pub(crate) async fn local_description(&self) -> String {
        self.0
            .local_description()
            .await
            .map(|desc| desc.sdp)
            .unwrap_or_default()
    }

    pub(crate) async fn set_remote_offer(&self, sdp: &str) -> Result<(), String> {
        let offer = RTCSessionDescription::offer(sdp.to_string()).map_err(|e| e.to_string())?;
        self.0
            .set_remote_description(offer)
            .await
            .map_err(|e| e.to_string())
    }

    pub(crate) async fn set_remote_answer(&self, sdp: &str) -> Result<(), String> {
        let answer = RTCSessionDescription::answer(sdp.to_string()).map_err(|e| e.to_string())?;
        self.0
            .set_remote_description(answer)
            .await
            .map_err(|e| e.to_string())
    }

    pub(crate) async fn add_ice_candidate(&self, candidate: &IceCandidate) {
        let init = RTCIceCandidateInit {
            candidate: candidate.candidate.clone(),
            sdp_mid: candidate.sdp_mid.clone(),
            sdp_mline_index: candidate.sdp_mline_index,
            username_fragment: None,
        };
        let _ = self.0.add_ice_candidate(init).await;
    }

    pub(crate) fn close(&self) {
        let pc = Arc::clone(&self.0);
        spawn(async move {
            let _ = pc.close().await;
        });
    }
}

impl DataChannel {
    pub(crate) fn is_open(&self) -> bool {
        self.0.ready_state() == RTCDataChannelState::Open
    }

    pub(crate) async fn send(&self, bytes: Bytes) -> Result<(), String> {
        self.0
            .send(&bytes)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    pub(crate) fn on_message(&self, handler: Box<dyn Fn(Bytes) + Send + Sync>) {
        let handler: Arc<dyn Fn(Bytes) + Send + Sync> = Arc::from(handler);
        self.0.on_message(Box::new(move |msg| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                handler(Bytes::from(msg.data.to_vec()));
            })
        }));
    }

    pub(crate) fn on_open(&self, handler: Box<dyn Fn() + Send + Sync>) {
        let handler: Arc<dyn Fn() + Send + Sync> = Arc::from(handler);
        self.0.on_open(Box::new(move || {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                handler();
            })
        }));
    }

    pub(crate) fn on_close(&self, handler: Box<dyn Fn() + Send + Sync>) {
        let handler: Arc<dyn Fn() + Send + Sync> = Arc::from(handler);
        self.0.on_close(Box::new(move || {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                handler();
            })
        }));
    }
}
