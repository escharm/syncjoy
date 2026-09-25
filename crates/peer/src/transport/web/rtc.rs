use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use js_sys::Array;
use syncjoy_protocol::IceCandidate;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    RtcConfiguration, RtcDataChannel, RtcDataChannelEvent, RtcDataChannelState,
    RtcIceCandidateInit, RtcPeerConnection, RtcPeerConnectionIceEvent, RtcSdpType,
    RtcSessionDescriptionInit,
};

use super::super::IceServer;

pub(crate) fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

pub(crate) async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, ()> {
    use futures_util::future::{select, Either};
    let millis = duration.as_millis().min(u32::MAX as u128) as u64;
    match select(
        Box::pin(future),
        Box::pin(gloo_timers::future::sleep(Duration::from_millis(millis))),
    )
    .await
    {
        Either::Left((output, _)) => Ok(output),
        Either::Right(_) => Err(()),
    }
}

pub(crate) struct PeerConnection(RtcPeerConnection);

pub(crate) struct DataChannel(RtcDataChannel);

impl Clone for PeerConnection {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Clone for DataChannel {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

fn ice_server_object(server: &IceServer) -> JsValue {
    let entry = js_sys::Object::new();
    js_sys::Reflect::set(
        &entry,
        &"urls".into(),
        &Array::from_iter(server.urls.iter().map(JsValue::from)),
    )
    .unwrap();
    js_sys::Reflect::set(&entry, &"username".into(), &server.username.clone().into()).unwrap();
    js_sys::Reflect::set(
        &entry,
        &"credential".into(),
        &server.credential.clone().into(),
    )
    .unwrap();
    entry.into()
}

pub(crate) async fn new_peer_connection(servers: Vec<IceServer>) -> Result<PeerConnection, String> {
    let config = RtcConfiguration::new();
    config.set_ice_servers(&Array::from_iter(servers.iter().map(ice_server_object)));
    let pc = RtcPeerConnection::new_with_configuration(&config).map_err(|e| format!("{e:?}"))?;
    Ok(PeerConnection(pc))
}

impl PeerConnection {
    pub(crate) fn on_ice_candidate(&self, handler: Box<dyn Fn(IceCandidate) + Send + Sync>) {
        let handler: std::sync::Arc<dyn Fn(IceCandidate) + Send + Sync> =
            std::sync::Arc::from(handler);
        let on_candidate = Closure::wrap(Box::new(move |event: RtcPeerConnectionIceEvent| {
            if let Some(candidate) = event.candidate() {
                let json = candidate.to_json();
                let field = |key: &str| js_sys::Reflect::get(&json, &JsValue::from_str(key)).ok();
                let number = |key: &str| field(key).and_then(|value| value.as_f64());
                handler(IceCandidate {
                    candidate: field("candidate")
                        .and_then(|value| value.as_string())
                        .unwrap_or_default(),
                    sdp_mid: field("sdpMid").and_then(|value| value.as_string()),
                    sdp_mline_index: number("sdpMLineIndex").map(|f| f as u16),
                });
            }
        }) as Box<dyn FnMut(RtcPeerConnectionIceEvent)>);
        self.0
            .set_onicecandidate(Some(on_candidate.as_ref().unchecked_ref()));
        on_candidate.forget();
    }

    pub(crate) fn on_data_channel(&self, handler: Box<dyn Fn(DataChannel) + Send + Sync>) {
        let handler: std::sync::Arc<dyn Fn(DataChannel) + Send + Sync> =
            std::sync::Arc::from(handler);
        let on_channel = Closure::wrap(Box::new(move |event: RtcDataChannelEvent| {
            handler(DataChannel(event.channel()));
        }) as Box<dyn FnMut(RtcDataChannelEvent)>);
        self.0
            .set_ondatachannel(Some(on_channel.as_ref().unchecked_ref()));
        on_channel.forget();
    }

    pub(crate) async fn create_data_channel(&self, label: &str) -> Result<DataChannel, String> {
        Ok(DataChannel(self.0.create_data_channel(label)))
    }

    pub(crate) async fn create_offer(&self) -> Result<String, String> {
        let promise = self.0.create_offer();
        let init: RtcSessionDescriptionInit = JsFuture::from(promise)
            .await
            .map_err(|e| format!("{e:?}"))?
            .unchecked_into();
        JsFuture::from(self.0.set_local_description(&init))
            .await
            .map_err(|e| format!("{e:?}"))?;
        Ok(self.local_description().await)
    }

    pub(crate) async fn create_answer(&self) -> Result<String, String> {
        let promise = self.0.create_answer();
        let init: RtcSessionDescriptionInit = JsFuture::from(promise)
            .await
            .map_err(|e| format!("{e:?}"))?
            .unchecked_into();
        JsFuture::from(self.0.set_local_description(&init))
            .await
            .map_err(|e| format!("{e:?}"))?;
        Ok(self.local_description().await)
    }

    pub(crate) async fn local_description(&self) -> String {
        self.0
            .local_description()
            .map(|desc| desc.sdp())
            .map(String::from)
            .unwrap_or_default()
    }

    pub(crate) async fn set_remote_offer(&self, sdp: &str) -> Result<(), String> {
        let init = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
        init.set_sdp(sdp);
        JsFuture::from(self.0.set_remote_description(&init))
            .await
            .map(|_| ())
            .map_err(|e| format!("{e:?}"))
    }

    pub(crate) async fn set_remote_answer(&self, sdp: &str) -> Result<(), String> {
        let init = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
        init.set_sdp(sdp);
        JsFuture::from(self.0.set_remote_description(&init))
            .await
            .map(|_| ())
            .map_err(|e| format!("{e:?}"))
    }

    pub(crate) async fn add_ice_candidate(&self, candidate: &IceCandidate) {
        let init = RtcIceCandidateInit::new(&candidate.candidate);
        init.set_sdp_m_line_index(candidate.sdp_mline_index);
        let _ = JsFuture::from(
            self.0
                .add_ice_candidate_with_opt_rtc_ice_candidate_init(Some(&init)),
        )
        .await;
    }

    pub(crate) fn close(&self) {
        self.0.close();
    }
}

impl DataChannel {
    pub(crate) fn is_open(&self) -> bool {
        self.0.ready_state() == RtcDataChannelState::Open
    }

    pub(crate) async fn send(&self, bytes: Bytes) -> Result<(), String> {
        self.0
            .send_with_u8_array(&bytes)
            .map_err(|e| format!("{e:?}"))
    }

    pub(crate) fn on_message(&self, handler: Box<dyn Fn(Bytes) + Send + Sync>) {
        let handler: std::sync::Arc<dyn Fn(Bytes) + Send + Sync> = std::sync::Arc::from(handler);
        let on_message = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
            if let Ok(buffer) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                handler(Bytes::from(js_sys::Uint8Array::new(&buffer).to_vec()));
            }
        }) as Box<dyn FnMut(web_sys::MessageEvent)>);
        self.0
            .set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        on_message.forget();
    }

    pub(crate) fn on_open(&self, handler: Box<dyn Fn() + Send + Sync>) {
        let handler: std::sync::Arc<dyn Fn() + Send + Sync> = std::sync::Arc::from(handler);
        let on_open = Closure::wrap(Box::new(move |_: web_sys::Event| {
            handler();
        }) as Box<dyn FnMut(web_sys::Event)>);
        self.0.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        on_open.forget();
    }

    pub(crate) fn on_close(&self, handler: Box<dyn Fn() + Send + Sync>) {
        let handler: std::sync::Arc<dyn Fn() + Send + Sync> = std::sync::Arc::from(handler);
        let on_close = Closure::wrap(Box::new(move |_: web_sys::Event| {
            handler();
        }) as Box<dyn FnMut(web_sys::Event)>);
        self.0.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        on_close.forget();
    }
}
