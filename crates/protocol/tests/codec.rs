use syncjoy_protocol::{decode, encode, IceCandidate, PeerId, PeerToRelay, ProtocolError};

#[test]
fn hello_roundtrip() {
    let original = PeerToRelay::Hello {
        peer_id: PeerId::new("alice"),
    };
    let text = encode(&original);
    let parsed: PeerToRelay = decode(&text).unwrap();
    assert_eq!(parsed, original);
}

#[test]
fn ice_candidate_roundtrip() {
    let original = PeerToRelay::Ice {
        to: PeerId::new("bob"),
        candidate: IceCandidate {
            candidate: "candidate:1 1 UDP 1 192.168.1.4 5000 typ host".to_owned(),
            sdp_mid: Some("0".to_owned()),
            sdp_mline_index: Some(0),
        },
    };
    let text = encode(&original);
    let parsed: PeerToRelay = decode(&text).unwrap();
    assert_eq!(parsed, original);
}

#[test]
fn rejects_other_versions() {
    let text = r#"{"v":99,"msg":{"type":"pong"}}"#;
    let parsed: Result<PeerToRelay, _> = decode(text);
    assert!(matches!(
        parsed,
        Err(ProtocolError::UnsupportedVersion { found: 99 })
    ));
}
