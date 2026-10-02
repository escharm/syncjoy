use serde_json::json;
use syncjoy_protocol::PeerId;

#[test]
fn peer_id_round_trips_through_json() {
    let encoded = serde_json::to_value(PeerId::new("alice-42_~")).unwrap();
    assert_eq!(encoded, json!("alice-42_~"));
    let decoded: PeerId = serde_json::from_value(json!("alice-42_~")).unwrap();
    assert_eq!(decoded, PeerId::new("alice-42_~"));
}

#[test]
fn peer_id_rejects_oversize_input() {
    let oversize = "a".repeat(129);
    let error = serde_json::from_value::<PeerId>(json!(oversize))
        .expect_err("129 bytes must be rejected");
    assert!(error.to_string().contains("128"), "got {error}");
}

#[test]
fn peer_id_rejects_non_printable_ascii() {
    for bad in ["has space", "tab\tchar", "new\nline", "中文", "nul\0byte"] {
        assert!(
            serde_json::from_value::<PeerId>(json!(bad)).is_err(),
            "{bad:?} must be rejected"
        );
    }
}

#[test]
fn peer_id_accepts_boundary_input() {
    let max = "a".repeat(128);
    assert!(serde_json::from_value::<PeerId>(json!(max)).is_ok());
    assert!(serde_json::from_value::<PeerId>(json!("!~-_.:")).is_ok());
}
