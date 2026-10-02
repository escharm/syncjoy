use std::path::Path;
use std::process::Command;

use clap::Parser;

mod chat_e2e;
mod harness;
mod verify;
mod ws_verify;

/// Workspace orchestration tasks.
#[derive(Parser)]
#[command(name = "xtask")]
enum Cli {
    /// Spawn relay-host plus two peer-demo TUIs in ptys and verify a chat round trip.
    E2e,
    /// Build and test the feature matrix: default, framework-free relay, wasm peer,
    /// and the native+web mutual-exclusion guard.
    Features,
    /// Regression-check the three fixed signaling behaviors against real binaries:
    /// routing-failure receipts, the connect timeout, and the link-closed event.
    Verify,
}

fn main() {
    let result = match Cli::parse() {
        Cli::E2e => chat_e2e::e2e().map(|()| "e2e passed"),
        Cli::Features => features().map(|()| "feature matrix passed"),
        Cli::Verify => verify::verify().map(|()| "verify passed"),
    };
    match result {
        Ok(label) => println!("{label}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn workspace_root() -> Result<(), String> {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let root = Path::new(manifest)
        .parent()
        .ok_or("no workspace root")?
        .to_path_buf();
    std::env::set_current_dir(&root).map_err(|e| e.to_string())
}

fn cargo(args: &[&str]) -> Result<(), String> {
    let status = Command::new("cargo")
        .args(args)
        .status()
        .map_err(|e| e.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("cargo {} failed", args.join(" ")))
    }
}

fn features() -> Result<(), String> {
    workspace_root()?;

    cargo(&["check", "--workspace", "--all-targets"])?;
    assert_no_http_framework()?;
    cargo(&[
        "check",
        "-p",
        "syncjoy-peer",
        "--target",
        "wasm32-unknown-unknown",
        "--no-default-features",
        "--features",
        "web",
    ])?;

    let guard = Command::new("cargo")
        .args(["check", "-p", "syncjoy-peer", "--all-features"])
        .output()
        .map_err(|e| e.to_string())?;
    if guard.status.success() {
        return Err("native+web must be rejected".into());
    }
    let stderr = String::from_utf8_lossy(&guard.stderr);
    if !stderr.contains("mutually exclusive") {
        return Err(format!(
            "expected the mutual-exclusion compile_error, got: {}",
            stderr.lines().next().unwrap_or_default()
        ));
    }
    Ok(())
}

fn assert_no_http_framework() -> Result<(), String> {
    let tree = Command::new("cargo")
        .args(["tree", "-p", "syncjoy-relay", "--edges", "normal"])
        .output()
        .map_err(|e| e.to_string())?;
    if !tree.status.success() {
        return Err("cargo tree syncjoy-relay failed".into());
    }
    let text = String::from_utf8_lossy(&tree.stdout);
    if let Some(line) = text.lines().find(|line| line.contains("axum")) {
        return Err(format!(
            "relay must stay http-framework-free, found: {line}"
        ));
    }
    Ok(())
}
