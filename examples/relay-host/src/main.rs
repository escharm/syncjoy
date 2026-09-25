use syncjoy_relay::{Relay, TurnConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let relay = match (
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
    let app = relay.router();

    let addr = std::env::var("SYNCJOY_LISTEN").unwrap_or_else(|_| "0.0.0.0:7700".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("relay listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
