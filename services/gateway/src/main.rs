use std::{net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use montlok_gateway::{GatewayState, router};
use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(about = "Montlok v2 loopback gateway")]
struct Arguments {
    #[arg(long, env = "MONTLOK_GATEWAY_BIND", default_value = "127.0.0.1:8090")]
    bind: SocketAddr,
    #[arg(long, env = "MONTLOK_GATEWAY_DB", default_value = "var/gateway.sqlite")]
    database: PathBuf,
    #[arg(long, env = "MONTLOK_CONTROL_SOCKET")]
    control_socket: Option<PathBuf>,
    #[arg(long, env = "MONTLOK_NATS_URL")]
    nats_url: Option<String>,
    #[arg(
        long,
        env = "MONTLOK_GATEWAY_CONSUMER",
        default_value = "montlok-gateway-v2"
    )]
    consumer: String,
    #[arg(long, env = "MONTLOK_READ_TOKEN_FILE")]
    read_token_file: Option<PathBuf>,
    #[arg(
        long,
        env = "MONTLOK_PUBLIC_ORIGIN",
        default_value = "https://tokyo.montlok.com"
    )]
    public_origin: String,
    #[arg(
        long,
        env = "MONTLOK_BFF_URL",
        default_value = "http://127.0.0.1:18081"
    )]
    bff_url: String,
    #[arg(long, env = "MONTLOK_GATEWAY_SECRET_FILE")]
    gateway_secret_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "montlok_gateway=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().json())
        .init();
    let arguments = Arguments::parse();
    if !arguments.bind.ip().is_loopback() {
        anyhow::bail!(
            "gateway must bind to loopback until authenticated proxy/device middleware is configured"
        );
    }
    let mut state = GatewayState::new(arguments.database, arguments.control_socket)?;
    state.auth.public_origin = arguments.public_origin;
    let bff_url = reqwest::Url::parse(&arguments.bff_url)?;
    if !matches!(
        bff_url.host_str(),
        Some("127.0.0.1" | "localhost" | "[::1]")
    ) {
        anyhow::bail!("BFF endpoint must be local");
    }
    state.auth.bff_url = arguments.bff_url;
    if let Some(path) = arguments.gateway_secret_file {
        let key = std::fs::read_to_string(path)?.trim().to_string();
        if key.len() < 32 {
            anyhow::bail!("gateway signing key must be at least 32 bytes");
        }
        state.auth.bridge_key = Some(key);
    }
    if let Some(path) = arguments.read_token_file {
        let token = std::fs::read_to_string(path)?.trim().to_owned();
        if token.len() < 32 {
            anyhow::bail!("service read token must be at least 32 characters");
        }
        state.auth.read_token = Some(token);
    }
    if let Some(url) = arguments.nats_url {
        let hub = state.events.clone();
        let consumer = arguments.consumer;
        tokio::spawn(async move {
            loop {
                if let Err(error) = montlok_gateway::bus::follow(hub.clone(), &url, &consumer).await
                {
                    tracing::error!(
                        ?error,
                        "event subscription interrupted; retained snapshot remains available"
                    );
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }
    let listener = TcpListener::bind(arguments.bind).await?;
    tracing::info!(address = %arguments.bind, "Montlok gateway listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=terminate.recv()=>{} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
