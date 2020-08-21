// futures::select! and futures::select_biased! are hungry.
// https://docs.rs/futures/0.3.5/futures/macro.select.html
#![recursion_limit = "1024"]

use std::env;

use log::error;
use log::info;

use tokio::net::TcpListener;

mod hanabi;
mod session;
mod sync;

use crate::hanabi::conn::handle_connect;

fn init_logging() {
    env_logger::Builder::new()
        .filter(None, log::LevelFilter::Info)
        .parse_filters(&env::var(env_logger::DEFAULT_FILTER_ENV).unwrap_or_default())
        .parse_write_style(&env::var(env_logger::DEFAULT_WRITE_STYLE_ENV).unwrap_or_default())
        .init()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9991".to_string());

    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(&addr).await;
    let mut listener = try_socket.expect("Failed to bind");
    info!("Listening on: {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_connect(stream).await {
                        error!("Peer {} connection closed: {}", peer_addr, e)
                    }
                });
            }
            Err(e) => error!("Unable to establish connection: {}", e),
        }
    }
}
