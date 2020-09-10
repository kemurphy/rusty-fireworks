use std::collections::hash_map::HashMap;

use anyhow::Context as _;
use cookie::Cookie;
use hmac::{Hmac, NewMac};
use http::header::COOKIE;
use jwt::VerifyWithKey;
use log::info;
use sha2::Sha256;
use thiserror::Error;
use tokio::net::TcpStream;
use tungstenite::handshake::server::Request;

use crate::hanabi::session::{HanabiSession, UserId};
use crate::session;

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("missing session secret")]
    NoSecret,
    #[error("missing session cookie")]
    NoCookie,
    #[error("bad session secret")]
    BadSecret,
    #[error("bad session cookie")]
    BadCookie,
}

impl UserId {
    fn from_request(req: &Request) -> anyhow::Result<UserId> {
        let headers = req.headers();

        let secret = headers
            .get_all("x-session-secret")
            .iter()
            .last()
            .context(RequestError::NoSecret)?
            .as_bytes();

        let token = headers
            .get_all(COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .filter_map(|v| Cookie::parse(v).ok())
            .filter(|c| c.name() == "hanabi.sid")
            .last()
            .context(RequestError::NoCookie)?;

        let key: Hmac<Sha256> = Hmac::new_varkey(secret).or(Err(RequestError::BadSecret))?;
        let claims: HashMap<String, serde_json::Value> = token.value().verify_with_key(&key)?;

        claims
            .get("id")
            .and_then(|v| v.as_u64())
            .map(UserId::new)
            .context(RequestError::BadCookie)
    }
}

pub async fn handle_connect(stream: TcpStream) -> anyhow::Result<()> {
    let addr = stream
        .peer_addr()
        .expect("connected streams should have a peer address");
    info!("Peer address: {}", addr);

    let mut user_id = None;
    let accept_cb = |req: &Request, resp| {
        UserId::from_request(req)
            .map(|id| {
                info!("Got WS connection for {}", id);
                user_id.replace(id);
                resp
            })
            .map_err(|_| {
                use http::StatusCode;
                http::Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(None)
                    .unwrap()
            })
    };

    let ws = tokio_tungstenite::accept_hdr_async(stream, accept_cb)
        .await
        .context("Handshake error")?;

    info!("New WebSocket connection: {}", addr);

    let session = HanabiSession::new(user_id.unwrap());

    Ok(session::run(session, ws).await?)
}
