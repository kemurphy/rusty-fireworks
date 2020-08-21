use std::fmt;

use async_trait::async_trait;
use log::info;
use tungstenite::protocol::Message;

use crate::session::MessageSink;
use crate::session::Session;

#[derive(Copy, Clone, fmt::Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct UserId(u64);

impl UserId {
    pub fn new(id: u64) -> Self {
        UserId(id)
    }
}

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "User:{}", self.0)
    }
}

#[derive(fmt::Debug)]
pub struct HanabiSession {
    id: UserId,
}

impl HanabiSession {
    pub fn new(id: UserId) -> HanabiSession {
        HanabiSession { id }
    }
}

#[async_trait]
impl Session for HanabiSession {
    type Key = UserId;
    type Message = Message;

    fn get_key(&self) -> Self::Key {
        self.id
    }

    async fn handle_open(&mut self, _sink: &mut MessageSink) {
        info!("Welcome! {:?}", self.id);
    }

    async fn handle_close(&mut self, _sink: &mut MessageSink) {
        info!("Later! {:?}", self.id);
    }

    async fn handle_message(&mut self, _sink: &mut MessageSink, msg: Message) {
        info!("Got message {:?}", msg);
    }
}
