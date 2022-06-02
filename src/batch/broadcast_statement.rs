use crate::{batch::Message, crypto::Header};

use serde::Serialize;

use talk::crypto::Statement;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BroadcastStatement(Message);

impl BroadcastStatement {
    pub fn new(message: Message) -> Self {
        BroadcastStatement(message)
    }
}

impl Statement for BroadcastStatement {
    type Header = Header;
    const HEADER: Header = Header::Broadcast;
}
