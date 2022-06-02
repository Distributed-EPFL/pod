use crate::batch::Message;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Payload {
    pub id: u64,
    pub message: Message,
}
