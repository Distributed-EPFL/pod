use async_trait::async_trait;

use crate::broadcast::Broadcast;

use sha1::{Digest, Sha1};

use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task;
use tokio::time;

pub struct HotStuffInterface {
    stream: TcpStream,
}

impl HotStuffInterface {
    async fn connect(addr: &SocketAddr) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            stream: TcpStream::connect(addr).await?,
        })
    }

    pub async fn order(&mut self, payload: &[u8]) {
        let magic: u32 = 0;
        let opcode: u8 = 100;
        let length: u32 = payload.len().try_into().unwrap();
        let mut hasher = Sha1::new();

        hasher.update(payload);

        let hash = hasher.finalize();

        self.stream.write(&magic.to_le_bytes()).await;
        self.stream.write(&opcode.to_le_bytes()).await;
        self.stream.write(&length.to_le_bytes()).await;
        self.stream.write(&hash[0..4]).await;
        self.stream.write(payload).await;
    }

    pub async fn deliver(&mut self) -> Vec<u8> {
        let mut buf = vec![0; 13];

        self.stream.read_exact(&mut buf).await;

        let length = u32::from_le_bytes(buf[5..9].try_into().unwrap());
        let mut msg = vec![0; length.try_into().unwrap()];

        self.stream.read_exact(&mut msg).await;

        return msg;
    }
}

pub struct HotStuff {
    state: Mutex<HotStuffInterface>,
}

#[async_trait]
impl Broadcast for HotStuff {
    async fn order(&self, payload: &[u8]) {
        let mut iface = self.state.lock().await;
        iface.order(payload).await;
    }

    async fn deliver(&self) -> Vec<u8> {
        let mut iface = self.state.lock().await;
        iface.deliver().await
    }
}

impl HotStuff {
    async fn connect(addr: &SocketAddr) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            state: Mutex::new(HotStuffInterface::connect(addr).await?),
        })
    }
}
