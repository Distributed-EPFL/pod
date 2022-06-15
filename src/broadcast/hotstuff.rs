use async_trait::async_trait;

use crate::broadcast::Broadcast;

use sha1::{Digest, Sha1};

use std::{error::Error, net::SocketAddr};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

pub struct HotStuffInterface {
    stream: TcpStream,
}

pub struct HotStuff {
    state: Mutex<HotStuffInterface>,
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

        println!("Writing stuff in stream..");

        self.stream.write(&magic.to_le_bytes()).await.unwrap();
        self.stream.write(&opcode.to_le_bytes()).await.unwrap();
        self.stream.write(&length.to_le_bytes()).await.unwrap();
        self.stream.write(&hash[0..4]).await.unwrap();
        self.stream.write(payload).await.unwrap();
    }

    pub async fn deliver(&mut self) -> Vec<u8> {
        let mut buf = vec![0; 13];

        self.stream.read_exact(&mut buf).await.unwrap();

        let length = u32::from_le_bytes(buf[5..9].try_into().unwrap());
        let mut msg = vec![0; length.try_into().unwrap()];

        self.stream.read_exact(&mut msg).await.unwrap();

        return msg;
    }
}

impl HotStuff {
    pub async fn connect(addr: &SocketAddr) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            state: Mutex::new(HotStuffInterface::connect(addr).await?),
        })
    }
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
