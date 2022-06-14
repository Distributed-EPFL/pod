use async_trait::async_trait;

#[async_trait]
pub trait Broadcast {
    async fn order(payload: &[u8]);
    async fn deliver() -> Vec<u8>;
}
