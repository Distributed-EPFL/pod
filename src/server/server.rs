use crate::{
    batch::{BatchError, CompressedBatch},
    directory::Directory,
    server::WitnessStatement,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::sync::Arc;

use talk::{
    crypto::{primitives::multi::Signature as MultiSignature, KeyChain},
    net::{Listener, SecureConnection},
};

use tokio::task;

pub struct Server {}

#[derive(Doom)]
enum ServeError {
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Batch invalid"))]
    BatchInvalid,
}

impl Server {
    pub fn new<L>(keychain: KeyChain, directory: Directory, listener: L) -> Self
    where
        L: Listener,
    {
        tokio::spawn(async move {
            Server::listen(keychain, directory, listener).await;
        });

        todo!()
    }

    async fn listen<L>(keychain: KeyChain, directory: Directory, mut listener: L)
    where
        L: Listener,
    {
        let directory = Arc::new(directory);

        loop {
            let (_, connection) = listener.accept().await.unwrap();

            let keychain = keychain.clone();
            let directory = directory.clone();

            tokio::spawn(async move {
                let _ = Server::serve(keychain, directory, connection).await;
            });
        }
    }

    async fn serve(
        keychain: KeyChain,
        directory: Arc<Directory>,
        mut connection: SecureConnection,
    ) -> Result<(), Top<ServeError>> {
        let batch = connection
            .receive_plain::<CompressedBatch>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        let verify = connection
            .receive_plain::<bool>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        // TODO: Add semaphore here

        let witness_shard =
            task::spawn_blocking(move || -> Result<Option<MultiSignature>, Top<BatchError>> {
                let batch = batch.decompress();

                if verify {
                    batch.verify(directory.as_ref())?;

                    let witness_shard = keychain
                        .multisign(&WitnessStatement::new(batch.root()))
                        .unwrap();

                    Ok(Some(witness_shard))
                } else {
                    Ok(None)
                }
            })
            .await
            .unwrap()
            .pot(ServeError::BatchInvalid, here!())?;

        if let Some(witness_shard) = witness_shard {
            connection
                .send_plain(&witness_shard)
                .await
                .pot(ServeError::ConnectionError, here!())?;
        }

        Ok(())
    }
}
