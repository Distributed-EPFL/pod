use crate::{
    batch::{BatchError, CompressedBatch},
    directory::Directory,
    server::WitnessStatement,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::sync::Arc;

use talk::{
    crypto::{primitives::multi::Signature as MultiSignature, KeyChain},
    net::{Session, SessionListener},
    sync::fuse::Fuse,
};

use tokio::{sync::Semaphore, task};

const TASKS: usize = 128;

pub struct Server {
    _fuse: Fuse,
}

#[derive(Doom)]
enum ServeError {
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Batch invalid"))]
    BatchInvalid,
}

impl Server {
    pub fn new(keychain: KeyChain, directory: Directory, listener: SessionListener) -> Self {
        let fuse = Fuse::new();

        fuse.spawn(async move {
            Server::listen(keychain, directory, listener).await;
        });

        Server { _fuse: fuse }
    }

    async fn listen(keychain: KeyChain, directory: Directory, mut listener: SessionListener) {
        let directory = Arc::new(directory);

        let semaphore = Semaphore::new(TASKS);
        let semaphore = Arc::new(semaphore);

        let fuse = Fuse::new();

        loop {
            let (_, session) = listener.accept().await;

            let keychain = keychain.clone();
            let directory = directory.clone();
            let semaphore = semaphore.clone();

            fuse.spawn(async move {
                let _ = Server::serve(keychain, directory, semaphore, session).await;
            });
        }
    }

    async fn serve(
        keychain: KeyChain,
        directory: Arc<Directory>,
        semaphore: Arc<Semaphore>,
        mut session: Session,
    ) -> Result<(), Top<ServeError>> {
        let batch = session
            .receive_plain::<CompressedBatch>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        let verify = session
            .receive_plain::<bool>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        let witness_shard = {
            let _permit = semaphore.acquire().await.unwrap();

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
            .pot(ServeError::BatchInvalid, here!())?
        };

        if let Some(witness_shard) = witness_shard {
            session
                .send_plain(&witness_shard)
                .await
                .pot(ServeError::ConnectionError, here!())?;
        }

        session.end();
        Ok(())
    }
}
