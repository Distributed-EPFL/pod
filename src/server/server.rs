use crate::{
    batch::{Batch, BatchError, CompressedBatch},
    broadcast::Broadcast,
    directory::Directory,
    membership::{Certificate, Membership},
    server::WitnessStatement,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use talk::{
    crypto::{
        primitives::{hash::Hash, multi::Signature as MultiSignature},
        KeyChain,
    },
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
    #[doom(description("Witness invalid"))]
    WitnessInvalid,
}

impl Server {
    pub fn new<B>(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        broadcast: B,
        listener: SessionListener,
    ) -> Self
    where
        B: Broadcast,
    {
        let broadcast = Arc::new(broadcast);

        let batches = HashMap::new();
        let batches = Arc::new(Mutex::new(batches));

        let fuse = Fuse::new();

        fuse.spawn(async move {
            Server::listen(
                keychain, membership, directory, broadcast, batches, listener,
            )
            .await;
        });

        Server { _fuse: fuse }
    }

    async fn listen(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        broadcast: Arc<dyn Broadcast>,
        batches: Arc<Mutex<HashMap<Hash, Batch>>>,
        mut listener: SessionListener,
    ) {
        let membership = Arc::new(membership);
        let directory = Arc::new(directory);

        let semaphore = Semaphore::new(TASKS);
        let semaphore = Arc::new(semaphore);

        let fuse = Fuse::new();

        loop {
            let (_, session) = listener.accept().await;

            let keychain = keychain.clone();
            let membership = membership.clone();
            let directory = directory.clone();
            let broadcast = broadcast.clone();
            let batches = batches.clone();
            let semaphore = semaphore.clone();

            fuse.spawn(async move {
                let _ = Server::serve(
                    keychain, membership, directory, broadcast, batches, semaphore, session,
                )
                .await;
            });
        }
    }

    async fn serve(
        keychain: KeyChain,
        membership: Arc<Membership>,
        directory: Arc<Directory>,
        broadcast: Arc<dyn Broadcast>,
        batches: Arc<Mutex<HashMap<Hash, Batch>>>,
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

        let (root, witness_shard) = {
            let _permit = semaphore.acquire().await.unwrap();

            task::spawn_blocking(
                move || -> Result<(Hash, Option<MultiSignature>), Top<BatchError>> {
                    let batch = batch.decompress();
                    let root = batch.root();

                    if verify {
                        batch.verify(directory.as_ref())?;

                        {
                            let mut batches = batches.lock().unwrap();
                            batches.insert(root, batch);
                        }

                        let witness_shard =
                            keychain.multisign(&WitnessStatement::new(root)).unwrap();

                        Ok((root, Some(witness_shard)))
                    } else {
                        Ok((root, None))
                    }
                },
            )
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

        let witness = session
            .receive_plain::<Certificate>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        witness
            .verify_plurality(membership.as_ref(), &WitnessStatement::new(root))
            .pot(ServeError::WitnessInvalid, here!())?;

        let submission = bincode::serialize(&(root, witness)).unwrap();
        broadcast.order(submission.as_slice()).await;

        session.end();
        Ok(())
    }
}
