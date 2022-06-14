use crate::{
    batch::{Batch, BatchError, CompressedBatch},
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
    pub fn new(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        listener: SessionListener,
    ) -> Self {
        let batches = HashMap::new();
        let batches = Arc::new(Mutex::new(batches));

        let fuse = Fuse::new();

        fuse.spawn(async move {
            Server::listen(keychain, membership, directory, batches, listener).await;
        });

        Server { _fuse: fuse }
    }

    async fn listen(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
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
            let batches = batches.clone();
            let semaphore = semaphore.clone();

            fuse.spawn(async move {
                let _ = Server::serve(keychain, membership, directory, batches, semaphore, session)
                    .await;
            });
        }
    }

    async fn serve(
        keychain: KeyChain,
        membership: Arc<Membership>,
        directory: Arc<Directory>,
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

        let (witness_statement, witness_shard) = {
            let _permit = semaphore.acquire().await.unwrap();

            task::spawn_blocking(
                move || -> Result<(WitnessStatement, Option<MultiSignature>), Top<BatchError>> {
                    let batch = batch.decompress();
                    let root = batch.root();

                    let witness_statement = WitnessStatement::new(root);

                    if verify {
                        batch.verify(directory.as_ref())?;

                        {
                            let mut batches = batches.lock().unwrap();
                            batches.insert(root, batch);
                        }

                        let witness_shard = keychain.multisign(&witness_statement).unwrap();

                        Ok((witness_statement, Some(witness_shard)))
                    } else {
                        Ok((witness_statement, None))
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
            .verify_plurality(membership.as_ref(), &witness_statement)
            .pot(ServeError::WitnessInvalid, here!())?;

        session.end();
        Ok(())
    }
}
