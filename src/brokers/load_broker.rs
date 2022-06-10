use crate::{
    batch::CompressedBatch,
    membership::{Certificate, Membership},
    server::WitnessStatement,
};

use doomstack::{here, Doom, ResultExt, Top};

use futures::stream::{FuturesUnordered, StreamExt};

use rand::prelude::*;

use std::sync::Arc;

use talk::{
    crypto::{
        primitives::{hash::Hash, multi::Signature as MultiSignature},
        Identity, KeyCard,
    },
    net::SessionConnector,
    sync::fuse::Fuse,
};

use tokio::sync::oneshot::{self, Sender as OneshotSender};

pub struct LoadBroker {
    membership: Arc<Membership>,
    connector: Arc<SessionConnector>,
    batches: Arc<Vec<(Hash, CompressedBatch)>>,
    fuse: Fuse,
}

#[derive(Doom)]
enum TrySubmitError {
    #[doom(description("Failed to connect."))]
    ConnectFailed,
    #[doom(description("Connection error"))]
    ConnectionError,
}

impl LoadBroker {
    pub fn new(
        membership: Membership,
        connector: SessionConnector,
        batches: Vec<(Hash, CompressedBatch)>,
    ) -> Self {
        let membership = Arc::new(membership);
        let connector = Arc::new(connector);
        let batches = Arc::new(batches);
        let fuse = Fuse::new();

        LoadBroker {
            membership,
            connector,
            batches,
            fuse,
        }
    }

    pub async fn broadcast(&self, index: usize) {
        let mut verifiers = self
            .membership
            .servers()
            .keys()
            .copied()
            .choose_multiple(&mut thread_rng(), self.membership.plurality());

        verifiers.sort();

        let mut witness_shard_receivers = Vec::new();

        for (identity, keycard) in self.membership.servers() {
            let witness_shard_sender = verifiers.binary_search(identity).ok().map(|_| {
                let (witness_shard_sender, witness_shard_receiver) = oneshot::channel();

                witness_shard_receivers.push(witness_shard_receiver);
                witness_shard_sender
            });

            let connector = self.connector.clone();
            let batches = self.batches.clone();
            let keycard = keycard.clone();

            self.fuse.spawn(async move {
                LoadBroker::submit(connector, batches, index, keycard, witness_shard_sender).await;
            });
        }

        let witness_shard_receivers = witness_shard_receivers
            .into_iter()
            .collect::<FuturesUnordered<_>>();

        let witness_shards = witness_shard_receivers
            .map(|shard| shard.unwrap())
            .collect::<Vec<_>>()
            .await;

        let _witness = Certificate::aggregate(self.membership.as_ref(), witness_shards);

        // TODO: Total-Order broadcast `root` and `witness`
    }

    async fn submit(
        connector: Arc<SessionConnector>,
        batches: Arc<Vec<(Hash, CompressedBatch)>>,
        index: usize,
        server: KeyCard,
        mut witness_shard_sender: Option<OneshotSender<(Identity, MultiSignature)>>,
    ) {
        while LoadBroker::try_submit(
            connector.as_ref(),
            batches.as_ref(),
            index,
            &server,
            &mut witness_shard_sender,
        )
        .await
        .is_err()
        {}
    }

    async fn try_submit(
        connector: &SessionConnector,
        batches: &Vec<(Hash, CompressedBatch)>,
        index: usize,
        server: &KeyCard,
        witness_shard_sender: &mut Option<OneshotSender<(Identity, MultiSignature)>>,
    ) -> Result<(), Top<TrySubmitError>> {
        let mut session = connector
            .connect(server.identity())
            .await
            .pot(TrySubmitError::ConnectFailed, here!())?;

        let (root, batch) = batches.get(index).unwrap();
        let root = *root;

        session
            .send_plain(batch)
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        if witness_shard_sender.is_some() {
            session
                .send_plain(&true)
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            let witness_shard = session
                .receive_plain::<MultiSignature>()
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            witness_shard
                .verify([server], &WitnessStatement::new(root))
                .unwrap();

            let _ = witness_shard_sender
                .take()
                .unwrap()
                .send((server.identity(), witness_shard));
        } else {
            session
                .send_plain(&false)
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;
        }

        session.end();
        Ok(())
    }
}
