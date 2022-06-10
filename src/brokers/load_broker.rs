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
    net::Connector,
};

use tokio::sync::oneshot::{self, Sender as OneshotSender};

pub struct LoadBroker {
    membership: Arc<Membership>,
    connector: Arc<dyn Connector>,
    batches: Arc<Vec<(Hash, CompressedBatch)>>,
}

#[derive(Doom)]
enum TrySubmitError {
    #[doom(description("Failed to connect."))]
    ConnectFailed,
    #[doom(description("Connection error"))]
    ConnectionError,
}

impl LoadBroker {
    pub fn new<C>(
        membership: Membership,
        connector: C,
        batches: Vec<(Hash, CompressedBatch)>,
    ) -> Self
    where
        C: Connector,
    {
        let membership = Arc::new(membership);
        let connector = Arc::new(connector);
        let batches = Arc::new(batches);

        LoadBroker {
            membership,
            connector,
            batches,
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

        let mut witness_receivers = Vec::new();

        for (identity, keycard) in self.membership.servers() {
            let witness_sender = verifiers.binary_search(identity).ok().map(|_| {
                let (witness_sender, witness_receiver) = oneshot::channel();

                witness_receivers.push(witness_receiver);
                witness_sender
            });

            let connector = self.connector.clone();
            let batches = self.batches.clone();
            let keycard = keycard.clone();

            tokio::spawn(async move {
                LoadBroker::submit(connector, batches, index, keycard, witness_sender).await;
            });
        }

        let witness_receivers = witness_receivers
            .into_iter()
            .collect::<FuturesUnordered<_>>();

        let witnesses = witness_receivers
            .map(|witness| witness.unwrap())
            .collect::<Vec<_>>()
            .await;

        let witness = Certificate::aggregate(self.membership.as_ref(), witnesses);
    }

    async fn submit(
        connector: Arc<dyn Connector>,
        batches: Arc<Vec<(Hash, CompressedBatch)>>,
        index: usize,
        server: KeyCard,
        mut witness_sender: Option<OneshotSender<(Identity, MultiSignature)>>,
    ) {
        while LoadBroker::try_submit(
            connector.as_ref(),
            batches.as_ref(),
            index,
            &server,
            &mut witness_sender,
        )
        .await
        .is_err()
        {}
    }

    async fn try_submit(
        connector: &dyn Connector,
        batches: &Vec<(Hash, CompressedBatch)>,
        index: usize,
        server: &KeyCard,
        witness_sender: &mut Option<OneshotSender<(Identity, MultiSignature)>>,
    ) -> Result<(), Top<TrySubmitError>> {
        let mut connection = connector
            .connect(server.identity())
            .await
            .pot(TrySubmitError::ConnectFailed, here!())?;

        let (root, batch) = batches.get(index).unwrap();
        let root = *root;

        connection
            .send_plain(batch)
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        if witness_sender.is_some() {
            connection
                .send_plain(&true)
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            let witness = connection
                .receive_plain::<MultiSignature>()
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            witness
                .verify([server], &WitnessStatement::new(root))
                .unwrap();

            let _ = witness_sender
                .take()
                .unwrap()
                .send((server.identity(), witness));
        } else {
            connection
                .send_plain(&false)
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;
        }

        todo!()
    }
}
