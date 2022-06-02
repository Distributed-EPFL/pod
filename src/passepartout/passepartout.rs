use crate::{directory::Directory, membership::Membership};

use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use serde::{Deserialize, Serialize};

use std::{collections::HashMap, fs, iter};

use talk::crypto::{Identity, KeyChain};

#[derive(Serialize, Deserialize)]
pub struct Passepartout {
    keychains: HashMap<Identity, KeyChain>,
}

const CHUNKS: usize = 64;

impl Passepartout {
    pub fn random(size: usize) -> Self {
        let keychains = (0..size)
            .into_par_iter()
            .map(|_| {
                let keychain = KeyChain::random();
                let identity = keychain.keycard().identity();
                (identity, keychain)
            })
            .collect::<HashMap<_, _>>();

        Passepartout { keychains }
    }

    pub fn load(path: &str) -> Self {
        let bytes = fs::read(path).unwrap();

        let chunks = bincode::deserialize::<Vec<Vec<u8>>>(bytes.as_slice()).unwrap();

        let keychains = chunks
            .par_iter()
            .map(|chunk| bincode::deserialize::<Vec<(Identity, KeyChain)>>(chunk).unwrap())
            .flatten()
            .collect::<HashMap<_, _>>();

        Passepartout { keychains }
    }

    pub fn keychain(&self, identity: Identity) -> KeyChain {
        self.keychains.get(&identity).unwrap().clone()
    }

    pub fn system(&self, servers: usize) -> (Membership, Directory) {
        let mut keycards = self.keychains.values().map(KeyChain::keycard);

        let servers = iter::repeat_with(|| keycards.next().unwrap()).take(servers);
        let membership = Membership::from_servers(servers);

        let clients = keycards
            .enumerate()
            .map(|(id, keycard)| (id as u64, keycard))
            .collect::<HashMap<_, _>>();

        let directory = Directory::from_keycards(clients);

        (membership, directory)
    }

    pub fn save(&self, path: &str) {
        let keychains = self
            .keychains
            .iter()
            .map(|(id, keychain)| (*id, keychain.clone()))
            .collect::<Vec<_>>();

        let chunk_size = (keychains.len() + CHUNKS - 1) / CHUNKS;

        let chunks = keychains
            .chunks(chunk_size)
            .map(|chunk| bincode::serialize(&chunk).unwrap())
            .collect::<Vec<_>>();

        fs::write(path, bincode::serialize(&chunks).unwrap().as_slice()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use talk::crypto::Statement;

    #[derive(Serialize)]
    struct TestHeader;

    #[derive(Serialize)]
    struct TestStatement(u64);

    impl Statement for TestStatement {
        type Header = TestHeader;
        const HEADER: Self::Header = TestHeader;
    }

    #[test]
    fn persist() {
        let original = Passepartout::random(1000);
        original.save("assets/passepartout.bin");

        let message = TestStatement(42);

        let signatures = original
            .keychains
            .iter()
            .map(|(identity, keychain)| (*identity, keychain.sign(&message).unwrap()))
            .collect::<HashMap<_, _>>();

        let loaded = Passepartout::load("assets/passepartout.bin");

        for (identity, signature) in signatures {
            signature
                .verify(&loaded.keychain(identity).keycard(), &message)
                .unwrap();
        }
    }
}
