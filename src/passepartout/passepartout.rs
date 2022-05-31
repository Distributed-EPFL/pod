use serde::{Deserialize, Serialize};

use std::{collections::HashMap, fs, iter};

use talk::crypto::{Identity, KeyChain};

use crate::{directory::Directory, membership::Membership};

#[derive(Serialize, Deserialize)]
pub struct Passepartout {
    keychains: HashMap<Identity, KeyChain>,
}

impl Passepartout {
    pub fn random(size: usize) -> Self {
        let keychains = iter::repeat_with(KeyChain::random)
            .take(size)
            .map(|keychain| (keychain.keycard().identity(), keychain))
            .collect::<HashMap<_, _>>();

        Passepartout { keychains }
    }

    pub fn load(path: &str) -> Self {
        bincode::deserialize(fs::read(path).unwrap().as_slice()).unwrap()
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
        fs::write(path, bincode::serialize(&self).unwrap().as_slice()).unwrap();
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
