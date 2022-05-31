use serde::{Deserialize, Serialize};

use std::{collections::BTreeMap, fs};

use talk::crypto::{Identity, KeyCard};

#[derive(Serialize, Deserialize)]
pub struct Membership {
    pub(in crate::membership) servers: BTreeMap<Identity, KeyCard>,
}

impl Membership {
    pub fn from_servers<K>(servers: K) -> Self
    where
        K: IntoIterator<Item = KeyCard>,
    {
        let servers = servers
            .into_iter()
            .map(|keycard| (keycard.identity(), keycard))
            .collect::<BTreeMap<_, _>>();

        Membership { servers }
    }

    pub fn load(path: &str) -> Membership {
        let servers = bincode::deserialize::<Vec<_>>(fs::read(path).unwrap().as_slice()).unwrap();
        Membership::from_servers(servers)
    }

    pub fn save(&self, path: &str) {
        let servers = self.servers.values().cloned().collect::<Vec<_>>();
        fs::write(path, bincode::serialize(&servers).unwrap().as_slice()).unwrap();
    }

    pub fn servers(&self) -> &BTreeMap<Identity, KeyCard> {
        &self.servers
    }

    pub fn plurality(&self) -> usize {
        (self.servers.len() - 1) / 3 + 1
    }

    pub fn quorum(&self) -> usize {
        self.servers.len() - self.plurality() + 1
    }
}
