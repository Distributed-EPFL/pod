use std::collections::BTreeMap;

use talk::crypto::{Identity, KeyCard};

pub struct Membership {
    pub(in crate::membership) servers: BTreeMap<Identity, KeyCard>,
}

impl Membership {
    pub(crate) fn from_servers<K>(servers: K) -> Self
    where
        K: IntoIterator<Item = KeyCard>,
    {
        let servers = servers
            .into_iter()
            .map(|keycard| (keycard.identity(), keycard))
            .collect::<BTreeMap<_, _>>();

        Membership { servers }
    }

    pub fn servers(&self) -> &BTreeMap<Identity, KeyCard> {
        &self.servers
    }
}
