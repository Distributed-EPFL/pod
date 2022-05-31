use serde::{Deserialize, Serialize};

use std::{collections::HashMap, fs, iter};

use talk::crypto::{Identity, KeyChain};

#[derive(Serialize, Deserialize)]
pub struct Passepartout {
    keychains: HashMap<Identity, KeyChain>,
}

impl Passepartout {
    pub fn random(size: usize) -> Passepartout {
        let keychains = iter::repeat_with(KeyChain::random)
            .take(size)
            .map(|keychain| (keychain.keycard().identity(), keychain))
            .collect::<HashMap<_, _>>();

        Passepartout { keychains }
    }

    pub fn load(path: &str) -> Passepartout {
        bincode::deserialize::<Passepartout>(fs::read(path).unwrap().as_slice()).unwrap()
    }

    pub fn save(&self, path: &str) {
        fs::write(path, bincode::serialize(&self).unwrap().as_slice()).unwrap();
    }
}
