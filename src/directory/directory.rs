use serde::{Deserialize, Serialize};

use std::{collections::HashMap, fs};

use talk::crypto::KeyCard;

#[derive(Serialize, Deserialize)]
pub struct Directory {
    keycards: HashMap<u64, KeyCard>,
}

impl Directory {
    pub fn new() -> Directory {
        Directory::from_keycards(HashMap::new())
    }

    pub fn load(path: &str) -> Directory {
        bincode::deserialize(fs::read(path).unwrap().as_slice()).unwrap()
    }

    pub(crate) fn from_keycards(keycards: HashMap<u64, KeyCard>) -> Directory {
        Directory { keycards }
    }

    pub fn keycard(&self, id: u64) -> Option<KeyCard> {
        self.keycards.get(&id).cloned()
    }

    pub fn save(&self, path: &str) {
        fs::write(path, bincode::serialize(&self).unwrap().as_slice()).unwrap();
    }
}
