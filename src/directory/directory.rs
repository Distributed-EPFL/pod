use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use serde::{Deserialize, Serialize};

use std::{collections::HashMap, fs};

use talk::crypto::KeyCard;

#[derive(Serialize, Deserialize)]
pub struct Directory {
    keycards: HashMap<u64, KeyCard>,
}

const CHUNKS: usize = 64;

impl Directory {
    pub fn new() -> Directory {
        Directory::from_keycards(HashMap::new())
    }

    pub fn load(path: &str) -> Directory {
        let bytes = fs::read(path).unwrap();

        let chunks = bincode::deserialize::<Vec<Vec<u8>>>(bytes.as_slice()).unwrap();

        let keycards = chunks
            .par_iter()
            .map(|chunk| bincode::deserialize::<Vec<(u64, KeyCard)>>(chunk).unwrap())
            .flatten()
            .collect::<HashMap<_, _>>();

        Directory { keycards }
    }

    pub(crate) fn from_keycards(keycards: HashMap<u64, KeyCard>) -> Directory {
        Directory { keycards }
    }

    pub fn keycard(&self, id: u64) -> Option<KeyCard> {
        self.keycards.get(&id).cloned()
    }

    pub fn len(&self) -> usize {
        self.keycards.len()
    }

    pub fn save(&self, path: &str) {
        let keycards = self
            .keycards
            .iter()
            .map(|(id, keycard)| (*id, keycard.clone()))
            .collect::<Vec<_>>();

        let chunk_size = (keycards.len() + CHUNKS - 1) / CHUNKS;

        let chunks = keycards
            .chunks(chunk_size)
            .map(|chunk| bincode::serialize(&chunk).unwrap())
            .collect::<Vec<_>>();

        fs::write(path, bincode::serialize(&chunks).unwrap().as_slice()).unwrap();
    }
}
