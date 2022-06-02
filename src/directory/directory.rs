use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use serde::{Deserialize, Serialize};

use std::fs;

use talk::crypto::KeyCard;

#[derive(Serialize, Deserialize)]
pub struct Directory {
    keycards: Vec<Option<KeyCard>>,
}

const CHUNKS: usize = 64;

impl Directory {
    pub fn new() -> Directory {
        Directory::from_keycards(Vec::new())
    }

    pub(crate) fn from_keycards(keycards: Vec<Option<KeyCard>>) -> Directory {
        Directory { keycards }
    }

    pub fn load(path: &str) -> Directory {
        let bytes = fs::read(path).unwrap();

        let chunks = bincode::deserialize::<Vec<Vec<u8>>>(bytes.as_slice()).unwrap();

        let keycards = chunks
            .par_iter()
            .map(|chunk| bincode::deserialize::<Vec<Option<KeyCard>>>(chunk).unwrap())
            .flatten()
            .collect::<Vec<_>>();

        Directory { keycards }
    }

    pub fn keycard(&self, id: u64) -> Option<KeyCard> {
        self.keycards.get(id as usize).cloned().flatten()
    }

    pub fn capacity(&self) -> usize {
        self.keycards.len()
    }

    pub fn save(&self, path: &str) {
        let chunk_size = (self.keycards.len() + CHUNKS - 1) / CHUNKS;

        let chunks = self
            .keycards
            .chunks(chunk_size)
            .map(|chunk| bincode::serialize(&chunk).unwrap())
            .collect::<Vec<_>>();

        fs::write(path, bincode::serialize(&chunks).unwrap().as_slice()).unwrap();
    }
}
