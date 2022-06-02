use crate::{
    batch::{Payload, ReductionStatement},
    directory::Directory,
    passepartout::Passepartout,
};

use rand::prelude::*;

use std::{collections::HashMap, convert::TryInto, iter};

use talk::crypto::primitives::{multi::Signature as MultiSignature, sign::Signature};

use zebra::vector::Vector;

const NIBBLE: usize = 4; // TODO: Find a more appropriate name
const NULL_ID: u64 = u64::MAX;

pub struct Batch {
    payloads: Vector<[Payload; NIBBLE]>,
    reduction: Option<MultiSignature>,
    stragglers: HashMap<u64, Signature>,
}

impl Batch {
    pub fn random(directory: &Directory, passepartout: &Passepartout, size: usize) -> Self {
        let range = 0..(directory.len() as u64);
        let ids = range.into_iter().choose_multiple(&mut thread_rng(), size);

        let mut payloads = ids
            .iter()
            .copied()
            .map(|id| {
                let message: [u8; 8] = random();
                Payload { id, message }
            })
            .collect::<Vec<_>>();

        payloads.sort_unstable_by_key(|payload| payload.id);

        payloads.extend(
            iter::repeat(Payload {
                id: NULL_ID,
                message: [u8::MAX; 8],
            })
            .take(NIBBLE - 1),
        );

        let payloads = payloads
            .chunks_exact(NIBBLE)
            .map(|chunk| {
                let chunk: &[Payload; NIBBLE] = chunk.try_into().unwrap();
                chunk.clone()
            })
            .collect::<Vec<_>>();

        let payloads = Vector::new(payloads).unwrap();
        let root = payloads.root();

        let reductions = ids.into_iter().map(|id| {
            let keycard = directory.keycard(id).unwrap();
            let keychain = passepartout.keychain(keycard.identity());

            keychain.multisign(&ReductionStatement::new(root)).unwrap()
        });

        let reduction = Some(MultiSignature::aggregate(reductions).unwrap());
        let stragglers = HashMap::new();

        Batch {
            payloads,
            reduction,
            stragglers,
        }
    }
}
