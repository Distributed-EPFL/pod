use crate::{
    batch::{BroadcastStatement, CompressedBatch, Message, Payload, ReductionStatement},
    directory::Directory,
    passepartout::Passepartout,
};

use doomstack::{here, Doom, ResultExt, Top};

use rand::prelude::*;

use std::{collections::BTreeMap, convert::TryInto, iter};

use talk::crypto::primitives::{hash::Hash, multi::Signature as MultiSignature, sign::Signature};

use varcram::VarCram;

use zebra::vector::Vector;

pub(in crate::batch) const NIBBLE: usize = 16; // TODO: Find a more appropriate name
const NULL_ID: u64 = u64::MAX;

pub struct Batch {
    payloads: Vector<[Payload; NIBBLE]>,
    reduction: Option<MultiSignature>,
    stragglers: BTreeMap<u64, Signature>,
}

#[derive(Doom)]
pub enum BatchError {
    #[doom(description("Batch invalid"))]
    BatchInvalid,
}

impl Batch {
    pub fn random(directory: &Directory, passepartout: &Passepartout, size: usize) -> Self {
        let range = 0..(directory.capacity() as u64);
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

        let payloads = Batch::vectorize_payloads(payloads);
        let root = payloads.root();

        let reductions = ids.into_iter().map(|id| {
            let keycard = directory.keycard(id).unwrap();
            let keychain = passepartout.keychain(keycard.identity());

            keychain.multisign(&ReductionStatement::new(root)).unwrap()
        });

        let reduction = Some(MultiSignature::aggregate(reductions).unwrap());
        let stragglers = BTreeMap::new();

        Batch {
            payloads,
            reduction,
            stragglers,
        }
    }

    pub(in crate::batch) fn from_compressed_batch(
        ids: VarCram,
        messages: Vec<Message>,
        reduction: Option<MultiSignature>,
        stragglers: BTreeMap<u64, Signature>,
    ) -> Self {
        let ids = ids.uncram().unwrap(); // TODO: Handle `None` case (`CompressedBatch` might be malformed)

        let payloads = ids
            .into_iter()
            .zip(messages.into_iter())
            .map(|(id, message)| Payload { id, message })
            .collect::<Vec<_>>();

        let payloads = Batch::vectorize_payloads(payloads);

        Batch {
            payloads,
            reduction,
            stragglers,
        }
    }

    fn vectorize_payloads(mut payloads: Vec<Payload>) -> Vector<[Payload; NIBBLE]> {
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

        Vector::new(payloads).unwrap()
    }

    pub fn root(&self) -> Hash {
        self.payloads.root()
    }

    pub fn payloads(&self) -> impl Iterator<Item = &Payload> {
        self.payloads
            .items()
            .iter()
            .flatten()
            .filter(|item| item.id != NULL_ID)
    }

    pub fn compress(self) -> CompressedBatch {
        CompressedBatch::from_batch(self.payloads, self.reduction, self.stragglers)
    }

    pub fn verify(&self, directory: &Directory) -> Result<(), Top<BatchError>> {
        let mut payloads = self.payloads();
        let mut last = payloads.next().unwrap().id; // TODO: Think of what you've done

        for next in payloads {
            if next.id <= last {
                return BatchError::BatchInvalid.fail();
            }

            last = next.id;
        }

        let mut stragglers = self.stragglers.iter().peekable();
        let mut reducers = Vec::with_capacity(self.payloads.len());

        for payload in self.payloads() {
            if let Some((id, signature)) = stragglers.peek().cloned() {
                if payload.id == *id {
                    signature
                        .verify(
                            &directory.keycard(*id).unwrap(),
                            &BroadcastStatement::new(payload.message),
                        )
                        .pot(BatchError::BatchInvalid, here!())?;

                    stragglers.next();
                }
            } else {
                reducers.push(directory.keycard(payload.id).unwrap());
            }
        }

        if reducers.len() > 0 {
            if let Some(reduction) = self.reduction {
                reduction
                    .verify(
                        reducers.into_iter(),
                        &ReductionStatement::new(self.payloads.root()),
                    )
                    .pot(BatchError::BatchInvalid, here!())?;
            } else {
                return BatchError::BatchInvalid.fail();
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_size() {
        let passepartout = Passepartout::random(100);
        let (_membership, directory) = passepartout.system(1);

        let batch = Batch::random(&directory, &passepartout, 42);
        assert_eq!(batch.payloads.len(), (42 + NIBBLE - 1) / NIBBLE);
    }

    #[test]
    fn verify() {
        let passepartout = Passepartout::random(100);
        let (_membership, directory) = passepartout.system(1);

        let batch = Batch::random(&directory, &passepartout, 42);
        batch.verify(&directory).unwrap();
    }
}
