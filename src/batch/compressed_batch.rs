use crate::batch::{batch::NIBBLE, Batch, Message, Payload};

use std::collections::BTreeMap;

use talk::crypto::primitives::{multi::Signature as MultiSignature, sign::Signature};

use varcram::VarCram;

use zebra::vector::Vector;

pub struct CompressedBatch {
    ids: VarCram,
    messages: Vec<Message>,
    reduction: Option<MultiSignature>,
    stragglers: BTreeMap<u64, Signature>,
}

impl CompressedBatch {
    pub(in crate::batch) fn from_batch(
        payloads: Vector<[Payload; NIBBLE]>,
        reduction: Option<MultiSignature>,
        stragglers: BTreeMap<u64, Signature>,
    ) -> Self {
        let mut ids = Vec::with_capacity(payloads.len());
        let mut messages = Vec::with_capacity(payloads.len());

        for payload in payloads.items().iter().flatten() {
            ids.push(payload.id);
            messages.push(payload.message.clone());
        }

        let ids = VarCram::cram(ids.as_slice());

        CompressedBatch {
            ids,
            messages,
            reduction,
            stragglers,
        }
    }

    pub fn decompress(self) -> Batch {
        Batch::from_compressed_batch(self.ids, self.messages, self.reduction, self.stragglers)
    }
}
