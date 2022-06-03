use crate::batch::{batch::NIBBLE, Message, Payload};

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
    ) {
    }
}
