use crate::crypto::Header;

use serde::Serialize;

use talk::crypto::{primitives::hash::Hash, Statement};

#[derive(Serialize)]
pub(crate) struct WitnessStatement {
    root: Hash,
}

impl WitnessStatement {
    pub fn new(root: Hash) -> Self {
        WitnessStatement { root }
    }
}

impl Statement for WitnessStatement {
    type Header = Header;
    const HEADER: Header = Header::Witness;
}
