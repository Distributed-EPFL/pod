use crate::crypto::Header;

use serde::Serialize;

use talk::crypto::{primitives::hash::Hash, Statement};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReductionStatement(Hash);

impl ReductionStatement {
    pub fn new(root: Hash) -> Self {
        ReductionStatement(root)
    }
}

impl Statement for ReductionStatement {
    type Header = Header;
    const HEADER: Header = Header::Reduction;
}
