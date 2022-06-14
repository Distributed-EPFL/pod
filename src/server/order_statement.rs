use crate::crypto::Header;

use serde::Serialize;

use talk::crypto::{primitives::hash::Hash, Statement};

#[derive(Serialize)]
pub(crate) struct OrderStatement {
    root: Hash,
}

impl OrderStatement {
    pub fn new(root: Hash) -> Self {
        OrderStatement { root }
    }
}

impl Statement for OrderStatement {
    type Header = Header;
    const HEADER: Header = Header::Order;
}
