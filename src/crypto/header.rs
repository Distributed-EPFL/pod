use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i8)]
pub(crate) enum Header {
    Broadcast = 0,
    Reduction = 1,
    Witness = 2,
}
