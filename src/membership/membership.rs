use talk::crypto::KeyCard;

pub struct Membership {
    pub(in crate::membership) servers: Vec<KeyCard>,
}

impl Membership {
    pub(crate) fn from_servers(servers: Vec<KeyCard>) -> Self {
        Membership { servers }
    }
}
