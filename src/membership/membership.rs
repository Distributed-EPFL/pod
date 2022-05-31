use talk::crypto::KeyCard;

pub struct Membership {
    servers: Vec<KeyCard>,
}

impl Membership {
    pub(crate) fn from_servers(servers: Vec<KeyCard>) -> Membership {
        Membership { servers }
    }
}
