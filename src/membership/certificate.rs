use bit_vec::BitVec;

use crate::membership::Membership;

use serde::{Deserialize, Serialize};

use talk::crypto::{primitives::multi::Signature as MultiSignature, Identity};

#[derive(Serialize, Deserialize)]
pub struct Certificate {
    signers: BitVec,
    signature: MultiSignature,
}

impl Certificate {
    pub fn aggregate<C>(membership: &Membership, components: C) -> Self
    where
        C: IntoIterator<Item = (Identity, MultiSignature)>,
    {
        let mut components = components.into_iter().collect::<Vec<_>>();
        components.sort_by_key(|component| component.0);

        let mut signers = BitVec::from_elem(membership.servers().len(), false);
        let mut signer_identities = components.iter().map(|component| component.0).peekable();

        // Both `view.members()` and `signer_identities` are sorted. In order to determine which
        // elements of `signers` to set to `true`, loop through all elements of `view.members()`:
        // for every `member`, if `member` is the next element of `signer_identities`, then set the
        // corresponding element of `signers` to `true`, and move `signer_identities` on.
        for (index, member) in membership.servers().keys().enumerate() {
            if signer_identities.peek() == Some(&member) {
                signers.set(index, true);
                signer_identities.next().unwrap();
            }
        }

        if signer_identities.next().is_some() {
            panic!("Called `Certificate::aggregate` with a foreign component");
        }

        let signatures = components.into_iter().map(|component| component.1);

        let signature = MultiSignature::aggregate(signatures)
            .expect("Called `Certificate::aggregate` with an incorrect multi-signature");

        Certificate { signers, signature }
    }

    pub fn power(&self) -> usize {
        self.signers.iter().filter(|mask| *mask).count()
    }
}
