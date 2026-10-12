use std::collections::{BTreeMap, HashSet};

use ed25519_dalek::VerifyingKey;

use crate::ChangeHash;

use super::{verifies, Signature, SignatureRef};

/// The signatures a document retains, keyed by the change they target.
#[derive(Clone, Debug, Default)]
pub(crate) struct SignatureStore {
    /// Signatures per target in arrival order, without exact duplicates.
    by_hash: BTreeMap<ChangeHash, Vec<Vec<u8>>>,
    /// Every signature in arrival order, as a target and an index into its
    /// entry above, so an incremental save can emit what arrived since a
    /// cursor without repeating anything.
    log: Vec<(ChangeHash, usize)>,
    /// Targets known to carry a signature which verifies. A positive answer is
    /// permanent because the key a target is checked against is fixed by the
    /// change itself, so this is never serialized and never invalidated.
    verified: HashSet<ChangeHash>,
}

impl SignatureStore {
    /// Retain a signature. Returns false if an identical signature was already held.
    pub(crate) fn insert(&mut self, hash: ChangeHash, bytes: Vec<u8>) -> bool {
        let signatures = self.by_hash.entry(hash).or_default();
        if signatures.contains(&bytes) {
            return false;
        }
        self.log.push((hash, signatures.len()));
        signatures.push(bytes);
        true
    }

    pub(crate) fn extend(&mut self, signatures: impl IntoIterator<Item = Signature>) {
        for signature in signatures {
            self.insert(signature.change_hash, signature.bytes);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.log.is_empty()
    }

    /// The number of signatures held, usable as a cursor for [`Self::since`].
    pub(crate) fn len(&self) -> usize {
        self.log.len()
    }

    pub(crate) fn signatures_for(&self, hash: &ChangeHash) -> impl Iterator<Item = &[u8]> {
        self.by_hash
            .get(hash)
            .into_iter()
            .flatten()
            .map(Vec::as_slice)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = SignatureRef<'_>> {
        self.by_hash.iter().flat_map(|(hash, signatures)| {
            signatures.iter().map(move |bytes| SignatureRef {
                change_hash: *hash,
                bytes,
            })
        })
    }

    /// Whether `hash` carries a signature which verifies under `key`, remembering a
    /// positive answer.
    ///
    /// The key a target is checked against is fixed by the change itself, so a
    /// signature which verifies once verifies forever. Remembering that is what keeps
    /// repeated questions — every enumeration of the fragment cover asks one per
    /// actor maximum — from re-running the signature check each time.
    pub(crate) fn verify(&mut self, hash: ChangeHash, key: &VerifyingKey) -> bool {
        if self.verified.contains(&hash) {
            return true;
        }
        let valid = self
            .signatures_for(&hash)
            .any(|signature| verifies(signature, &hash, key));
        if valid {
            self.verified.insert(hash);
        }
        valid
    }

    /// [`Self::verify`] for a `&self` caller, which cannot remember the answer and
    /// so pays for the check whenever it was not already remembered.
    pub(crate) fn is_valid(&self, hash: &ChangeHash, key: &VerifyingKey) -> bool {
        self.verified.contains(hash)
            || self
                .signatures_for(hash)
                .any(|signature| verifies(signature, hash, key))
    }

    /// The targets which carry at least one signature.
    pub(crate) fn targets(&self) -> impl Iterator<Item = ChangeHash> + '_ {
        self.by_hash.keys().copied()
    }

    /// Whether `hash` carries any signature at all, verified or not.
    pub(crate) fn has_any(&self, hash: &ChangeHash) -> bool {
        self.by_hash
            .get(hash)
            .is_some_and(|signatures| !signatures.is_empty())
    }

    /// Signatures which arrived after the first `cursor` signatures.
    pub(crate) fn since(&self, cursor: usize) -> impl Iterator<Item = SignatureRef<'_>> {
        self.log[cursor.min(self.log.len())..]
            .iter()
            .map(|(hash, index)| SignatureRef {
                change_hash: *hash,
                bytes: &self.by_hash[hash][*index],
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signatures_dedupe_and_log_arrival_order() {
        let a = ChangeHash([1; 32]);
        let b = ChangeHash([2; 32]);
        let mut store = SignatureStore::default();
        assert!(store.insert(b, vec![0, 9]));
        assert!(store.insert(a, vec![1, 8]));
        assert!(!store.insert(b, vec![0, 9]));
        assert!(store.insert(a, vec![0, 7]));

        assert_eq!(store.len(), 3);
        assert_eq!(
            store.signatures_for(&a).collect::<Vec<_>>(),
            [&[1, 8][..], &[0, 7]]
        );
        assert_eq!(
            store.iter().collect::<Vec<_>>(),
            [
                SignatureRef {
                    change_hash: a,
                    bytes: &[1, 8]
                },
                SignatureRef {
                    change_hash: a,
                    bytes: &[0, 7]
                },
                SignatureRef {
                    change_hash: b,
                    bytes: &[0, 9]
                },
            ]
        );
        assert_eq!(
            store.since(1).collect::<Vec<_>>(),
            [
                SignatureRef {
                    change_hash: a,
                    bytes: &[1, 8]
                },
                SignatureRef {
                    change_hash: a,
                    bytes: &[0, 7]
                },
            ]
        );
        assert!(store.since(3).next().is_none());

        let mut copy = SignatureStore::default();
        copy.extend(store.iter().map(SignatureRef::to_owned));
        assert_eq!(
            copy.iter().collect::<Vec<_>>(),
            store.iter().collect::<Vec<_>>()
        );
    }
}
