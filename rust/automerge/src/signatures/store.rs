use std::collections::BTreeMap;

use crate::ChangeHash;

use super::{Signature, SignatureRef};

/// The signatures a document retains, keyed by the change they target.
#[derive(Clone, Debug, Default)]
pub(crate) struct SignatureStore {
    /// Signatures per target in arrival order, without exact duplicates.
    by_hash: BTreeMap<ChangeHash, Vec<Vec<u8>>>,
    /// Every signature in arrival order, as a target and an index into its
    /// entry above, so an incremental save can emit what arrived since a
    /// cursor without repeating anything.
    log: Vec<(ChangeHash, usize)>,
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
