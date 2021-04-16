use std::convert::TryFrom;
use std::convert::TryInto;
use std::{borrow::Cow, collections::HashSet};

use automerge_protocol::{ChangeHash, Patch};

use crate::{
    encoding::{Decoder, Encodable},
    sync::BloomFilter,
    AutomergeError, Backend, Change,
};

const MESSAGE_TYPE_SYNC: u8 = 0x42; // first byte of a sync message, for identification

#[derive(Debug, Default)]
pub struct PeerState {
    shared_heads: Vec<ChangeHash>,
    last_sent_heads: Option<Vec<ChangeHash>>,
    their_heads: Option<Vec<ChangeHash>>,
    their_need: Option<Vec<ChangeHash>>,
    our_need: Vec<ChangeHash>,
    their_have: Option<Vec<SyncHave>>,
    unapplied_changes: Vec<Change>,
}

#[derive(Debug)]
pub struct SyncMessage {
    heads: Vec<ChangeHash>,
    need: Vec<ChangeHash>,
    have: Vec<SyncHave>,
    changes: Vec<Change>,
}

impl SyncMessage {
    pub fn encode(self) -> Vec<u8> {
        let mut buf = vec![MESSAGE_TYPE_SYNC];

        encode_hashes(&mut buf, &self.heads);
        encode_hashes(&mut buf, &self.need);
        (self.have.len() as u32).encode(&mut buf).unwrap();
        for have in self.have {
            encode_hashes(&mut buf, &have.last_sync);
            buf.extend(have.bloom.into_bytes());
        }

        (self.changes.len() as u32).encode(&mut buf).unwrap();
        for change in self.changes {
            buf.extend(change.raw_bytes())
        }

        buf
    }

    pub fn decode(bytes: Vec<u8>) -> Result<SyncMessage, AutomergeError> {
        let mut decoder = Decoder::new(Cow::Owned(bytes));

        let message_type = decoder.read::<u8>().unwrap();
        if message_type != MESSAGE_TYPE_SYNC {
            return Err(AutomergeError::EncodingError);
        }

        let heads = decode_hashes(&mut decoder);
        let need = decode_hashes(&mut decoder);
        let have_count = decoder.read::<u32>().unwrap();
        let mut have = Vec::new();
        for _ in 0..have_count {
            let last_sync = decode_hashes(&mut decoder);
            let bloom_bytes: Vec<u8> = decoder.read().unwrap();
            let bloom = BloomFilter::from(bloom_bytes);
            have.push(SyncHave { last_sync, bloom });
        }

        let change_count = decoder.read::<u32>().unwrap();
        let mut changes = Vec::new();
        for _ in 0..change_count {
            let change = decoder.read().unwrap();
            changes.push(Change::from_bytes(change).unwrap());
        }

        Ok(Self {
            heads,
            need,
            have,
            changes,
        })
    }
}

fn encode_hashes(buf: &mut Vec<u8>, hashes: &[ChangeHash]) {
    (hashes.len() as u32).encode(buf).unwrap();
    // debug_assert!(hashes.is_sorted());
    for hash in hashes {
        let bytes = &hash.0[..];
        buf.extend(bytes);
    }
}

fn decode_hashes(decoder: &mut Decoder) -> Vec<ChangeHash> {
    let length = decoder.read::<u32>().unwrap();
    let mut hashes = Vec::new();

    for i in 0..length {
        hashes.push(
            ChangeHash::try_from(decoder.read_bytes((i as u32).try_into().unwrap()).unwrap())
                .unwrap(),
        )
    }

    hashes
}

#[derive(Debug, Default)]
pub struct SyncHave {
    pub last_sync: Vec<ChangeHash>,
    pub bloom: BloomFilter,
}

impl Backend {
    pub fn generate_sync_message(
        &self,
        mut peer_state: PeerState,
    ) -> (PeerState, Option<SyncMessage>) {
        let our_heads = self.get_heads();

        let have = if peer_state.our_need.is_empty() {
            vec![self.make_bloom_filter(&peer_state.shared_heads)]
        } else {
            Vec::new()
        };

        if let Some(ref their_have) = peer_state.their_have {
            if !their_have.is_empty() {
                let last_sync = &their_have.first().unwrap().last_sync;
                if last_sync
                    .iter()
                    .all(|hash| self.get_change_by_hash(hash).is_some())
                {
                    let reset_msg = SyncMessage {
                        heads: our_heads,
                        need: Vec::new(),
                        have: vec![SyncHave {
                            last_sync: Vec::new(),
                            bloom: BloomFilter::default(),
                        }],
                        changes: Vec::new(),
                    };
                    return (peer_state, Some(reset_msg));
                }
            }
        }

        let changes_to_send = if let (Some(their_have), Some(their_need)) = (
            peer_state.their_have.as_ref(),
            peer_state.their_need.as_ref(),
        ) {
            self.get_changes_to_send(their_have, their_need)
        } else {
            Vec::new()
        };

        let heads_unchanged = if let Some(last_sent_heads) = peer_state.last_sent_heads.as_ref() {
            last_sent_heads == &our_heads
        } else {
            false
        };

        let heads_equal = if let Some(their_heads) = peer_state.their_heads.as_ref() {
            their_heads == &our_heads
        } else {
            false
        };

        if heads_unchanged
            && heads_equal
            && changes_to_send.is_empty()
            && peer_state.our_need.is_empty()
        {
            return (peer_state, None);
        }

        let sync_message = SyncMessage {
            heads: our_heads.clone(),
            have,
            need: peer_state.our_need.clone(),
            changes: changes_to_send,
        };

        peer_state.last_sent_heads = Some(our_heads);

        (peer_state, Some(sync_message))
    }

    pub fn receive_sync_message(
        &mut self,
        message: SyncMessage,
        mut old_peer_state: PeerState,
    ) -> (PeerState, Option<Patch>) {
        let mut patch = None;

        let before_heads = self.get_heads();

        if !message.changes.is_empty() {
            old_peer_state.unapplied_changes.extend(message.changes);
            let unapplied_changes = &old_peer_state.unapplied_changes;

            let our_need = self.get_missing_deps(unapplied_changes, &message.heads);

            if our_need.is_empty() {
                patch = Some(self.apply_changes(unapplied_changes.to_vec()).unwrap());
                old_peer_state.unapplied_changes = Vec::new();
                old_peer_state.shared_heads =
                    advance_heads(before_heads, self.get_heads(), old_peer_state.shared_heads);
            }
        } else if message.heads == before_heads {
            old_peer_state.last_sent_heads = Some(message.heads.clone())
        }

        if message
            .heads
            .iter()
            .all(|head| self.get_change_by_hash(head).is_some())
        {
            old_peer_state.shared_heads = message.heads.clone()
        }

        let new_peer_state = PeerState {
            shared_heads: old_peer_state.shared_heads,
            last_sent_heads: old_peer_state.last_sent_heads,
            their_have: Some(message.have),
            their_heads: Some(message.heads),
            their_need: Some(message.need),
            our_need: old_peer_state.our_need,
            unapplied_changes: old_peer_state.unapplied_changes,
        };
        (new_peer_state, patch)
    }
}

fn advance_heads(
    my_old_heads: Vec<ChangeHash>,
    my_new_heads: Vec<ChangeHash>,
    our_old_shared_heads: Vec<ChangeHash>,
) -> Vec<ChangeHash> {
    let new_heads = my_new_heads
        .into_iter()
        .filter(|head| !my_old_heads.contains(head))
        .collect::<Vec<_>>();

    let common_heads = new_heads
        .clone()
        .into_iter()
        .filter(|head| my_old_heads.contains(head) && our_old_shared_heads.contains(head))
        .collect::<Vec<_>>();

    let mut advanced_heads = HashSet::new();
    for head in new_heads {
        advanced_heads.insert(head);
    }
    for head in common_heads {
        advanced_heads.insert(head);
    }
    let mut advanced_heads = advanced_heads.into_iter().collect::<Vec<_>>();
    advanced_heads.sort();
    advanced_heads
}
