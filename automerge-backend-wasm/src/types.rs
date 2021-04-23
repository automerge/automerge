use automerge_backend::{AutomergeError, Change};
use automerge_backend::{BloomFilter, SyncHave, SyncMessage, SyncState};
use automerge_protocol::ChangeHash;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::convert::TryFrom;

#[derive(Serialize, Deserialize)]
pub struct BinaryChange(#[serde(with = "serde_bytes")] pub Vec<u8>);

#[derive(Serialize, Deserialize)]
pub struct BinaryDocument(#[serde(with = "serde_bytes")] pub Vec<u8>);

#[derive(Serialize, Deserialize)]
pub struct BinarySyncState(#[serde(with = "serde_bytes")] pub Vec<u8>);

#[derive(Serialize, Deserialize)]
pub struct BinarySyncMessage(#[serde(with = "serde_bytes")] pub Vec<u8>);

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawSyncState {
    shared_heads: Vec<ChangeHash>,
    last_sent_heads: Option<Vec<ChangeHash>>,
    their_heads: Option<Vec<ChangeHash>>,
    their_need: Option<Vec<ChangeHash>>,
    their_have: Option<Vec<RawSyncHave>>,
    sent_hashes: HashSet<ChangeHash>,
}

impl TryFrom<SyncState> for RawSyncState {
    type Error = AutomergeError;

    fn try_from(value: SyncState) -> Result<Self, Self::Error> {
        let have = if let Some(have) = value.their_have {
            Some(
                have.into_iter()
                    .map(RawSyncHave::try_from)
                    .collect::<Result<_, _>>()?,
            )
        } else {
            None
        };
        Ok(Self {
            shared_heads: value.shared_heads,
            last_sent_heads: value.last_sent_heads,
            their_heads: value.their_heads,
            their_need: value.their_need,
            their_have: have,
            sent_hashes: value.sent_hashes,
        })
    }
}

impl TryFrom<RawSyncState> for SyncState {
    type Error = AutomergeError;

    fn try_from(value: RawSyncState) -> Result<Self, Self::Error> {
        let have = if let Some(have) = value.their_have {
            Some(
                have.into_iter()
                    .map(SyncHave::try_from)
                    .collect::<Result<_, _>>()?,
            )
        } else {
            None
        };
        Ok(Self {
            shared_heads: value.shared_heads,
            last_sent_heads: value.last_sent_heads,
            their_heads: value.their_heads,
            their_need: value.their_need,
            their_have: have,
            sent_hashes: value.sent_hashes,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawSyncMessage {
    pub heads: Vec<ChangeHash>,
    pub need: Vec<ChangeHash>,
    pub have: Vec<RawSyncHave>,
    pub changes: Vec<BinaryChange>,
}

impl TryFrom<SyncMessage> for RawSyncMessage {
    type Error = AutomergeError;

    fn try_from(value: SyncMessage) -> Result<Self, Self::Error> {
        let have = value
            .have
            .into_iter()
            .map(RawSyncHave::try_from)
            .collect::<Result<_, _>>()?;
        let changes = value
            .changes
            .into_iter()
            .map(|c| BinaryChange(c.raw_bytes().to_vec()))
            .collect();
        Ok(Self {
            heads: value.heads,
            need: value.need,
            have,
            changes,
        })
    }
}

impl TryFrom<RawSyncMessage> for SyncMessage {
    type Error = AutomergeError;

    fn try_from(value: RawSyncMessage) -> Result<Self, Self::Error> {
        let have = value
            .have
            .into_iter()
            .map(SyncHave::try_from)
            .collect::<Result<_, _>>()?;
        let changes = value
            .changes
            .into_iter()
            .map(|b| Change::from_bytes(b.0))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            heads: value.heads,
            need: value.need,
            have,
            changes,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawSyncHave {
    pub last_sync: Vec<ChangeHash>,
    #[serde(with = "serde_bytes")]
    pub bloom: Vec<u8>,
}

impl TryFrom<SyncHave> for RawSyncHave {
    type Error = AutomergeError;

    fn try_from(value: SyncHave) -> Result<Self, Self::Error> {
        Ok(Self {
            last_sync: value.last_sync,
            bloom: value.bloom.into_bytes()?,
        })
    }
}

impl TryFrom<RawSyncHave> for SyncHave {
    type Error = AutomergeError;

    fn try_from(raw: RawSyncHave) -> Result<Self, Self::Error> {
        Ok(Self {
            last_sync: raw.last_sync,
            bloom: BloomFilter::try_from(raw.bloom.as_slice())?,
        })
    }
}
