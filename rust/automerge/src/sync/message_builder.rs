use crate::{Change, ChangeHash};

use super::{Capability, Have, Message, MessageVersion, State};

use std::borrow::Cow;

pub(super) struct MessageBuilder<'a> {
    heads: Vec<ChangeHash>,
    need: Vec<ChangeHash>,
    have: Vec<Have>,
    changes: Vec<Vec<u8>>,
    hashes: Cow<'a, [ChangeHash]>,
    supported_capabilities: Option<Vec<Capability>>,
    version: MessageVersion,
}

impl<'a> MessageBuilder<'a> {
    pub(super) fn new(changes: Vec<Change>, sync_state: &State) -> MessageBuilder<'static> {
        if sync_state.supports_v2_messages() {
            MessageBuilder::new_v2_from_changes(changes)
        } else {
            MessageBuilder::new_v1(changes)
        }
    }

    fn new_v1(changes: Vec<Change>) -> MessageBuilder<'static> {
        let hashes = Cow::Owned(changes.iter().map(|c| c.hash()).collect());
        let changes = changes
            .into_iter()
            .map(|c| c.raw_bytes().to_vec())
            .collect();
        MessageBuilder {
            heads: Vec::new(),
            need: Vec::new(),
            have: Vec::new(),
            changes,
            hashes,
            supported_capabilities: None,
            version: MessageVersion::V1,
        }
    }

    fn new_empty_v2() -> MessageBuilder<'static> {
        MessageBuilder {
            heads: Vec::new(),
            need: Vec::new(),
            hashes: Cow::Owned(vec![]),
            changes: Vec::new(),
            have: Vec::new(),
            supported_capabilities: None,
            version: MessageVersion::V2,
        }
    }

    fn new_v2_from_changes(changes: Vec<Change>) -> MessageBuilder<'static> {
        let hashes: Cow<'static, _> = Cow::Owned(changes.iter().map(|c| c.hash()).collect());
        let size = changes.iter().map(|c| c.raw_bytes().len()).sum();
        let mut encoded = Vec::with_capacity(size);
        for c in changes {
            encoded.extend_from_slice(c.raw_bytes())
        }
        Self::new_v2(encoded, hashes)
    }

    pub(super) fn new_v2<'b>(data: Vec<u8>, hashes: Cow<'b, [ChangeHash]>) -> MessageBuilder<'b> {
        if data.is_empty() {
            return Self::new_empty_v2();
        }
        MessageBuilder {
            heads: Vec::new(),
            need: Vec::new(),
            hashes,
            changes: vec![data],
            have: Vec::new(),
            supported_capabilities: None,
            version: MessageVersion::V2,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    pub(super) fn hashes(&self) -> impl Iterator<Item = &ChangeHash> {
        self.hashes.iter()
    }

    pub(super) fn heads(mut self, heads: Vec<ChangeHash>) -> Self {
        self.heads = heads;
        self
    }

    pub(super) fn need(mut self, need: Vec<ChangeHash>) -> Self {
        self.need = need;
        self
    }

    pub(super) fn have(mut self, have: Vec<Have>) -> Self {
        self.have = have;
        self
    }

    pub(super) fn supported_capabilities(
        mut self,
        supported_capabilities: Option<Vec<Capability>>,
    ) -> Self {
        self.supported_capabilities = supported_capabilities;
        self
    }

    pub(super) fn build(self) -> Message {
        Message {
            heads: self.heads,
            need: self.need,
            have: self.have,
            changes: super::ChunkList::from(self.changes),
            supported_capabilities: self.supported_capabilities,
            version: self.version,
        }
    }
}
