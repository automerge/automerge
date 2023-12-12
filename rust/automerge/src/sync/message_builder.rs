use crate::{Change, ChangeHash};

use super::{Capability, Have, Message, MessageVersion};

pub(super) struct MessageBuilder {
    heads: Vec<ChangeHash>,
    need: Vec<ChangeHash>,
    have: Vec<Have>,
    changes: Vec<Vec<u8>>,
    supported_capabilities: Option<Vec<Capability>>,
    version: MessageVersion,
}

impl MessageBuilder {
    pub(super) fn new_v1<'a, I: Iterator<Item = &'a Change>>(changes: I) -> Self {
        MessageBuilder {
            heads: Vec::new(),
            need: Vec::new(),
            have: Vec::new(),
            changes: changes.map(|c| c.raw_bytes().to_vec()).collect(),
            supported_capabilities: None,
            version: MessageVersion::V1,
        }
    }

    pub(super) fn new_v2(changes: Vec<u8>) -> Self {
        MessageBuilder {
            heads: Vec::new(),
            need: Vec::new(),
            changes: if changes.len() > 1 {
                vec![changes]
            } else {
                Vec::new()
            },
            have: Vec::new(),
            supported_capabilities: None,
            version: MessageVersion::V2,
        }
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

    pub(super) fn has_changes_to_send(&self) -> bool {
        !self.changes.is_empty()
    }
}
