use crate::{Change, ChangeHash};

use super::{Capability, Changes, Have, Message};

enum MessageVersion {
    V1(Vec<Change>),
    V2(Changes),
}

pub(super) struct MessageBuilder {
    heads: Vec<ChangeHash>,
    need: Vec<ChangeHash>,
    have: Vec<Have>,
    supported_capabilities: Option<Vec<Capability>>,
    version: MessageVersion,
}

impl MessageBuilder {
    pub(super) fn new_v1(changes: Vec<Change>) -> Self {
        MessageBuilder {
            heads: Vec::new(),
            need: Vec::new(),
            have: Vec::new(),
            supported_capabilities: None,
            version: MessageVersion::V1(changes),
        }
    }

    pub(super) fn new_v2(changes: Changes) -> Self {
        MessageBuilder {
            heads: Vec::new(),
            need: Vec::new(),
            have: Vec::new(),
            supported_capabilities: None,
            version: MessageVersion::V2(changes),
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
        match self.version {
            MessageVersion::V1(changes) => Message::V1 {
                heads: self.heads,
                need: self.need,
                have: self.have,
                changes,
                supported_capabilities: self.supported_capabilities,
            },
            MessageVersion::V2(changes) => Message::V2 {
                heads: self.heads,
                need: self.need,
                have: self.have,
                changes,
                supported_capabilities: self.supported_capabilities,
            },
        }
    }

    pub(super) fn has_changes_to_send(&self) -> bool {
        match self {
            Self {
                version: MessageVersion::V1(changes),
                ..
            } => !changes.is_empty(),
            Self {
                version: MessageVersion::V2(changes),
                ..
            } => !changes.is_empty(),
        }
    }
}
