use crate::op_set::{OpSetData, OpSetInternal};
use crate::Automerge;

pub(crate) trait Exportable<T> {
    fn export<M: HasMetadata>(&self, m: &M) -> T;
}

pub(crate) trait Importable<T, M: HasMutMetadata> {
    type Error;
    fn import(&self, m: &mut M) -> Result<T, Self::Error>;
}

pub(crate) trait HasMetadata {
    fn meta(&self) -> &OpSetData;
}

pub(crate) trait HasMutMetadata {
    fn mut_meta(&mut self) -> &mut OpSetData;
}

impl HasMetadata for OpSetData {
    fn meta(&self) -> &OpSetData {
        self
    }
}

impl HasMetadata for &OpSetData {
    fn meta(&self) -> &OpSetData {
        self
    }
}

impl HasMutMetadata for OpSetData {
    fn mut_meta(&mut self) -> &mut OpSetData {
        self
    }
}

impl HasMutMetadata for &mut OpSetData {
    fn mut_meta(&mut self) -> &mut OpSetData {
        self
    }
}

impl HasMetadata for OpSetInternal {
    fn meta(&self) -> &OpSetData {
        &self.osd
    }
}

impl HasMutMetadata for OpSetInternal {
    fn mut_meta(&mut self) -> &mut OpSetData {
        &mut self.osd
    }
}

impl HasMetadata for Automerge {
    fn meta(&self) -> &OpSetData {
        self.ops().meta()
    }
}

impl HasMetadata for &Automerge {
    fn meta(&self) -> &OpSetData {
        self.ops().meta()
    }
}

impl HasMutMetadata for Automerge {
    fn mut_meta(&mut self) -> &mut OpSetData {
        self.ops_mut().mut_meta()
    }
}

/*
impl Exportable<ExId> for OpId {
    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(
                id.counter(),
                self.m.actors.cache[id.actor()].clone(),
                id.actor(),
            )
        }
    }
}
*/
