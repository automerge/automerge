use crate::op_set::{OpSetInternal, OpSetMetadata};
use crate::Automerge;

pub(crate) trait Exportable<T> {
    fn export<M: HasMetadata>(&self, m: &M) -> T;
}

pub(crate) trait Importable<T, M: HasMutMetadata> {
    type Error;
    fn import(&self, m: &mut M) -> Result<T, Self::Error>;
}

pub(crate) trait HasMetadata {
    fn meta(&self) -> &OpSetMetadata;
}

pub(crate) trait HasMutMetadata {
    fn mut_meta(&mut self) -> &mut OpSetMetadata;
}

impl HasMetadata for OpSetMetadata {
    fn meta(&self) -> &OpSetMetadata {
        self
    }
}

impl HasMetadata for &OpSetMetadata {
    fn meta(&self) -> &OpSetMetadata {
        self
    }
}

impl HasMutMetadata for OpSetMetadata {
    fn mut_meta(&mut self) -> &mut OpSetMetadata {
        self
    }
}

impl HasMutMetadata for &mut OpSetMetadata {
    fn mut_meta(&mut self) -> &mut OpSetMetadata {
        self
    }
}

impl HasMetadata for OpSetInternal {
    fn meta(&self) -> &OpSetMetadata {
        &self.m
    }
}

impl HasMutMetadata for OpSetInternal {
    fn mut_meta(&mut self) -> &mut OpSetMetadata {
        &mut self.m
    }
}

impl HasMetadata for Automerge {
    fn meta(&self) -> &OpSetMetadata {
        self.ops().meta()
    }
}

impl HasMutMetadata for Automerge {
    fn mut_meta(&mut self) -> &mut OpSetMetadata {
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
