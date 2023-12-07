use crate::exid::ExId;
use crate::port::{Exportable, HasMetadata};
use crate::types::{OpId, ROOT};

impl<U, E: Exportable<U>> Exportable<U> for &E {
    fn export<M: HasMetadata>(&self, m: &M) -> U {
        (*self).export(m)
    }
}

impl<T: Exportable<E>, E> Exportable<Option<E>> for Option<T> {
    fn export<M: HasMetadata>(&self, m: &M) -> Option<E> {
        self.as_ref().map(|e| e.export(m))
    }
}

impl Exportable<ExId> for OpId {
    fn export<M: HasMetadata>(&self, m: &M) -> ExId {
        if self == &ROOT {
            ExId::Root
        } else {
            ExId::Id(
                self.counter(),
                m.meta().actors.cache[self.actor()].clone(),
                self.actor(),
            )
        }
    }
}
