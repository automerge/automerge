use crate::Automerge;

pub(crate) trait DocRef {
    fn doc_ref(&self) -> &Automerge;
}

impl DocRef for Automerge {
    fn doc_ref(&self) -> &Automerge {
        self
    }
}
