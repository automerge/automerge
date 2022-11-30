//! This module is essentially a type level Option. It is used in sitations where we know at
//! compile time whether an `OpObserver` is available to track changes in a transaction.
use crate::{ChangeHash, OpObserver};

mod private {
    pub trait Sealed {}
    impl<O: super::OpObserver> Sealed for super::Observed<O> {}
    impl Sealed for super::UnObserved {}
}

pub trait Observation: private::Sealed {
    type Obs: OpObserver;
    type CommitResult;

    fn observer(&mut self) -> Option<&mut Self::Obs>;
    fn make_result(self, hash: Option<ChangeHash>) -> Self::CommitResult;
    fn branch(&self) -> Self;
    fn merge(&mut self, other: &Self);
}

#[derive(Clone, Debug)]
pub struct Observed<Obs: OpObserver>(Obs);

impl<O: OpObserver> Observed<O> {
    pub(crate) fn new(o: O) -> Self {
        Self(o)
    }

    pub(crate) fn observer(&mut self) -> &mut O {
        &mut self.0
    }
}

impl<Obs: OpObserver> Observation for Observed<Obs> {
    type Obs = Obs;
    type CommitResult = (Obs, Option<ChangeHash>);
    fn observer(&mut self) -> Option<&mut Self::Obs> {
        Some(&mut self.0)
    }

    fn make_result(self, hash: Option<ChangeHash>) -> Self::CommitResult {
        (self.0, hash)
    }

    fn branch(&self) -> Self {
        Self(self.0.branch())
    }

    fn merge(&mut self, other: &Self) {
        self.0.merge(&other.0)
    }
}

#[derive(Clone, Default, Debug)]
pub struct UnObserved;
impl UnObserved {
    pub fn new() -> Self {
        Self
    }
}

impl Observation for UnObserved {
    type Obs = ();
    type CommitResult = Option<ChangeHash>;
    fn observer(&mut self) -> Option<&mut Self::Obs> {
        None
    }

    fn make_result(self, hash: Option<ChangeHash>) -> Self::CommitResult {
        hash
    }

    fn branch(&self) -> Self {
        Self
    }

    fn merge(&mut self, _other: &Self) {}
}
