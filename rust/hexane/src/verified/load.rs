//! A column's load, run by run ([`Column::load_iter`]).
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`load/imp.rs`), which has
//! its contract word for word and is proved there.

use super::kind::{Kind, VIndex};
use super::model::Loading;
#[cfg(verus_keep_ghost)]
use super::model::Model;
use super::seal;
use super::LenIndex;
#[cfg(verus_keep_ghost)]
use super::{fill_of, Held, VGet};
use super::{Codec, Column, ColumnValueRef, Get};
#[cfg(verus_keep_ghost)]
use crate::cursor::ReadsAgree;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{Decode, Decoders};
#[cfg(verus_keep_ghost)]
use crate::load_core::Loads;
use crate::{Leb128, LoadOpts, MaybeFill, PackError, Run};
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

/// A column's load in progress, run by run: [`try_next_run`] gives the saved column's runs in turn while its slabs are cut
/// underneath, and [`finalize`] reads the rest and gives the column.
///
/// What it has given so far is `given()`; and the column `finalize`
/// gives begins with it. An error ends the load: every later call of
/// `try_next_run` fails too, and `finalize` is then not sure to give a
/// column.
///
/// [`try_next_run`]: Self::try_next_run
/// [`finalize`]: Self::finalize
pub struct ColumnLoadIter<'a, V: ColumnValueRef<C>, C: Codec = Leb128, K: VIndex<V, C> = LenIndex> {
    core: KLoad<'a, V, C, K>,
    /// empty bytes, and a fill value with a length: one run of that
    fill: Option<(Get<'a, V>, usize)>,
    /// the fill's run is given
    emitted: bool,
    length: Option<usize>,
    max_segments: usize,
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> ColumnLoadIter<'a, V, C, K> {
    /// What the load keeps to, from one call to the next.
    pub closed spec fn wf(&self) -> bool {
        &&& self.core.inv()
        &&& self.max_segments >= 2
        &&& self.fill matches Some((_v, len)) ==> len <= i64::MAX && len <= usize::MAX - 3
    }

    /// The bytes it loads.
    pub closed spec fn bytes(&self) -> Seq<u8> {
        self.core.ld.data()
    }

    /// The length the load options ask for.
    pub closed spec fn length(&self) -> Option<usize> {
        self.length
    }

    /// The bytes are empty, and the load options ask for a length and
    /// give a value to fill it with: the column is that many of it
    /// ([`fill_of`]).
    pub closed spec fn filling(&self) -> Option<(usize, Get<'a, V>)> {
        match self.fill {
            Some((v, len)) => Some((len, v)),
            None => None,
        }
    }

    /// The values of the runs it has given.
    pub closed spec fn given(&self) -> Seq<V::S> {
        match self.fill {
            Some((v, len)) => if self.emitted {
                Seq::new(len as nat, |_i: int| v.sv())
            } else {
                Seq::empty()
            },
            None => <Get<'a, V> as VGet>::svs(self.core.ld.pulled()),
        }
    }

    /// The load has given every run: `try_next_run` gave `None`.
    pub closed spec fn done(&self) -> bool {
        match self.fill {
            Some((_v, len)) => self.emitted || len == 0,
            None => !self.core.failed && self.core.ld.at_end(),
        }
    }

    /// A step of the load failed: it ends there.
    pub closed spec fn failed(&self) -> bool {
        self.fill is None && self.core.failed
    }

    /// The next run of the saved column, or `None` at its end. A fill
    /// gives all of its values as one run. The bytes are not trusted:
    /// a run is checked before it is given, and an error ends the load.
    #[inline(always)]
    pub fn try_next_run(&mut self) -> (r: Result<Option<Run<Get<'a, V>>>, PackError>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            r is Ok ==> final(self).bytes() == old(self).bytes(),
            final(self).length() == old(self).length(),
            final(self).filling() == old(self).filling(),
            r matches Ok(Some(run)) ==> run.count > 0 && final(self).given() == old(self).given() + Seq::new(
                run.count as nat,
                |_j: int| run.value.sv(),
            ),
            r matches Ok(None) ==> final(self).given() == old(self).given() && final(self).done(),
            r is Ok ==> !old(self).failed() && !final(self).failed(),
            r is Err ==> final(self).failed(),
    {
        self.try_next_run_impl()
    }

    /// Read and check what is left, and give the column: as
    /// [`load_with`](Column::load_with) gives it, and beginning
    /// with the runs given. A fill gives its column. No error unless a
    /// step failed, or the bytes are no column `load_with` takes.
    #[inline(always)]
    pub fn finalize(self) -> (r: Result<Column<V, C, K>, PackError>)
        requires
            self.wf(),
        ensures
            r matches Ok(c) ==> self.given().len() <= c@.len() && c@.take(self.given().len() as int)
                == self.given(),
            self.done() ==> (r matches Ok(c) ==> c@ == self.given()),
            match self.filling() {
                Some((len, v)) => r matches Ok(c) && c@ == Seq::new(len as nat, |_i: int| v.sv()),
                None => {
                    &&& r matches Ok(c) ==> {
                        &&& Column::<V, C>::valid(self.bytes())
                        &&& c@ == Column::<V, C>::values(self.bytes())
                        &&& self.length() matches Some(len) ==> c@.len() == len
                    }
                    &&& !self.failed() && Column::<V, C>::loadable(self.bytes()) && K::keeps(
                        Column::<V, C>::values(self.bytes()),
                    ) && (self.length() matches Some(len) ==> Column::<V, C>::values(self.bytes()).len()
                        == len) ==> r is Ok
                },
            },
    {
        self.finalize_impl()
    }
}

impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Column<V, C, K> {
    /// A load of the saved column `bytes`, with options ([`LoadOpts`]),
    /// run by run: [`ColumnLoadIter::try_next_run`] gives its runs in
    /// turn, and [`ColumnLoadIter::finalize`] the column, as
    /// [`load_with`](Self::load_with) would. Pulling runs is not needed:
    /// `load_iter(bytes, opts).finalize()` is a plain load.
    ///
    /// A slab holds two runs or more: [`Column`] panics
    /// when asked for fewer, and here that is a `requires`.
    #[inline(always)]
    pub fn load_iter<'a, F: MaybeFill<Get<'a, V>>>(bytes: &'a [u8], opts: LoadOpts<F>) -> (r: ColumnLoadIter<'a, V, C, K>)
        requires
            opts.max_segments() >= 2,
        ensures
            r.wf(),
            r.bytes() == bytes@,
            r.length() == opts.length(),
            r.filling() == fill_of(bytes@, opts.length(), opts.fill().value()),
            r.given().len() == 0,
            !r.failed(),
    {
        Self::load_iter_impl::<F>(bytes, opts)
    }
}

} // verus!
