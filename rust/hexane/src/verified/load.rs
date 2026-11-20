//! A column's load, run by run ([`VerifiedColumn::load_iter`]), as the
//! old API's [`Column::load_iter`](crate::Column::load_iter) gives it.

use super::kind::{Kind, VIndex};
use super::model::Loading;
#[cfg(verus_keep_ghost)]
use super::model::Model;
use super::seal;
use super::LenIndex;
#[cfg(verus_keep_ghost)]
use super::{fill_of, lemma_held, Held, VGet};
use super::{Get, VCodec, VValue, VerifiedColumn};
use crate::btree::SlabBTree;
#[cfg(verus_keep_ghost)]
use crate::cursor::ReadsAgree;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{Decode, Decoders};
use crate::load_core::LoadCore;
#[cfg(verus_keep_ghost)]
use crate::load_core::Loads;
use crate::{Leb128, LoadOpts, MaybeFill, PackError, Run};
use vstd::prelude::*;

verus! {

/// The load core of a column of `V` kept with an index of kind `K`, of
/// bytes that live for `'a`.
type KLoad<'a, V, C, K> = LoadCore<
    'a,
    <V as seal::Sealed<C>>::E,
    <V as seal::Sealed<C>>::X,
    <K as Kind<V, C>>::W,
    <K as Kind<V, C>>::G,
    <<V as seal::Sealed<C>>::E as Loading<'a, <V as seal::Sealed<C>>::X>>::Ld,
>;

/// A column's load in progress, run by run, as the old API's
/// [`ColumnLoadIter`](crate::ColumnLoadIter) is: [`try_next_run`]
/// gives the saved column's runs in turn while its slabs are cut
/// underneath, and [`finalize`] reads the rest and gives the column.
///
/// What it has given so far is `given()`; and the column `finalize`
/// gives begins with it. An error ends the load: every later call, and
/// `finalize`, fails too.
///
/// [`try_next_run`]: Self::try_next_run
/// [`finalize`]: Self::finalize
pub struct VerifiedLoadIter<'a, V: VValue<C>, C: VCodec = Leb128, K: VIndex<V, C> = LenIndex> {
    core: KLoad<'a, V, C, K>,
    /// empty bytes, and a fill value with a length: one run of that
    fill: Option<(Get<'a, V>, usize)>,
    /// the fill's run is given
    emitted: bool,
    length: Option<usize>,
    max_segments: usize,
}

impl<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>> VerifiedLoadIter<'a, V, C, K> {
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
        if let Some((value, len)) = self.fill {
            if self.emitted || len == 0 {
                return Ok(None);
            }
            self.emitted = true;
            return Ok(Some(Run { count: len, value }));
        }
        let ghost p0 = self.core.ld.pulled();
        let r = self.core.next_run();
        proof {
            if let Ok(Some(run)) = r {
                let more = Seq::new(run.count as nat, |_j: int| run.value);
                <Get<'a, V> as VGet>::lemma_svs(p0);
                <Get<'a, V> as VGet>::lemma_svs(more);
                <Get<'a, V> as VGet>::lemma_svs(p0 + more);
            }
        }
        r
    }

    /// Read and check what is left, and give the column: as
    /// [`load_with`](VerifiedColumn::load_with) gives it, and beginning
    /// with the runs given. A fill gives its column. No error unless a
    /// step failed, or the bytes are no column `load_with` takes.
    pub fn finalize(self) -> (r: Result<VerifiedColumn<V, C, K>, PackError>)
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
                        &&& VerifiedColumn::<V, C>::valid(self.bytes())
                        &&& c@ == VerifiedColumn::<V, C>::values(self.bytes())
                        &&& self.length() matches Some(len) ==> c@.len() == len
                    }
                    &&& !self.failed() && VerifiedColumn::<V, C>::loadable(self.bytes()) && K::keeps(
                        VerifiedColumn::<V, C>::values(self.bytes()),
                    ) && (self.length() matches Some(len) ==> VerifiedColumn::<V, C>::values(self.bytes()).len()
                        == len) ==> r is Ok
                },
            },
    {
        if let Some((value, len)) = self.fill {
            let c = VerifiedColumn::<V, C, K>::filled(len, value, self.max_segments);
            return Ok(c);
        }
        let ghost (data, p) = (self.core.ld.data(), self.core.ld.pulled());
        proof {
            let vs = <V::E as Model<V::X>>::values(data);
            <V::E as Loading<'a, V::X>>::lemma_loader(data);
            <Held<V, C> as VGet>::lemma_svs(vs);
            if K::keeps(<Held<V, C> as VGet>::svs(vs)) {
                K::lemma_refuses(vs);
            }
        }
        match self.core.finish::<SlabBTree<K::W>>(self.max_segments, self.length, i64::MIN, i64::MAX) {
            Ok(core) => {
                let c = VerifiedColumn { core };
                proof {
                    // the column begins with what was given
                    let n = p.len() as int;
                    let h = p.map_values(
                        |v: <<V::X as Decoders<'a>>::D as Decode>::Item| <V::E as ReadsAgree<'a, V::X>>::own(v),
                    );
                    lemma_held::<V, C>(p);
                    assert(h =~= super::held::<V, C>(p));
                    <Held<V, C> as VGet>::lemma_svs(core@);
                    <Held<V, C> as VGet>::lemma_svs(h);
                }
                Ok(c)
            },
            Err(e) => Err(e),
        }
    }
}

impl<V: VValue<C>, C: VCodec, K: VIndex<V, C>> VerifiedColumn<V, C, K> {
    /// A load of the saved column `bytes`, with options ([`LoadOpts`]),
    /// run by run: [`VerifiedLoadIter::try_next_run`] gives its runs in
    /// turn, and [`VerifiedLoadIter::finalize`] the column, as
    /// [`load_with`](Self::load_with) would. Pulling runs is not needed:
    /// `load_iter(bytes, opts).finalize()` is a plain load.
    ///
    /// A slab holds two runs or more: [`Column`](crate::Column) panics
    /// when asked for fewer, and here that is a `requires`.
    pub fn load_iter<'a, F: MaybeFill<Get<'a, V>>>(bytes: &'a [u8], opts: LoadOpts<F>) -> (r: VerifiedLoadIter<'a, V, C, K>)
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
        let (length, fill, max_segments) = opts.parts();
        let mut fill_run = None;
        if bytes.len() == 0 {
            if let Some(len) = length {
                if len as u64 <= i64::MAX as u64 && len <= usize::MAX - 3 {
                    if let Some(v) = fill.fill_value() {
                        fill_run = Some((v, len));
                    }
                }
            }
        }
        let core = LoadCore::start(bytes, max_segments);
        let r: VerifiedLoadIter<'a, V, C, K> = VerifiedLoadIter { core, fill: fill_run, emitted: false, length, max_segments };
        proof {
            <Get<'a, V> as VGet>::lemma_svs(r.core.ld.pulled());
        }
        r
    }
}

} // verus!
