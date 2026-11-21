//! The implementation of `verified::load` and its proofs: what its
//! specification (`load.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

#[cfg(verus_keep_ghost)]
use super::super::lemma_held;
use super::*;

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
pub(super) type KLoad<'a, V, C, K> = LoadCore<
    'a,
    <V as seal::Sealed<C>>::E,
    <V as seal::Sealed<C>>::X,
    <K as Kind<V, C>>::W,
    <K as Kind<V, C>>::G,
    <<V as seal::Sealed<C>>::E as Loading<'a, <V as seal::Sealed<C>>::X>>::Ld,
>;

impl<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>> VerifiedLoadIter<'a, V, C, K> {
    pub(super) fn try_next_run_impl(&mut self) -> (r: Result<Option<Run<Get<'a, V>>>, PackError>)
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

    pub(super) fn finalize_impl(self) -> (r: Result<VerifiedColumn<V, C, K>, PackError>)
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
        match self.core.finish::<K::I>(self.max_segments, self.length, i64::MIN, i64::MAX) {
            Ok(core) => {
                let c = VerifiedColumn { core };
                proof {
                    // the column begins with what was given
                    let n = p.len() as int;
                    let h = p.map_values(
                        |v: <<V::X as Decoders<'a>>::D as Decode>::Item| <V::E as ReadsAgree<'a, V::X>>::own(v),
                    );
                    lemma_held::<V, C>(p);
                    assert(h =~= super::super::held::<V, C>(p));
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
    pub(super) fn load_iter_impl<'a, F: MaybeFill<Get<'a, V>>>(bytes: &'a [u8], opts: LoadOpts<F>) -> (r: VerifiedLoadIter<'a, V, C, K>)
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
