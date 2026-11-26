use crate::lookup_spec::{model, modeled};
use crate::projection::{FragmentView, Symbols};
use crate::selection_spec::{biggest, closed, supplies};
use crate::spec::candidate_edges;
use std::hash::Hash;
use vstd::{
    laws_eq::obeys_concrete_eq,
    prelude::*,
    std_specs::hash::{hash_map_inv, obeys_key_model},
};

verus! {

broadcast use {vstd::seq_lib::group_seq_properties,vstd::set::group_set_lemmas,vstd::seq_lib::group_to_multiset_ensures,vstd::multiset::group_multiset_axioms};

pub(crate) open spec fn hash_laws<H:Eq+Hash>()->bool {obeys_key_model::<H>() && obeys_concrete_eq::<H>()}

pub(crate) open spec fn symbols_match<H>(hashes:Seq<H>,ids:Map<H,usize>)->bool {
    &&& hashes.no_duplicates() && ids.dom()==hashes.to_set()
    &&& forall|i:int| 0<=i<hashes.len() ==> ids[#[trigger] hashes[i]]==i
}

pub(crate) open spec fn valid<H:Eq+Hash>(s:&Symbols<H>)->bool {
    hash_map_inv(&s.ids) && symbols_match(s.hashes@,s.ids@)
}

pub(crate) open spec fn prefix<H>(before:Seq<H>,after:Seq<H>)->bool {
    before.len()<=after.len() && forall|i:int| 0<=i<before.len() ==> #[trigger] before[i]==#[trigger] after[i]
}

pub(crate) open spec fn interned<H>(before:Seq<H>,after:Seq<H>,hash:H,id:usize)->bool {
    &&& prefix(before,after) && id<after.len() && after[id as int]==hash
    &&& if before.contains(hash) {after==before} else {after==before.push(hash) && id==before.len()}
}

pub(crate) proof fn added_symbol<H>(before:Seq<H>,old_ids:Map<H,usize>,after:Seq<H>,ids:Map<H,usize>,hash:H)
    requires before.len()<=usize::MAX,symbols_match(before,old_ids),!old_ids.contains_key(hash),after==before.push(hash),
        ids==old_ids.insert(hash,before.len() as usize),
    ensures symbols_match(after,ids),interned(before,after,hash,before.len() as usize),
{
    assert(!before.contains(hash));
    before.lemma_push_to_set_commute(hash);
    assert(ids.dom() =~= after.to_set());
    assert forall|i:int| 0<=i<after.len() implies ids[#[trigger] after[i]]==i by {
        if i<before.len() { assert(before.contains(before[i])); assert(before[i]!=hash); }
    }
    assert forall|i:int,j:int| 0<=i<j<after.len() implies after[i]!=after[j] by {
        if j==before.len(){assert(before.contains(before[i]));}
    }
}

pub(crate) proof fn prove_prefix<H>(start:Seq<H>,old:Seq<H>,new:Seq<H>)
    requires prefix(start,old),prefix(old,new),ensures prefix(start,new),
{
    assert forall|i:int| 0<=i<start.len() implies #[trigger] start[i]==#[trigger] new[i] by {
        assert(start[i]==old[i]); assert(old[i]==new[i]);
    }
}

proof fn symbol_for<H:Eq+Hash>(symbols:&Symbols<H>,h:H)
    requires valid(symbols),symbols.ids@.contains_key(h),
    ensures symbols.ids@[h]<symbols.hashes@.len(),symbols.hashes@[symbols.ids@[h] as int]==h,
{
    assert(symbols.hashes@.to_set().contains(h));
    assert(symbols.hashes@.contains(h));
    let i=choose|i:int| 0<=i<symbols.hashes@.len() && #[trigger] symbols.hashes@[i]==h;
    assert(symbols.ids@[symbols.hashes@[i]]==i);
}

pub(crate) open spec fn member_observations<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>>(items:Seq<T>,view:F)->bool {
    &&& items.len()<=usize::MAX
    &&& forall|i:int| 0<=i<items.len() ==> #[trigger] modeled(view,&items[i])
}

pub(crate) open spec fn raw_need<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>,A:Fn(H)->bool>(items:Seq<T>,view:F,accepted:A,f:int,h:H)->bool {
    let descriptor=model(view,&items[f]);
    descriptor.boundary@.contains(h) && !descriptor.members@.contains(h) && !model(accepted,h)
}

pub(crate) open spec fn observations<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>,A:Fn(H)->bool>(items:Seq<T>,view:F,accepted:A)->bool {
    &&& member_observations(items,view)
    &&& forall|i:int,j:int| 0<=i<items.len() && 0<=j<model(view,&items[i]).boundary@.len()
        ==> modeled(accepted,#[trigger] model(view,&items[i]).boundary@[j])
}

pub(crate) open spec fn aligned_row<H>(source:Seq<H>,ids:Seq<usize>,hashes:Seq<H>)->bool {
    &&& source.len()==ids.len()
    &&& forall|j:int| 0<=j<ids.len() ==> (#[trigger] ids[j])<hashes.len() && hashes[ids[j] as int]==source[j]
}

pub(crate) open spec fn members_aligned<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>>(items:Seq<T>,view:F,hashes:Seq<H>,rows:Seq<Vec<usize>>)->bool {
    &&& rows.len()<=items.len()
    &&& forall|i:int| 0<=i<rows.len() ==> aligned_row(model(view,&items[i]).members@,(#[trigger] rows[i])@,hashes)
}

pub(crate) proof fn members_follow_extension<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>>(items:Seq<T>,view:F,before:Seq<H>,after:Seq<H>,rows:Seq<Vec<usize>>)
    requires members_aligned(items,view,before,rows),prefix(before,after),
    ensures members_aligned(items,view,after,rows),
{
    assert forall|i:int| 0<=i<rows.len() implies aligned_row(model(view,&items[i]).members@,(#[trigger] rows[i])@,after) by {
        let ids=rows[i]@;
        assert forall|j:int| 0<=j<ids.len() implies ids[j]<after.len() && (#[trigger] after[ids[j] as int])==model(view,&items[i]).members@[j] by {
            assert(ids[j]<before.len());
        }
    }
}

pub(crate) proof fn extend_member_row<H>(members:Seq<H>,j:int,prior:Seq<usize>,before:Seq<H>,after:Seq<H>,id:usize,next:Seq<usize>)
    requires 0<=j<members.len(),aligned_row(members.take(j),prior,before),interned(before,after,members[j],id),next==prior.push(id),
    ensures aligned_row(members.take(j+1),next,after),
{
    assert forall|i:int| 0<=i<next.len() implies (#[trigger] next[i])<after.len() && after[next[i] as int]==members.take(j+1)[i] by {
        if i<j {assert(prior[i]<before.len());}
    }
}

// A prefix of whole groups plus a prefix of the current boundary. No
// other group or hash may contribute a requirement to this index.
pub(crate) open spec fn observed_need<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>,A:Fn(H)->bool>(items:Seq<T>,view:F,accepted:A,i:int,j:int,f:usize,h:H)->bool {
    if f<i {raw_need(items,view,accepted,f as int,h)}
    else if f==i && f<items.len(){
        let descriptor=model(view,&items[f as int]);
        descriptor.boundary@.take(j).contains(h) && !descriptor.members@.contains(h) && !model(accepted,h)
    }else{false}
}

pub(crate) open spec fn at_requirement<H:Eq+Hash>(symbols:&Symbols<H>,required:Seq<Vec<usize>>,f:usize,h:H)->bool{
    symbols.ids@.contains_key(h) && required[symbols.ids@[h] as int]@.contains(f)
}

pub(crate) open spec fn exact_requirements<'i,'m,T:'i,H:Eq+Hash+'m,F:Fn(&'i T)->FragmentView<'m,H>,A:Fn(H)->bool>(
    items:Seq<T>,view:F,accepted:A,i:int,j:int,symbols:&Symbols<H>,required:Seq<Vec<usize>>,
)->bool {
    &&& required.len()==symbols.hashes@.len()
    &&& forall|key:int,k:int| 0<=key<required.len() && 0<=k<required[key]@.len() ==> #[trigger] required[key]@[k]<items.len()
    &&& forall|f:usize,h:H| #[trigger] at_requirement(symbols,required,f,h) <==> observed_need(items,view,accepted,i,j,f,h)
}

pub(crate) open spec fn requirement_step<H:Eq+Hash>(before:&Symbols<H>,prior:Seq<Vec<usize>>,after:&Symbols<H>,now:Seq<Vec<usize>>,h:H,required:bool,f:usize)->bool {
    if !required { after.hashes@==before.hashes@ && after.ids@==before.ids@ && now==prior }
    else {
        let id=after.ids@[h];
        &&& after.ids@.contains_key(h) && interned(before.hashes@,after.hashes@,h,id)
        &&& now.len()==after.hashes@.len()
        &&& forall|q:int| 0<=q<now.len() ==> (#[trigger] now[q])@ == if q==id {
            if q<prior.len() {prior[q]@.push(f)} else {seq![f]}
        } else { prior[q]@ }
    }
}

pub(crate) proof fn extend_requirements<'i,'m,T:'i,H:Eq+Hash+'m,F:Fn(&'i T)->FragmentView<'m,H>,A:Fn(H)->bool>(
    items:Seq<T>,view:F,accepted:A,i:int,j:int,before:&Symbols<H>,prior:Seq<Vec<usize>>,after:&Symbols<H>,now:Seq<Vec<usize>>,
)
    requires valid(before),valid(after),0<=i<items.len()<=usize::MAX,0<=j<model(view,&items[i]).boundary@.len(),
        exact_requirements(items,view,accepted,i,j,before,prior),
        {
            let f=model(view,&items[i]); let h=f.boundary@[j];
            requirement_step(before,prior,after,now,h,!f.members@.contains(h) && !model(accepted,h),i as usize)
        },
    ensures exact_requirements(items,view,accepted,i,j+1,after,now),prefix(before.hashes@,after.hashes@),
{
    let descriptor=model(view,&items[i]);
    let h=descriptor.boundary@[j];
    let required=!descriptor.members@.contains(h) && !model(accepted,h);
    descriptor.boundary@.lemma_take_succ_push(j);
    assert forall|f:usize,b:H| #[trigger] at_requirement(after,now,f,b) <==> observed_need(items,view,accepted,i,j+1,f,b) by {
        assert(observed_need(items,view,accepted,i,j+1,f,b)<==>
            observed_need(items,view,accepted,i,j,f,b) || (f==i && b==h && required));
        if required {
            if before.ids@.contains_key(b) {
                symbol_for(before,b);
                let id=before.ids@[b];
                assert(after.hashes@[id as int]==b);
                assert(after.hashes@.contains(b));
                assert(after.ids@[b]==id);
                assert(after.ids@.contains_key(b));
                if b==h {
                    assert(now[id as int]@==prior[id as int]@.push(i as usize));
                } else {
                    symbol_for(after,h);
                    assert(id!=after.ids@[h]);
                    assert(now[id as int]@==prior[id as int]@);
                }
            } else if b==h {
                assert(!before.hashes@.contains(h));
                let id=after.ids@[h];
                assert(id==before.hashes@.len());
                assert(now[id as int]@ == seq![i as usize]);
            } else {
                if after.ids@.contains_key(b){
                    symbol_for(after,b);
                    let q=after.ids@[b];
                    if q<before.hashes@.len() {
                        assert(before.hashes@[q as int]==b);
                        assert(before.hashes@.contains(b));
                        assert(before.ids@.contains_key(b));
                    } else {
                        assert(after.hashes@==before.hashes@.push(h));
                        assert(q==before.hashes@.len());
                        assert(after.hashes@[q as int]==h);
                    }
                }
                assert(!after.ids@.contains_key(b));
            }
        }
        assert(at_requirement(after,now,f,b)<==>at_requirement(before,prior,f,b) || (f==i && b==h && required));
    }
}

pub(crate) proof fn advance_requirements<'i,'m,T:'i,H:Eq+Hash+'m,F:Fn(&'i T)->FragmentView<'m,H>,A:Fn(H)->bool>(
    items:Seq<T>,view:F,accepted:A,i:int,symbols:&Symbols<H>,required:Seq<Vec<usize>>,
)
    requires 0<=i<items.len()<=usize::MAX,
        exact_requirements(items,view,accepted,i,model(view,&items[i]).boundary@.len() as int,symbols,required),
    ensures exact_requirements(items,view,accepted,i+1,0,symbols,required),
{
    let boundary=model(view,&items[i]).boundary@;
    assert(boundary.take(boundary.len() as int) =~= boundary);
}

#[verifier::opaque]
pub(crate) open spec fn total_members(rows:Seq<Vec<usize>>,end:int)->nat
    decreases end,
{
    if end<=0 {0} else {total_members(rows,end-1)+rows[end-1]@.len()}
}

#[verifier::opaque]
pub(crate) open spec fn raw_member_count<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>>(items:Seq<T>,view:F,end:int)->nat
    decreases end,
{
    if end<=0 {0} else {raw_member_count(items,view,end-1)+model(view,&items[end-1]).members@.len()}
}

pub(crate) proof fn count_is_preserved<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>>(items:Seq<T>,view:F,hashes:Seq<H>,rows:Seq<Vec<usize>>)
    requires members_aligned(items,view,hashes,rows),rows.len()==items.len(),
    ensures total_members(rows,rows.len() as int)==raw_member_count(items,view,items.len() as int),
{
    count_at_prefix(items,view,hashes,rows,rows.len() as int);
}

proof fn count_at_prefix<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>>(items:Seq<T>,view:F,hashes:Seq<H>,rows:Seq<Vec<usize>>,i:int)
    requires 0<=i<=rows.len(),members_aligned(items,view,hashes,rows),
    ensures total_members(rows,i)==raw_member_count(items,view,i),
    decreases i,
{
    reveal(total_members);reveal(raw_member_count);
    if i>0 {
        assert(aligned_row(model(view,&items[i-1]).members@,rows[i-1]@,hashes));
        count_at_prefix(items,view,hashes,rows,i-1);
    }
}

proof fn increasing_total(rows:Seq<Vec<usize>>,i:int,j:int)
    requires 0<=i<=j<=rows.len(),ensures total_members(rows,i)<=total_members(rows,j),decreases j,
{
    reveal(total_members);
    if i<j {increasing_total(rows,i,j-1);}
}

pub(crate) proof fn count_limit(rows:Seq<Vec<usize>>,i:int)
    requires 0<=i<rows.len(),total_members(rows,rows.len() as int)<=usize::MAX,
    ensures total_members(rows,i)+rows[i]@.len()<=usize::MAX,
{
    reveal(total_members);increasing_total(rows,i+1,rows.len() as int);
}

pub(crate) open spec fn counted(rows:Seq<Vec<usize>>,end:int,counts:Seq<usize>,n:nat)->bool{
    counts.len()==n && forall|k:usize| k<n ==> #[trigger] counts[k as int]==candidate_edges(rows.take(end),Seq::empty(),k)
}

pub(crate) open spec fn partial_counts(before:Seq<usize>,row:Seq<usize>,j:int,now:Seq<usize>)->bool{
    now.len()==before.len() && forall|k:usize| k<now.len() ==> #[trigger] now[k as int]==before[k as int]+row.take(j).to_multiset().count(k)
}

proof fn slot_within_total(rows:Seq<Vec<usize>>,i:int,key:usize)
    requires 0<=i<=rows.len(),ensures candidate_edges(rows.take(i),Seq::empty(),key)<=total_members(rows,i),decreases i,
{
    reveal(total_members);reveal(candidate_edges);
    if i>0 {
        assert(rows.take(i).drop_last() =~= rows.take(i-1));
        slot_within_total(rows,i-1,key);
        assert(rows[i-1]@.to_multiset().count(key)<=rows[i-1]@.to_multiset().len());
    }
}

pub(crate) proof fn count_room(rows:Seq<Vec<usize>>,i:int,counts:Seq<usize>,before:Seq<usize>,row:Seq<usize>,j:int,id:usize)
    requires 0<=i<rows.len(),0<=j<row.len(),id<counts.len(),counted(rows,i,before,before.len()),partial_counts(before,row,j,counts),
        total_members(rows,i)+row.len()<=usize::MAX,
    ensures counts[id as int]<usize::MAX,
{
    slot_within_total(rows,i,id);
    assert(row.take(j).to_multiset().count(id)<=row.take(j).to_multiset().len());
}

pub(crate) proof fn start_counts(before:Seq<usize>,row:Seq<usize>)
    ensures partial_counts(before,row,0,before),
{
    let start=row.take(0);
    assert(start =~= Seq::<usize>::empty());
    assert forall|id:usize| id<before.len() implies #[trigger] start.to_multiset().count(id)==0 by {};
}

pub(crate) proof fn count_edge(before:Seq<usize>,row:Seq<usize>,j:int,prior:Seq<usize>,next:Seq<usize>)
    requires 0<=j<row.len(),row[j]<prior.len(),partial_counts(before,row,j,prior),
        prior[row[j] as int]<usize::MAX,next==prior.update(row[j] as int,(prior[row[j] as int]+1) as usize),
    ensures partial_counts(before,row,j+1,next),
{
    row.lemma_take_succ_push(j);
}

pub(crate) proof fn finish_row_count(rows:Seq<Vec<usize>>,i:int,before:Seq<usize>,now:Seq<usize>,n:nat)
    requires 0<=i<rows.len(),counted(rows,i,before,n),partial_counts(before,rows[i]@,rows[i]@.len() as int,now),
    ensures counted(rows,i+1,now,n),
{
    reveal(candidate_edges);
    assert(rows.take(i+1).drop_last() =~= rows.take(i));
    let row=rows[i]@;
    assert(row.take(row.len() as int) =~= row);
}

// The readable, hash-level selector contract uses no dense rows or counts.
pub(crate) open spec fn raw_supplies<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>>(items:Seq<T>,view:F,choice:Set<usize>,hash:H)->bool {
    exists|f:usize| f<items.len() && choice.contains(f) && #[trigger] model(view,&items[f as int]).members@.contains(hash)
}

pub(crate) open spec fn raw_closed<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>,A:Fn(H)->bool>(items:Seq<T>,view:F,accepted:A,choice:Set<usize>)->bool {
    &&& forall|f:usize| #[trigger] choice.contains(f) ==> f<items.len()
    &&& forall|f:usize,h:H| #[trigger] choice.contains(f) && #[trigger] model(view,&items[f as int]).boundary@.contains(h)
        ==> model(accepted,h) || raw_supplies(items,view,choice,h)
}

pub(crate) open spec fn greatest<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>,A:Fn(H)->bool>(items:Seq<T>,view:F,accepted:A,excluded:Seq<bool>)->bool {
    let kept=Set::range(0usize,items.len() as usize).filter(|i:usize| !excluded[i as int]);
    &&& excluded.len()==items.len()
    &&& raw_closed(items,view,accepted,kept)
    &&& forall|alternative:Set<usize>| #[trigger] raw_closed(items,view,accepted,alternative) ==> alternative.subset_of(kept)
}

proof fn aligned_supplier<'i,'m,T:'i,H:'m,F:Fn(&'i T)->FragmentView<'m,H>>(
    items:Seq<T>,view:F,hashes:Seq<H>,members:Seq<Vec<usize>>,choice:Set<usize>,id:usize,
)
    requires members.len()==items.len(),members_aligned(items,view,hashes,members),hashes.no_duplicates(),id<hashes.len(),
    ensures supplies(members,choice,id) <==> raw_supplies(items,view,choice,hashes[id as int]),
{
    let h=hashes[id as int];
    if supplies(members,choice,id) {
        let f=choose|f:usize| f<members.len() && choice.contains(f) && #[trigger] members[f as int]@.contains(id);
        let row=members[f as int]@;
        let j=choose|j:int| 0<=j<row.len() && row[j]==id;
        let original=model(view,&items[f as int]).members@;
        assert(row[j]<hashes.len() && hashes[row[j] as int]==original[j]);
        assert(original.contains(h));
    }
    if raw_supplies(items,view,choice,h) {
        let f=choose|f:usize| f<items.len() && choice.contains(f) && #[trigger] model(view,&items[f as int]).members@.contains(h);
        let original=model(view,&items[f as int]).members@;
        let j=choose|j:int| 0<=j<original.len() && original[j]==h;
        let row=members[f as int]@;
        assert(row[j]<hashes.len() && hashes[row[j] as int]==original[j]);
        assert(row[j]==id);
        assert(row.contains(id));
    }
}

proof fn closures_agree<'i,'m,T:'i,H:Eq+Hash+'m,F:Fn(&'i T)->FragmentView<'m,H>,A:Fn(H)->bool>(
    items:Seq<T>,view:F,accepted:A,symbols:&Symbols<H>,members:Seq<Vec<usize>>,required:Seq<Vec<usize>>,choice:Set<usize>,
)
    requires valid(symbols),members.len()==items.len(),members_aligned(items,view,symbols.hashes@,members),
        exact_requirements(items,view,accepted,items.len() as int,0,symbols,required),
    ensures closed(members,required,choice) <==> raw_closed(items,view,accepted,choice),
{
    if raw_closed(items,view,accepted,choice) {
        assert forall|h:usize,f:usize| h<required.len() && choice.contains(f) && #[trigger] required[h as int]@.contains(f)
            implies supplies(members,choice,h) by {
            let raw=symbols.hashes@[h as int];
            assert(symbols.ids@[raw]==h);
            assert(at_requirement(symbols,required,f,raw));
            assert(raw_need(items,view,accepted,f as int,raw));
            assert(raw_supplies(items,view,choice,raw));
            aligned_supplier(items,view,symbols.hashes@,members,choice,h);
        }
        assert(closed(members,required,choice));
    }
    if closed(members,required,choice) {
        assert forall|f:usize,h:H| #[trigger] choice.contains(f) && #[trigger] model(view,&items[f as int]).boundary@.contains(h)
            implies model(accepted,h) || raw_supplies(items,view,choice,h) by {
            if !model(accepted,h) && !model(view,&items[f as int]).members@.contains(h) {
                assert(raw_need(items,view,accepted,f as int,h));
                assert(at_requirement(symbols,required,f,h));
                let id=symbols.ids@[h];
                assert(id<required.len());
                assert(symbols.hashes@[id as int]==h);
                assert(supplies(members,choice,id));
                aligned_supplier(items,view,symbols.hashes@,members,choice,id);
            }
        }
    }
}

pub(crate) proof fn ready_to_exclude<'i,'m,T:'i,H:Eq+Hash+'m,F:Fn(&'i T)->FragmentView<'m,H>,A:Fn(H)->bool>(
    items:Seq<T>,view:F,accepted:A,symbols:&Symbols<H>,members:Seq<Vec<usize>>,required:Seq<Vec<usize>>,counts:Seq<usize>,
)
    requires items.len()<=usize::MAX,symbols.hashes@.len()<=usize::MAX,valid(symbols),members.len()==items.len(),
        members_aligned(items,view,symbols.hashes@,members),exact_requirements(items,view,accepted,items.len() as int,0,symbols,required),
        counted(members,members.len() as int,counts,symbols.hashes@.len()),
    ensures crate::selection_spec::well_formed(members,required,counts),
{
    assert(members.take(members.len() as int) =~= members);
    assert forall|i:int,j:int| 0<=i<members.len() && 0<=j<members[i]@.len() implies #[trigger] members[i]@[j]<counts.len() by {
        assert(aligned_row(model(view,&items[i]).members@,members[i]@,symbols.hashes@));
    }
}

pub(crate) proof fn translate_selection<'i,'m,T:'i,H:Eq+Hash+'m,F:Fn(&'i T)->FragmentView<'m,H>,A:Fn(H)->bool>(
    items:Seq<T>,view:F,accepted:A,symbols:&Symbols<H>,members:Seq<Vec<usize>>,required:Seq<Vec<usize>>,excluded:Seq<bool>,
)
    requires valid(symbols),members.len()==items.len(),members_aligned(items,view,symbols.hashes@,members),items.len()<=usize::MAX,
        exact_requirements(items,view,accepted,items.len() as int,0,symbols,required),biggest(members,required,excluded),
    ensures greatest(items,view,accepted,excluded),
{
    let kept=Set::range(0usize,items.len() as usize).filter(|i:usize| !excluded[i as int]);
    assert(closed(members,required,kept));
    closures_agree(items,view,accepted,symbols,members,required,kept);
    assert forall|alternative:Set<usize>| #[trigger] raw_closed(items,view,accepted,alternative) implies alternative.subset_of(kept) by {
        closures_agree(items,view,accepted,symbols,members,required,alternative);
        assert(closed(members,required,alternative));
    }
}

} // verus!
