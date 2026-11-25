use vstd::prelude::*;

verus! {

pub(crate) open spec fn author_value<A>(entry:Option<Option<A>>) -> Option<A> {
    match entry { Some(author)=>author, None=>None }
}

} // verus!
