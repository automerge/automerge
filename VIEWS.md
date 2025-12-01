# Views

## Introduction

Currently, Automerge documents can take a large amount of compute time to read
and modify, even if the "current" state of the document is quite small. This
means that for applications which want to remain responsive the only choice is
to start adding background processes and threads which queue up changes to the
document and so on. This is kind of defeating the purpose of Automerge, which is
to remove all of the complexity of working with a distributed system. This
document describes a strategy for handling this problem based on creating
lightweight "views" of a document which have predictable performance
characteristics and deferring compute intensive work.

## Performance Problems

What exactly are the performance problems I'm talking about? Well, there are
roughly these categories of operation applications want to perform on Automerge
documents:

* Loading a document
* Reading the current value of the document
* Writing some new data into the document
* Generating or receiving sync messages
* Examining the history of a document (typically by generating diffs)

In all of these cases the larger the history of the document, the longer the
operation takes. Now, for some cases that isn't necessarily a problem,
responding to network messages or loading a document can be done in background
processes - to a lesser extent examining the history falls into this category as
well. However, reading and writing values is something that we really want to be
able to do on the UI thread. The reason is that if we can't perform these
operations within a frame we force developers to construct a distributed system
inside their application which handles the divergence of state between the
optimistic update of the UI and the state in the Automerge document. This is an
absurd thing to need to do when we already have a CRDT, concurrent updates are
the point!

## The structure of the Automerge library

Why do these performance problems arise? To understand it's worth understanding
in a little more detail the struture of an automerge document conceptually and
concretely. Automerge documents are, in abstract terms, a DAG of commits similar
to Git. Each commit holds a sequence of "operations" which are individual
mutations to the document and unlike Git, it is entirely valid to have multiple
heads of the graph. Under the hood though, we do not use the commit graph
representation directly, instead we split the representation of the document
into the "changegraph" and the "opset". Our focus here is on the OpSet, which is
where most of the performance problems I am interested in here arise.

First though, we need to understand a little bit about how we represent
operation identity in Automerge. Each operation in the document has an "actor
ID" and a counter - i.e. a lamport timestamp. Actor IDs only ever issue
sequential updates (i.e. an actor ID is a serial process ID). Each commit is
identified by a hash. This means that for any commit we perform a topological
traversal of the commit graph up to that commit and form a vector clock
consisting of the combined lamport timestamps of all operations up to that
point. This clock can then be used to determine what other operations in the
document occurred concurrently or causally after the given commit.

## The OpSet

The purpose of the opset is to organise operations in such a way that a) it is
relatively quick to read and write operations and b) we can take advatnage of
run length encoding to compress op metadata (i.e. the lamport timestamps). To
achieve this we organise the individual operations of the whole document into
"document order" in a structure called the "OpSet". The OpSet is a B-Tree of
operations in this order:

* Object ID (the lamport timestamp of the operations which created an object)
* Key within the object (either the lexicographic order of keys in a map, or the
  causal order of operation IDs in the RGA sequence for lists)
* Causal order of operations for the same key

This structure is very convenient, we can produce diffs in a single pass over it
by computing a vector clock for the before and after views of the document and
filtering ops by those that are (in)visible according to the two clocks. The
downside to this structure though is that any time we modify the document we
have to do two things:

* Figure out where in the B-Tree the new operation should go
* Actually insert the operation in the tree

This is an operation that grows with the size of the history of the document,
which makes it hard for users to manage as histories never shrink. For example,
if we want to insert an item at index 10 in a list, we first have to seek
through the Opset to the list, then through the operations under that list to
the 10th visible item. 

The OpSet is crucial for handling conurrent changes, but it is also always going
to require an amount of work proportional to the size of the history to work
with. This is not compatible with a "next frame or your money back" guarantee
where we can never really do any unbounded work - or at least, we would like to
do work bounded by the size of the materialized view of the document, not the
history.

## Ideal Performance

Being bounded by the size of the materialized view is actually an interesting
idea. Part of the problem with the performance being sensitive to history is
that it is unexpected by, and out of the control of, the application developer.
Developers typically expect to reason about the data structures that the
application is producing, not some giant hidden history. Let's compare what the
developer expects to happen in a common scenario to what Automerge has to do.

Imagine we are inserting an item at index `1` in the "contacts" list in this document:

```json
{
  "contacts": [
    { name: "bob", email: "robobob@ob.com" }
  ]
}
```

What a developer would _expect_ to happen here is that the "contacts" list is
either an array or a list and the insertion at the end is proportional to the
size of the list (maybe amortized in the case of an array but still). But now
imagine that this document is actually an Automerge document and, unbeknownst to
the developer, there are 10,000,000 commits in this document which successively
create and remove items from the "contacts" array. What Automerge does now is
something like this:

* Seek to the "contacts" key of the root object ID in the OpSet, lookup the
  object ID
* Now seek to the object ID we just found
* Now seek through all the ops in the contacts list filtering out those which
  are not visible and counting the visible items until we find th zeroth visible
  item

There are various caches and optimizations we perform to make this fast in many
cases, but inevitably the work done in the latter case is generally much more
than an application developer might expect and/or be able to plan for. We
deliberately provide no mechanisms for developers to remove history or otherwise
control it. 

To me this points the direction to a reasonable strategy for solving the
problem. Instead of directly reading and mutating the OpSet, we provide an API
whereby the developer can create a "view". A view is the state of the document
as at some point in the commit graph (typically the heads) along with the
minimal amount of information needed to create new commits. When the view is
mutated it creates a commit and appends it to a local queue, and updates its
local state. The queue of commits can be retrieved and applied to the OpSet at
leisure (e.g. when the UI loop is idle, or on a background thread).

There are some details here to work out though, let's dive into them.

## Views

Here are some questions which arise when we start imagining this split:

* What is the minimal information required to create a new commit?
* How are remote changes applied to the view?

### Minimal State

To create a new commit we need:

* The dependencies of the commit
* The actor ID we are operating as
* The sequence number of the last commit this actor produced
* The maximum operation counter we have seen from _any_ actor
* Every op refers to a key within some object so we need:
  * The object ID of each object
  * The "key", which is:
    * Just a string for maps
    * The operation ID which inserted element preceding the insertion point (the
      "reference element") _or_ an all zeros op ID representing the head of the
      list
* For operations which are deleting or overwriting a value we directly reference
  the op ID we are overwriting, so we need that op ID

The materialized view of an Automerge document is similar to a JSON object with
a few more types. This means that what we need is basically this structure along
with the object ID of each object (map, list, or text) and the op ID of each
field in each object.

### How are remote changes applied to the view?

Applying remote changes relies on information in the OpSet to resolve concurrent
modifications. An example will probably make this clearer. Imagine the view has
this state:

```json
{
  "contacts": [
    { name: "Alice"},
    { name: "Bob"}
  ]
}
```

Then, a local change is made which deletes "bob" and inserts a new record at the
end of the list, so we have this state:

```json
{
  "contacts": [
    { name: "Alice"},
    { name: "Charlie"}
  ]
}
```

Now suppose a concurrent modification inserts `{name: "Derek"}` at index `1`. We
need to determine how to update the view with this new operation. In the OpSet
this is achieved by finding the reference element (the operation ID) which the
insertions occurs after and then inserting the new operation in causal order
in the operations which follow the reference element. Thus, everyone agrees
on the order of all elements.

It's not obvious that we can use this strategy in the view. The view state 
doesn't have the reference element ("bob") any more because it was deleted
locally.

One strategy would be to generate patches for the view from the OpSet. I think a
rough version of this in a single threaded dont-block-the-ui-loop environment
would look like this:

* Apply local mutations to the view
* When the UI loop is idle:
  * Apply remote mutations to the OpSet
  * Apply queued local mutations to the OpSet
  * Generate a patch from the heads of the OpSet to the heads of the view
  * Apply the patch to the view 
  
What is maybe not obvious here is that the first two steps of "when the UI loop
is idle" could in principle take a huge amount of time (if there are lots of 
local or remote changes) and so if we want to stay responsive on the UI loop
we may actually never reach the "generate a patch" step before we have to
yield to the UI loop again. For example, if we have many users sketching in
a drawing application they may all be producing changes too fast to merge,
maybe we never get to show any collaborative changes until they've all 
stopped. 

One way of seeing this problem is that the view might always be "ahead" of
the OpSet. Local changes may be being produced so fast, or there may be
so many remote changes to apply, that there is never a point where the
queue of local changes to apply is empty _and_ the OpSet is not busy.
The reason this is a problem is because the local view forgets things
which are deleted, and so there is no way for the OpSet to emit some
kind of data structure which describes how to incorporate concurrent
changes into the view.

There is an asymmetry here which is interesting. The OpSet never drops
anything and so it always has the information it needs to apply the 
changes from the view, the view on the other hand forgets information
and so it cannot incorporate arbitrary changes from the OpSet. From
the perspective of the OpSet the view could be some arbitrary number
of changes removed from the OpSet state and so there's no way to
describe the changes which should be made to the view. This is what
leads to the need to wait for both the view and the OpSet to be idle,
because only then can the OpSet know the current state of the view
and thus what patches should be applied to the view to incorporate
concurrent changes.

What we want then is a way for the OpSet to know the state of the
view without coordination. We can achieve this by having the view
_not_ delete information until some kind of confirmation has 
been received from the OpSet. The logic of the whole system then
becomes something like this:

* Apply local mutations to the view
  * These mutations add commits to the queue to apply, but do not delete
    superceded information
* Apply remote changes to the OpSet
* When the UI loop is idle
  * Apply remote changes to the OpSet
  * Apply the local changes to the remote
  * At the OpSet, generate a patch based on the last observed heads of the view
    * This patch contains some new heads which tell the view what it can update
      it's local heads to. I.e. the heads all following patches will be based 
      on. Thus the local view can discard information older than these heads.
  
The critical part here is that the final step can be done at any time (i.e.
before applying remote or local changes). Because the view ensures that it
retains all information needed to apply changes since the last flushed heads
this means the OpSet can just generate patches when it is idle and the 
view can apply patches when it is idle. As long as neither side is entirely
consumed with local/remote changes, we will make progress.

What data structure should the view use? Well at this point we need something
that looks more or less exactly like an OpSet, except that old information is
discarded. We can also likely reuse many components of the existing sync
protocol.

## The API

At this point we have a conceptual design for how we can achieve
next-frame-or-your-money-back. To ensure we are grounded in reality we now turn
to the question of what the API should look like. We'll start with the JavaScript
API, then drop down to the Rust API which would sit behind it.

### JavaScript API

One principle we should follow with the JavaScript API is the for users for
whom this kind of performance is uninteresting (perhaps they typically deal 
with small histories, perhaps they are not latency sensitive) then the existing
API should remain the same. That API being something like this:

```typescript
import * as Automerge from "@automerge/automerge"

let doc: Automerge.Doc<{text: string}> = Automerge.from({text: "hello world"})
doc = Automerge.change(doc, d => {
  d.text = "hello again world"
})

let msg: Uint8Array = receiveFromNetwork()
doc = Automerge.applySyncMessage(msg)
```

I.e. the document is a locally mutable thing which blocks on access to the 
OpSet. For users who _are_ latency sensitive we can expose an API which
allows them to schedule work. Something like this:

```typescript
let doc: Automerge.Doc<{text: string}> = Automerge.from(
  {text: "hello world"},
  {writeMode: "deferred"} // This option tells Automerge to create a view internally
)

// This call mutates the view and enqueus a local commit, but does not 
// modify the OpSet
doc = Automerge.change(doc, d => {
  d.text = "hello again world"
})

// When the UI loop is idle
const start = performance.now()
while (true) {
  doc = Automerge.applyDeferred(doc) // This does work in batches, so that the call does not block
  if (performance.now() - start > 100) {
    // yield to the UI loop 
    return doc
  }
}
```

The idea here is that in the non-deferrred mode, all calls flush immediately. In deferred mode
methods which _require_ a flush to be completed (such as a diff) would throw an exception. 
Libraries like `automerge-repo` can then use this API to implement APIs that are asynchronous
in these deferred modes.

### Rust API

The Rust API is much more low level and also less mature, so we can break compatibility a lot
more. I think we would want an explicit "view" type here.

```rust
let mut doc = Automerge::new();

// Make a bunch of changes to the document
update_doc(&mut doc);

let mut view = doc.create_view(&[]) // The argument is the heads to create the view at
// This state includes the actor and counter of the latest change of the view so the opset knows what the view still has
let watermark = view.watermark(); 

// Note that View implements Transactable, so it can be passed in the same place as a doc
update_doc(&mut view);

// Now flush the changes to the original doc
let changes = view.pending_changes().to_vec();

// Apply local changes and get a patch to send to the view
let opset_patch = doc.apply_pending_changes(&changes);

// Apply the opset patch
let watermark = view.apply_patch(&opset_patch);

// Receive remote changes
receive_remote_changes(&mut doc);

let opset_patch = doc.view_patch(&watermark);

let watermark = view.apply_patch(&opset_patch);
```

## The View's Internal Structure

We've established that the view needs to retain information until the OpSet confirms
it has been incorporated. But what exactly should the view's internal data structure
look like? One tempting approach would be to design a new, simpler data structure
optimized for the view's specific needs. However, there's a much better option: use
the OpSet itself.

This might seem counterintuitive at first. Wasn't the whole point of views to avoid
the performance problems of the OpSet? The key insight is that the OpSet's performance
problems stem from the _size_ of the history it contains, not from the data structure
itself. A view's OpSet would contain only the operations necessary to represent the
current materialized state plus any pending changes not yet confirmed by the source
document. This is a much smaller set of operations than the full history.

Using the same data structure has significant advantages. First, it means we can reuse
all the existing machinery for reading and writing documents. The view can implement
`Transactable` by delegating to its internal OpSet, which already knows how to handle
all the edge cases around concurrent modifications, list ordering, and so on. Second,
it means the logic for applying patches and generating diffs is already written and
tested. Third, it dramatically simplifies the codebase because we don't have two
parallel implementations of document semantics that could drift apart.

### Creating a View

If the view contains an OpSet, how do we create one? The answer is straightforward:
we clone the source document's OpSet and then garbage collect it down to the current
heads. The source OpSet contains the full history, but we only need the operations
that contribute to the current visible state. This "GC to heads" operation removes
all the historical cruft while preserving exactly the information needed to read
the current state and create new operations.

After this cloning and garbage collection, the view's OpSet is much smaller than the
source. For a document with millions of historical operations but only a few thousand
visible values, the view might be several orders of magnitude smaller. This is where
the performance win comes from: not from a different data structure, but from
operating on a much smaller instance of the same data structure.

### Patches as OpSet Deltas

Now we come to a crucial question: what should patches look like? When the source
OpSet needs to send updates to a view, what form should those updates take?

One approach would be to send patches in terms of the materialized state, something
like "set key 'name' to 'Alice'" or "insert 'foo' at index 3". But this approach has
problems. The view needs to maintain its OpSet in a consistent state, which means it
needs to know the operation IDs, reference elements, and causal relationships of
the operations it contains. A high-level patch doesn't provide this information.

A better approach is to send patches as deltas to the OpSet itself. Rather than
describing changes to the materialized state, we describe changes to the set of
operations. A patch says "here are the new operations to insert into your OpSet"
and the view inserts them in the appropriate positions according to document order.
Once the operations are inserted, the view can use the existing diff machinery to
determine what changed in the materialized state.

This approach has a pleasing symmetry. The view sends operations to the source OpSet
(the pending changes), and the source OpSet sends operations back to the view (the
patches). Both directions use the same currency: operations.

### The Watermark

The view and source OpSet need to coordinate so that the source knows which operations
the view already has. This is the purpose of the watermark. The watermark is essentially
a clock, a vector of counters indicating the latest operation from each actor that
the view has incorporated into its OpSet.

When the source OpSet generates a patch for a view, it looks at the view's watermark
and includes only operations that are newer than that watermark. When the view applies
a patch, it updates its watermark to reflect the new operations it has received. This
way, each patch contains only the delta since the last patch, and no operation is
sent twice.

The watermark also serves another purpose: it tells the view what it can safely forget.
Operations older than the watermark have been incorporated into the source OpSet,
which means the source has all the information it needs. The view can garbage collect
operations that are both older than the watermark and no longer needed for the current
materialized state.

## Patch Scenarios

To make this concrete, let's walk through several scenarios showing how patches flow
between the source OpSet and a view. In each case we'll see what operations exist,
what patch is generated, and how the view's state changes.

### Simple Map Update

Consider a document with a single key:

```json
{"name": "Alice"}
```

The view's OpSet contains one operation: `Op(A, 1, Put, root, "name", "Alice")`. Now
suppose a remote actor updates the name. The source OpSet receives and incorporates
`Op(B, 5, Put, root, "name", "Bob", pred=[A:1])`. This operation supersedes the
original because it lists `A:1` in its predecessors.

The source generates a patch for the view containing this single operation. The view
inserts it into its OpSet at the appropriate position under the root object's "name"
key. Now the view's OpSet has both operations for that key, with `B:5` superseding
`A:1`. The existing diff machinery determines that the materialized value changed
from "Alice" to "Bob".

### Concurrent Map Updates

Now suppose instead that while the remote actor was setting the name to "Bob", the
local user was setting it to "Carol" through the view. The view creates
`Op(V, 1, Put, root, "name", "Carol", pred=[A:1])` and queues it as a pending change.
The view's OpSet already contains this operation because local changes are applied
immediately to the view.

When the source OpSet processes both the remote change and the pending local change,
it ends up with three operations for the "name" key: the original `A:1`, the remote
`B:5`, and the local `V:1`. Both `B:5` and `V:1` have `pred=[A:1]`, indicating they
were concurrent modifications.

The patch sent to the view contains only `B:5` because the view already has `A:1`
and `V:1`. After applying the patch, the view's OpSet matches the source's for this
key. The diff machinery recognizes this as a conflict and reports whichever value
wins according to the deterministic conflict resolution rules, along with a flag
indicating a conflict exists.

### List Insertion

Lists are more interesting because operations reference other operations to determine
their position. Consider a list `["X", "Y", "Z"]` represented by these operations:

```
Op(A, 1, MakeList, root, "items") -> creates list object @A:1
Op(A, 2, Insert, @A:1, HEAD, "X")
Op(A, 3, Insert, @A:1, @A:2, "Y")
Op(A, 4, Insert, @A:1, @A:3, "Z")
```

Each insert operation specifies a reference element: the operation ID of the element
it should follow. `A:2` follows HEAD (the beginning of the list), `A:3` follows `A:2`,
and `A:4` follows `A:3`.

Now suppose a remote actor inserts "W" between "X" and "Y". They create
`Op(B, 7, Insert, @A:1, @A:2, "W")`, which references `A:2` (the "X" element) as
its predecessor. The patch contains this operation, and the view inserts it into
its OpSet. The document order places `B:7` after `A:2` but before `A:3` because
that's where operations referencing `A:2` belong. The resulting list is
`["X", "W", "Y", "Z"]`.

### Concurrent List Insertions

What happens when two actors insert at the same position concurrently? Suppose both
the view and a remote actor insert after "X". The view creates
`Op(V, 1, Insert, @A:1, @A:2, "Local")` and the remote creates
`Op(R, 5, Insert, @A:1, @A:2, "Remote")`. Both reference the same predecessor.

The OpSet handles this by sorting concurrent operations with the same reference
element by their operation IDs. This is deterministic: everyone agrees on the order
regardless of when they received the operations. If `R:5` sorts before `V:1`, the
list becomes `["X", "Remote", "Local", "Y", "Z"]`. The patch to the view contains
only `R:5`, and after applying it, the view's diff machinery reports that "Remote"
was inserted at index 1, shifting "Local" to index 2.

### Deletion and Concurrent Insertion

This scenario illustrates why tombstone retention matters. The view has `["X", "Y", "Z"]`
and locally deletes "Y" by creating `Op(V, 1, Delete, @A:1, @A:3)`. The view's OpSet
still contains the insert operation for "Y" (`A:3`) but now also has a delete
operation targeting it. The materialized list shows `["X", "Z"]`.

Meanwhile, a remote actor who hasn't seen the deletion inserts "W" after "Y":
`Op(R, 5, Insert, @A:1, @A:3, "W")`. This operation references `A:3`, which is the
"Y" that the view has locally deleted.

When the patch arrives containing `R:5`, the view can still process it correctly
because it retained the tombstone for "Y". The operation `A:3` is still in the
view's OpSet, just marked as deleted. The new operation `R:5` is inserted after
`A:3` in document order. The resulting list is `["X", "W", "Z"]` because "W" comes
after the deleted "Y" position but before "Z".

If the view had eagerly garbage collected the "Y" operation when it was deleted,
it wouldn't know where to place "W". This is why views must retain tombstones
until the source OpSet confirms it has processed all operations that might
reference them.

### Counter Increments

Counters deserve special mention because they merge differently than other values.
A counter with value 5 might be represented as `Op(A, 1, Put, root, "count", Counter(5))`.
When actors increment it concurrently, they create increment operations:
`Op(V, 1, Inc, root, "count", 2)` and `Op(R, 7, Inc, root, "count", 3)`.

Unlike puts where concurrent operations conflict, increment operations combine
additively. The final counter value is 5 + 2 + 3 = 10. The patch to the view
contains the remote increment operation, and the diff reports the counter increased
by 3 (the remote delta). The view doesn't need to know the final value because
it can compute it from the operations in its OpSet.

## Garbage Collection

The view's OpSet grows as patches arrive and local changes are made. Without garbage
collection, it would eventually become as large as the source OpSet, defeating the
purpose of views. We need a strategy for removing operations that are no longer needed.

### What Can Be Collected

For map keys, garbage collection is straightforward. When a key has multiple operations
and one clearly supersedes the others (it lists them in its predecessors and there are
no concurrent operations), the superseded operations can be removed. Only the winning
operation, or operations in the case of an unresolved conflict, needs to be retained.

For lists, the situation is more nuanced. Deleted elements cannot be immediately
garbage collected because other operations might reference them. In the scenario above,
if we garbage collected the "Y" operation after deleting it, we wouldn't be able to
process the concurrent "W" insertion that referenced "Y".

### Conservative Tombstone Retention

The simplest correct approach is to retain all tombstones, which are list element
operations that have been deleted. This ensures that any future operation referencing
a deleted element can still be placed correctly. The trade-off is that the view's
OpSet may contain more operations than strictly necessary.

In practice, this trade-off is acceptable for most applications. The pathological case
would be a list with maximally interleaved insertions and deletions, where elements
are repeatedly inserted and then deleted such that every remaining element references
a deleted predecessor. This pattern is unusual in real applications. More common
patterns like appending to lists, editing text sequentially, or updating map keys
don't generate long chains of tombstones.

If tombstone accumulation becomes a problem for specific use cases, more aggressive
garbage collection strategies are possible. One could rewrite reference elements to
skip over tombstones, or periodically rebuild the view from scratch by re-cloning
and garbage collecting the source OpSet. But starting with conservative tombstone
retention keeps the implementation simple and handles the common cases well.

### When to Collect

Garbage collection happens when the watermark advances. The watermark represents
the point up to which the source OpSet has incorporated the view's changes. Once
the watermark advances past an operation, the view knows that:

1. The source OpSet has seen this operation
2. Any future patches will be based on state that includes this operation
3. No future patch will contain operations that are concurrent with this operation

At this point, superseded map operations can be collected. Tombstones are retained
per the conservative strategy, but operations that are strictly superseded, with
no possibility of concurrent operations arriving, can be removed.

The view might also periodically perform compaction by regenerating itself from
the source OpSet. This is equivalent to creating a fresh view: clone the source
OpSet and garbage collect to the current heads. This eliminates any accumulated
tombstones and resets the view to minimal size. Applications can trigger compaction
during idle periods or when the view's size exceeds some threshold.

## Multiple Views

A single source document can have multiple views, each with its own OpSet and
watermark. This is useful when different parts of an application need to read and
write to the document independently. For example, a document editor might have one
view for the main editing surface and another for a sidebar that shows metadata.

Each view operates independently. It receives patches from the source OpSet based
on its own watermark, and it sends its pending changes to the source OpSet
independently. The source OpSet doesn't need to know how many views exist or
coordinate between them. It simply responds to patch requests based on the
watermark it receives.

When one view's pending changes are applied to the source OpSet, other views learn
about those changes through the normal patch mechanism. From view B's perspective,
changes made by view A look like any other remote changes: they arrive in a patch
from the source OpSet and are incorporated into view B's OpSet.

This design keeps the views loosely coupled. They don't need to communicate with
each other directly, and the source OpSet doesn't need special logic to handle
multiple views. The watermark mechanism ensures each view receives exactly the
operations it needs, regardless of what other views are doing.
