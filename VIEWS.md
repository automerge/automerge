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

let watermark = view.apply_patch(&opest_patch);
```
