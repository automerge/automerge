/**
 * # Automerge
 *
 * This library provides the core automerge data structure and sync algorithms.
 * Other libraries can be built on top of this one which provide IO and
 * persistence.
 *
 * An automerge document can be thought of an immutable POJO (plain old javascript
 * object) which `automerge` tracks the history of, allowing it to be merged with
 * any other automerge document.
 *
 * ## Creating and modifying a document
 *
 * You can create a document with {@link init} or {@link from} and then make
 * changes to it with {@link change}, you can merge two documents with {@link
 * merge}.
 *
 * ```ts
 * import * as automerge from "@automerge/automerge"
 *
 * type DocType = {ideas: Array<automerge.Text>}
 *
 * let doc1 = automerge.init<DocType>()
 * doc1 = automerge.change(doc1, d => {
 *     d.ideas = [new automerge.Text("an immutable document")]
 * })
 *
 * let doc2 = automerge.init<DocType>()
 * doc2 = automerge.merge(doc2, automerge.clone(doc1))
 * doc2 = automerge.change<DocType>(doc2, d => {
 *     d.ideas.push(new automerge.Text("which records its history"))
 * })
 *
 * // Note the `automerge.clone` call, see the "cloning" section of this readme for
 * // more detail
 * doc1 = automerge.merge(doc1, automerge.clone(doc2))
 * doc1 = automerge.change(doc1, d => {
 *     d.ideas[0].deleteAt(13, 8)
 *     d.ideas[0].insertAt(13, "object")
 * })
 *
 * let doc3 = automerge.merge(doc1, doc2)
 * // doc3 is now {ideas: ["an immutable object", "which records its history"]}
 * ```
 *
 * ## Applying changes from another document
 *
 * You can get a representation of the result of the last {@link change} you made
 * to a document with {@link getLastLocalChange} and you can apply that change to
 * another document using {@link applyChanges}.
 *
 * If you need to get just the changes which are in one document but not in another
 * you can use {@link getHeads} to get the heads of the document without the
 * changes and then {@link getMissingDeps}, passing the result of {@link getHeads}
 * on the document with the changes.
 *
 * ## Saving and loading documents
 *
 * You can {@link save} a document to generate a compresed binary representation of
 * the document which can be loaded with {@link load}. If you have a document which
 * you have recently made changes to you can generate recent changes with {@link
 * saveIncremental}, this will generate all the changes since you last called
 * `saveIncremental`, the changes generated can be applied to another document with
 * {@link loadIncremental}.
 *
 * ## Viewing different versions of a document
 *
 * Occasionally you may wish to explicitly step to a different point in a document
 * history. One common reason to do this is if you need to obtain a set of changes
 * which take the document from one state to another in order to send those changes
 * to another peer (or to save them somewhere). You can use {@link view} to do this.
 *
 * ```ts
 * import * as automerge from "@automerge/automerge"
 * import * as assert from "assert"
 *
 * let doc = automerge.from({
 *   key1: "value1",
 * })
 *
 * // Make a clone of the document at this point, maybe this is actually on another
 * // peer.
 * let doc2 = automerge.clone < any > doc
 *
 * let heads = automerge.getHeads(doc)
 *
 * doc =
 *   automerge.change <
 *   any >
 *   (doc,
 *   d => {
 *     d.key2 = "value2"
 *   })
 *
 * doc =
 *   automerge.change <
 *   any >
 *   (doc,
 *   d => {
 *     d.key3 = "value3"
 *   })
 *
 * // At this point we've generated two separate changes, now we want to send
 * // just those changes to someone else
 *
 * // view is a cheap reference based copy of a document at a given set of heads
 * let before = automerge.view(doc, heads)
 *
 * // This view doesn't show the last two changes in the document state
 * assert.deepEqual(before, {
 *   key1: "value1",
 * })
 *
 * // Get the changes to send to doc2
 * let changes = automerge.getChanges(before, doc)
 *
 * // Apply the changes at doc2
 * doc2 = automerge.applyChanges < any > (doc2, changes)[0]
 * assert.deepEqual(doc2, {
 *   key1: "value1",
 *   key2: "value2",
 *   key3: "value3",
 * })
 * ```
 *
 * If you have a {@link view} of a document which you want to make changes to you
 * can {@link clone} the viewed document.
 *
 * ## Syncing
 *
 * The sync protocol is stateful. This means that we start by creating a {@link
 * SyncState} for each peer we are communicating with using {@link initSyncState}.
 * Then we generate a message to send to the peer by calling {@link
 * generateSyncMessage}. When we receive a message from the peer we call {@link
 * receiveSyncMessage}. Here's a simple example of a loop which just keeps two
 * peers in sync.
 *
 * ```ts
 * let sync1 = automerge.initSyncState()
 * let msg: Uint8Array | null
 * ;[sync1, msg] = automerge.generateSyncMessage(doc1, sync1)
 *
 * while (true) {
 *   if (msg != null) {
 *     network.send(msg)
 *   }
 *   let resp: Uint8Array =
 *     (network.receive()[(doc1, sync1, _ignore)] =
 *     automerge.receiveSyncMessage(doc1, sync1, resp)[(sync1, msg)] =
 *       automerge.generateSyncMessage(doc1, sync1))
 * }
 * ```
 *
 * ## Conflicts
 *
 * The only time conflicts occur in automerge documents is in concurrent
 * assignments to the same key in an object. In this case automerge
 * deterministically chooses an arbitrary value to present to the application but
 * you can examine the conflicts using {@link getConflicts}.
 *
 * ```
 * import * as automerge from "@automerge/automerge"
 *
 * type Profile = {
 *     pets: Array<{name: string, type: string}>
 * }
 *
 * let doc1 = automerge.init<Profile>("aaaa")
 * doc1 = automerge.change(doc1, d => {
 *     d.pets = [{name: "Lassie", type: "dog"}]
 * })
 * let doc2 = automerge.init<Profile>("bbbb")
 * doc2 = automerge.merge(doc2, automerge.clone(doc1))
 *
 * doc2 = automerge.change(doc2, d => {
 *     d.pets[0].name = "Beethoven"
 * })
 *
 * doc1 = automerge.change(doc1, d => {
 *     d.pets[0].name = "Babe"
 * })
 *
 * const doc3 = automerge.merge(doc1, doc2)
 *
 * // Note that here we pass `doc3.pets`, not `doc3`
 * let conflicts = automerge.getConflicts(doc3.pets[0], "name")
 *
 * // The two conflicting values are the keys of the conflicts object
 * assert.deepEqual(Object.values(conflicts), ["Babe", "Beethoven"])
 * ```
 *
 * ## Actor IDs
 *
 * By default automerge will generate a random actor ID for you, but most methods
 * for creating a document allow you to set the actor ID. You can get the actor ID
 * associated with the document by calling {@link getActorId}. Actor IDs must not
 * be used in concurrent threads of executiong - all changes by a given actor ID
 * are expected to be sequential.
 *
 * ## Listening to patches
 *
 * Sometimes you want to respond to changes made to an automerge document. In this
 * case you can use the {@link PatchCallback} type to receive notifications when
 * changes have been made.
 *
 * ## Cloning
 *
 * Currently you cannot make mutating changes (i.e. call {@link change}) to a
 * document which you have two pointers to. For example, in this code:
 *
 * ```javascript
 * let doc1 = automerge.init()
 * let doc2 = automerge.change(doc1, d => (d.key = "value"))
 * ```
 *
 * `doc1` and `doc2` are both pointers to the same state. Any attempt to call
 * mutating methods on `doc1` will now result in an error like
 *
 *     Attempting to change an out of date document
 *
 * If you encounter this you need to clone the original document, the above sample
 * would work as:
 *
 * ```javascript
 * let doc1 = automerge.init()
 * let doc2 = automerge.change(automerge.clone(doc1), d => (d.key = "value"))
 * ```
 */

export * from "./implementation.js"
export * as next from "./implementation.js"
