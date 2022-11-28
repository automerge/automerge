## Automerge WASM Low Level Interface

This package is a low level interface to the [automerge rust](https://github.com/automerge/automerge-rs/tree/experiment) CRDT.  The api is intended to be as "close to the metal" as possible with only a few ease of use accommodations.  This library is used as the underpinnings for the [Automerge JS wrapper](https://github.com/automerge/automerge-rs/tree/experiment/automerge-js) and can be used as is or as a basis for another higher level expression of a CRDT.

All example code can be found in `test/readme.ts`

### Why CRDT?

CRDT stands for Conflict Free Replicated Data Type.  It is a data structure that offers eventual consistency where multiple actors can write to the document independently and then these edits can be automatically merged together into a coherent document that, as much as possible, preserves the intent of the different writers.  This allows for novel masterless application design where different components need not have a central coordinating server when altering application state.

### Terminology

The term Actor, Object Id and Heads are used through this documentation.  Detailed explanations are in the glossary at the end of this readme.  But the most basic definition would be...

An Actor is a unique id that distinguishes a single writer to a document.  It can be any hex string.

An Object id uniquely identifies a Map, List or Text object within a document.  It can be treated as an opaque string and can be used across documents.  This id comes as a string in the form of `{number}@{actor}` - so `"10@aabbcc"` for example.  The string `"_root"` or `"/"` can also be used to refer to the document root.  These strings are durable and can be used on any descendant or copy of the document that generated them.

Heads refers to a set of hashes that uniquely identifies a point in time in a document's history.  Heads are useful for comparing documents state or retrieving past states from the document.

### Automerge Scalar Types

Automerge has many scalar types.  Methods like `put()` and `insert()` take an optional data type parameter.  Normally the type can be inferred but in some cases, such as telling the difference between int, uint and a counter, it cannot.

These are puts without a data type

```javascript
  import { create } from "@automerge/automerge-wasm"

  let doc = create()
  doc.put("/", "prop1", 100)  // int
  doc.put("/", "prop2", 3.14) // f64
  doc.put("/", "prop3", "hello world")
  doc.put("/", "prop4", new Date())
  doc.put("/", "prop5", new Uint8Array([1,2,3]))
  doc.put("/", "prop6", true)
  doc.put("/", "prop7", null)
```

Put's with a data type and examples of all the supported data types.

While int vs uint vs f64 matters little in javascript, Automerge is a cross platform library where these distinctions matter.

```javascript
  import { create } from "@automerge/automerge-wasm"

  let doc = create()
  doc.put("/", "prop1", 100, "int")
  doc.put("/", "prop2", 100, "uint")
  doc.put("/", "prop3", 100.5, "f64")
  doc.put("/", "prop4", 100, "counter")
  doc.put("/", "prop5", 1647531707301, "timestamp")
  doc.put("/", "prop6", new Date(), "timestamp")
  doc.put("/", "prop7", "hello world", "str")
  doc.put("/", "prop8", new Uint8Array([1,2,3]), "bytes")
  doc.put("/", "prop9", true, "boolean")
  doc.put("/", "prop10", null, "null")
```

### Automerge Object Types

Automerge WASM supports 3 object types.  Maps, lists, and text.  Maps are key value stores where the values can be any scalar type or any object type.  Lists are numerically indexed sets of data that can hold any scalar or any object type.

```javascript
  import { create } from "@automerge/automerge-wasm"

  let doc = create()

  // you can create an object by passing in the inital state - if blank pass in `{}`
  // the return value is the Object Id
  // these functions all return an object id

  let config = doc.putObject("/", "config", { align: "left", archived: false, cycles: [10, 19, 21] })
  let token = doc.putObject("/", "tokens", {})

  // lists can be made with javascript arrays

  let birds = doc.putObject("/", "birds", ["bluejay", "penguin", "puffin"])
  let bots = doc.putObject("/", "bots", [])

  // text is initialized with a string

  let notes = doc.putObject("/", "notes", "Hello world!")
```

You can access objects by passing the object id as the first parameter for a call.

```javascript
  import { create } from "@automerge/automerge-wasm"

  let doc = create()

  let config = doc.putObject("/", "config", { align: "left", archived: false, cycles: [10, 19, 21] })

  doc.put(config, "align", "right")

  // Anywhere Object Ids are being used a path can also be used.
  // The following two statements are equivalent:

  // get the id then use it

  // get returns a single simple javascript value or undefined
  // getWithType returns an Array of the datatype plus basic type or null

  let id = doc.getWithType("/", "config")
  if (id && id[0] === 'map') {
    doc.put(id[1], "align", "right")
  }

  // use a path instead

  doc.put("/config", "align", "right")
```

Using the id directly is always faster (as it prevents the path to id conversion internally) so it is preferred for performance critical code.

### Maps

Maps are key/value stores.  The root object is always a map.  The keys are always strings.  The values can be any scalar type or any object.

```javascript
    let doc = create()
    let mymap = doc.putObject("_root", "mymap", { foo: "bar"})
                              // make a new map with the foo key

    doc.put(mymap, "bytes", new Uint8Array([1,2,3]))
                              // assign a byte array to key `bytes` of the mymap object

    let submap = doc.putObject(mymap, "sub", {})
                              // make a new empty object and assign it to the key `sub` of mymap

    doc.keys(mymap)           // returns ["bytes","foo","sub"]
    doc.materialize("_root")  // returns { mymap: { bytes: new Uint8Array([1,2,3]), foo: "bar", sub: {}}}
```

### Lists

Lists are index addressable sets of values.  These values can be any scalar or object type.  You can manipulate lists with `insert()`, `put()`, `insertObject()`, `putObject()`, `push()`, `pushObject()`, `splice()`, and `delete()`.

```javascript
    let doc = create()
    let items = doc.putObject("_root", "items", [10,"box"])
                                                  // init a new list with two elements
    doc.push(items, true)                         // push `true` to the end of the list
    doc.putObject(items, 0, { hello: "world" })  // overwrite the value 10 with an object with a key and value
    doc.delete(items, 1)                             // delete "box"
    doc.splice(items, 2, 0, ["bag", "brick"])     // splice in "bag" and "brick" at position 2
    doc.insert(items, 0, "bat")                   // insert "bat" to the beginning of the list
    doc.insertObject(items, 1, [1,2])             // insert a list with 2 values at pos 1

    doc.materialize(items)                        // returns [ "bat", [1,2], { hello : "world" }, true, "bag", "brick"]
    doc.length(items)                             // returns 6
```

### Text

Text is a specialized list type intended for modifying a text document.  The primary way to interact with a text document is via the `splice()` method. Spliced strings will be indexable by character (important to note for platforms that index by graphmeme cluster).

```javascript
    let doc = create("aaaaaa")
    let notes = doc.putObject("_root", "notes", "Hello world")
    doc.splice(notes, 6, 5, "everyone")

    doc.text(notes)      // returns "Hello everyone"
```

### Tables

Automerge's Table type is currently not implemented.

### Querying Data

When querying maps use the `get()` method with the object in question and the property to query.  This method returns a tuple with the data type and the data.  The `keys()` method will return all the keys on the object.  If you are interested in conflicted values from a merge use `getAll()` instead which returns an array of values instead of just the winner.

```javascript
    let doc1 = create("aabbcc")
    doc1.put("_root", "key1", "val1")
    let key2 = doc1.putObject("_root", "key2", [])

    doc1.get("_root", "key1") // returns "val1"
    doc1.getWithType("_root", "key2") // returns ["list", "2@aabbcc"]
    doc1.keys("_root")          // returns ["key1", "key2"]

    let doc2 = doc1.fork("ffaaff")

    // put a value concurrently
    doc1.put("_root","key3","doc1val")
    doc2.put("_root","key3","doc2val")

    doc1.merge(doc2)

    doc1.get("_root","key3")   // returns "doc2val"
    doc1.getAll("_root","key3")  // returns [[ "str", "doc1val"], ["str", "doc2val"]]
```

### Counters

Counters are 64 bit ints that support the increment operation.  Frequently different actors will want to increment or decrement a number and have all these coalesse into a merged value.

```javascript
    let doc1 = create("aaaaaa")
    doc1.put("_root", "number", 0)
    doc1.put("_root", "total", 0, "counter")

    let doc2 = doc1.fork("bbbbbb")
    doc2.put("_root", "number", 10)
    doc2.increment("_root", "total", 11)

    doc1.put("_root", "number", 20)
    doc1.increment("_root", "total", 22)

    doc1.merge(doc2)

    doc1.materialize("_root")  // returns { number: 10, total: 33 }
```

### Transactions

Generally speaking you don't need to think about transactions when using Automerge.  Normal edits queue up into an in-progress transaction.  You can query the number of ops in the current transaction with `pendingOps()`.  The transaction will commit automatically on certains calls such as `save()`, `saveIncremental()`, `fork()`, `merge()`, `getHeads()`, `applyChanges()`, `generateSyncMessage()`, and `receiveSyncMessage()`.  When the transaction commits the heads of the document change.  If you want to roll back all the in progress ops you can call `doc.rollback()`.  If you want to manually commit a transaction in progress you can call `doc.commit()` with an optional commit message and timestamp.

```javascript
    let doc = create()

    doc.put("_root", "key", "val1")

    doc.get("_root", "key")        // returns "val1"
    doc.pendingOps()                 // returns 1

    doc.rollback()

    doc.get("_root", "key")        // returns null
    doc.pendingOps()                 // returns 0

    doc.put("_root", "key", "val2")

    doc.pendingOps()                 // returns 1

    doc.commit("test commit 1")

    doc.get("_root", "key")        // returns "val2"
    doc.pendingOps()                 // returns 0
```

### Viewing Old Versions of the Document

All query functions can take an optional argument of `heads` which allow you to query a prior document state. Heads are a set of change hashes that uniquely identify a point in the document history.  The `getHeads()` method can retrieve these at any point.

```javascript
    let doc = create()

    doc.put("_root", "key", "val1")
    let heads1 = doc.getHeads()

    doc.put("_root", "key", "val2")
    let heads2 = doc.getHeads()

    doc.put("_root", "key", "val3")

    doc.get("_root","key")          // returns "val3"
    doc.get("_root","key",heads2)   // returns "val2"
    doc.get("_root","key",heads1)   // returns "val1"
    doc.get("_root","key",[])       // returns undefined
```

This works for `get()`, `getAll()`, `keys()`, `length()`, `text()`, and `materialize()`

Queries of old document states are not indexed internally and will be slower than normal access.  If you need a fast indexed version of a document at a previous point in time you can create one with `doc.forkAt(heads, actor?)`

### Forking and Merging

You can `fork()` a document which makes an exact copy of it.  This assigns a new actor so changes made to the fork can be merged back in with the original.  The `forkAt()` takes a Heads, allowing you to fork off a document from a previous point in its history.  These documents allocate new memory in WASM and need to be freed.

The `merge()` command applies all changes in the argument doc into the calling doc.  Therefore if doc a has 1000 changes that doc b lacks and doc b has only 10 changes that doc a lacks, `a.merge(b)` will be much faster than `b.merge(a)`.

```javascript
    let doc1 = create()
    doc1.put("_root", "key1", "val1")

    let doc2 = doc1.fork()

    doc1.put("_root", "key2", "val2")
    doc2.put("_root", "key3", "val3")

    doc1.merge(doc2)

    doc1.materialize("_root")       // returns { key1: "val1", key2: "val2", key3: "val3" }
    doc2.materialize("_root")       // returns { key1: "val1", key3: "val3" }
```

Note that calling `a.merge(a)` will produce an unrecoverable error from the wasm-bindgen layer which (as of this writing) there is no workaround for.

### Saving and Loading

Calling `save()` converts the document to a compressed `Uint8Array()` that can be saved to durable storage.  This format uses a columnar storage format that compresses away most of the Automerge metadata needed to manage the CRDT state, but does include all of the change history.

If you wish to incrementally update a saved Automerge doc you can call `saveIncremental()` to get a `Uint8Array()` of bytes that can be appended to the file with all the new changes(). Note that the `saveIncremental()` bytes are not as compressed as the whole document save as each chunk has metadata information needed to parse it.  It may make sense to periodically perform a new `save()` to get the smallest possible file footprint.

The `load()` function takes a `Uint8Array()` of bytes produced in this way and constitutes a new document.  The `loadIncremental()` method is available if you wish to consume the result of a `saveIncremental()` with an already instanciated document.

```javascript
  import { create, load } from "@automerge/automerge-wasm"

  let doc1 = create()

  doc1.put("_root", "key1", "value1")

  let save1 = doc1.save()

  let doc2 = load(save1)

  doc2.materialize("_root")  // returns { key1: "value1" }

  doc1.put("_root", "key2", "value2")

  let saveIncremental = doc1.saveIncremental()

  let save2 = doc1.save()

  let save3 = new Uint8Array([... save1, ... saveIncremental])

  // save2 has fewer bytes than save3 but contains the same ops

  doc2.loadIncremental(saveIncremental)

  let doc3 = load(save2)

  let doc4 = load(save3)

  doc1.materialize("_root")  // returns { key1: "value1", key2: "value2" }
  doc2.materialize("_root")  // returns { key1: "value1", key2: "value2" }
  doc3.materialize("_root")  // returns { key1: "value1", key2: "value2" }
  doc4.materialize("_root")  // returns { key1: "value1", key2: "value2" }
```

One interesting feature of automerge binary saves is that they can be concatenated together in any order and can still be loaded into a coherent merged document.

```javascript
import { load } from "@automerge/automerge-wasm"
import * as fs from "fs"

let file1 = fs.readFileSync("automerge_save_1");
let file2 = fs.readFileSync("automerge_save_2");

let docA = load(file1).merge(load(file2))
let docB = load(Buffer.concat([ file1, file2 ]))

assert.deepEqual(docA.materialize("/"), docB.materialize("/"))
assert.equal(docA.save(), docB.save())
```

### Syncing

When syncing a document the `generateSyncMessage()` and `receiveSyncMessage()` methods will produce and consume sync messages.  A sync state object will need to be managed for the duration of the connection (created by the function `initSyncState()` and can be serialized to a Uint8Array() to preserve sync state with the `encodeSyncState()` and `decodeSyncState()` functions.

A very simple sync implementation might look like this.

```javascript
  import { encodeSyncState, decodeSyncState, initSyncState } from "@automerge/automerge-wasm"

  let states = {}

  function receiveMessageFromPeer(doc, peer_id, message) {
      let syncState = states[peer_id]
      doc.receiveMessage(syncState, message)
      let reply = doc.generateSyncMessage(syncState)
      if (reply) {
          sendMessage(peer_id, reply)
      }
  }

  function notifyPeerAboutUpdates(doc, peer_id) {
      let syncState = states[peer_id]
      let message = doc.generateSyncMessage(syncState)
      if (message) {
          sendMessage(peer_id, message)
      }
  }

  function onDisconnect(peer_id) {
      let state = states[peer_id]
      if (state) {
        saveSyncToStorage(peer_id, encodeSyncState(state))
      }
      delete states[peer_id]
  }

  function onConnect(peer_id) {
      let state = loadSyncFromStorage(peer_id)
      if (state) {
        states[peer_id] = decodeSyncState(state)
      } else {
        states[peer_id] = initSyncState()
      }
  }
```

### Glossary: Actors

Some basic concepts you will need to know to better understand the api are Actors and Object Ids.

Actors are ids that need to be unique to each process writing to a document.  This is normally one actor per device.  Or for a web app one actor per tab per browser would be needed.  It can be a uuid, or a public key, or a certificate, as your application demands.  All that matters is that its bytes are unique.  Actors are always expressed in this api as a hex string.

Methods that create new documents will generate random actors automatically - if you wish to supply your own it is always taken as an optional argument.  This is true for the following functions.

```javascript
  import { create, load } from "@automerge/automerge-wasm"

  let doc1 = create()  // random actorid
  let doc2 = create("aabbccdd")
  let doc3 = doc1.fork()  // random actorid
  let doc4 = doc2.fork("ccdd0011")
  let doc5 = load(doc3.save()) // random actorid
  let doc6 = load(doc4.save(), "00aabb11")

  let actor = doc1.getActor()
```

### Glossary: Object Id's

Object Ids uniquely identify an object within a document.  They are represented as strings in the format of `{counter}@{actor}`.  The root object is a special case and can be referred to as `_root`.  The counter is an ever increasing integer, starting at 1, that is always one higher than the highest counter seen in the document thus far.  Object Id's do not change when the object is modified but they do if it is overwritten with a new object.

```javascript
  let doc = create("aabbcc")
  let o1 = doc.putObject("_root", "o1", {})
  let o2 = doc.putObject("_root", "o2", {})
  doc.put(o1, "hello", "world")

  assert.deepEqual(doc.materialize("_root"), { "o1": { hello: "world" }, "o2": {} })
  assert.equal(o1, "1@aabbcc")
  assert.equal(o2, "2@aabbcc")

  let o1v2 = doc.putObject("_root", "o1", {})

  doc.put(o1, "a", "b")    // modifying an overwritten object - does nothing
  doc.put(o1v2, "x", "y")  // modifying the new "o1" object

  assert.deepEqual(doc.materialize("_root"), { "o1": { x: "y" }, "o2": {} })
```

### Appendix: Building

  The following steps should allow you to build the package

  ```
   $ rustup target add wasm32-unknown-unknown
   $ cargo install wasm-bindgen-cli
   $ cargo install wasm-opt
   $ yarn
   $ yarn release
   $ yarn pack
  ```

### Appendix: WASM and Memory Allocation

Allocated memory in rust will be freed automatically on platforms that support `FinalizationRegistry`.

This is currently supported in [all major browsers and nodejs](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/FinalizationRegistry).

On unsupported platforms you can free memory explicitly.

```javascript
  import { create, initSyncState } from "@automerge/automerge-wasm"

  let doc = create()
  let sync = initSyncState()

  doc.free()
  sync.free()
```
