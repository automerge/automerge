## Automerge WASM Low Level Interface

This package is a low level interface to the [automerge rust](https://github.com/automerge/automerge-rs/tree/experiment) CRDT.  The api is intended to be as "close to the metal" as possible with only a few ease of use accommodations.  This library is used as the underpinnings for the [Automerge JS wrapper](https://github.com/automerge/automerge-rs/tree/experiment/automerge-js) and can be used as is or as a basis for another higher level expression of a CRDT.

### Why CRDT?

CRDT stands for Conflict Free Replicated Data Type.  It is a data structure that offers eventual consistency where multiple actors can write to the document independently and then these edits can be automatically merged together into a coherent document that, as much as possible, preserves the intent of the different writers.  This allows for novel masterless application design where different components need not have a central coordinating server when altering application state.

### Terminology

The term Actor, Object Id and Heads are used through this documentation.  Detailed explanations are in the glossary at the end of this readme.  But the most basic definition would be...

An Actor is a unique id that distinguishes a single writer to a document.  It can be any hex string.

An Object id uniquely identifies a Map, List or Text object within a document.  This id comes as a string in the form of `{number}@{actor}` - so `"10@aabbcc"` for example.  The string `"_root"` or `"/"` can also be used to refer to the document root.  These strings are durable and can be used on any descendant or copy of the document that generated them.

Heads refers to a set of hashes that uniquely identifies a point in time in a document's history.  Heads are useful for comparing documents state or retrieving past states from the document.

### Using the Library and Creating a Document

This is a rust/wasm package and will work in a node or web environment.  Node is able to load wasm synchronously but a web environment is not.  The default import of the package is a function that returns a promise that resolves once the wasm is loaded.

This creates a document in node.  The memory allocated is handled by wasm and isn't managed by the javascript garbage collector and thus needs to be manually freed.

```javascript
  import { create } from "automerge-wasm"

  let doc = create()

  doc.free()
```

While this will work in both node and in a web context

```javascript
  import init, { create } from "automerge-wasm"

  init().then(_ => {
    let doc = create()
    doc.free()
  })

```

The examples below will assume a node context for brevity.

### Automerge Scalar Types

Automerge has many scalar types.  Methods like `set()` and `insert()` take an optional data type parameter.  Normally the type can be inferred but in some cases, such as telling the difference between int, uint and a counter, it cannot.

These are sets without a data type

```javascript
  import { create } from "automerge-wasm"

  let doc = create()
  doc.set("/", "prop1", 100)  // int
  doc.set("/", "prop2", 3.14) // f64
  doc.set("/", "prop3", "hello world")
  doc.set("/", "prop4", new Date())
  doc.set("/", "prop5", new Uint8Array([1,2,3]))
  doc.set("/", "prop6", true)
  doc.set("/", "prop7", null)
  doc.free()
```

Sets with a data type and examples of all the supported data types.

While int vs uint vs f64 matters little in javascript, Automerge is a cross platform library where these distinctions matter.

```javascript
  import { create } from "automerge-wasm"

  let doc = create()
  doc.set("/", "prop1", 100, "int")
  doc.set("/", "prop2", 100, "uint")
  doc.set("/", "prop3", 100.5, "f64")
  doc.set("/", "prop4", 100, "counter")
  doc.set("/", "prop5", new Date(), "timestamp")
  doc.set("/", "prop6", "hello world", "str")
  doc.set("/", "prop7", new Uint8Array([1,2,3]), "bytes")
  doc.set("/", "prop8", true, "boolean")
  doc.set("/", "prop9", null, "null")
  doc.free()
```

### Automerge Object Types

Automerge WASM supports 3 object types.  Maps, lists, and text.  Maps are key value stores where the values can be any scalar type or any object type.  Lists are numerically indexed sets of data that can hold any scalar or any object type.  Text is numerically indexed sets of grapheme clusters.

```javascript
  import { create } from "automerge-wasm"

  let doc = create()

  // you can create an object by passing in the inital state - if blank pass in `{}`
  // the return value is the Object Id
  // these functions all return an object id

  let config = doc.set_object("/", "config", { align: "left", archived: false, cycles: [10, 19, 21] })
  let token = doc.set_object("/", "tokens", {})

  // lists can be made with javascript arrays

  let birds = doc.set_object("/", "birds", ["bluejay", "penguin", "puffin"])
  let bots = doc.set_object("/", "bots", [])

  // text is initialized with a string

  let notes = doc.set_object("/", "notes", "Hello world!")

  doc.free()
```

You can access objects by passing the object id as the first parameter for a call.

```javascript
  import { create } from "automerge-wasm"

  let doc = create()

  let config = doc.set_object("/", "config", { align: "left", archived: false, cycles: [10, 19, 21] })

  doc.set(config, "align", "right")
```

Anywhere Object Ids are being used a path can also be used.  The following two statements are equivalent:

```javascript
  // get the id then use it

  let id = doc.value("/", "config")[1]
  doc.set(id, "align", "right")

  // use a path instead

  doc.set("/config", "align", "right")
```

Using the id directly is always faster (as it prevents the path to id conversion internally) so it is preferred for performance critical code.

### Maps

Maps are key/value stores.  The root object is always a map.  The keys are always strings.  The values can be any scalar type or any object.

```javascript
    let doc = create()
    let mymap = doc.set_object("_root", "mymap", { foo: "bar"})
                              // make a new map with the foo key

    doc.set(mymap, "bytes", new Uint8Array([1,2,3]))
                              // assign a byte array to key `bytes` of the mymap object

    let submap = doc.set_object(mymap, "sub", {})
                              // make a new empty object and assign it to the key `sub` of mymap

    doc.keys(mymap)           // returns ["bytes","foo","sub"]
    doc.materialize("_root")  // returns { mymap: { bytes: new Uint8Array([1,2,3]), foo: "bar", sub: {} }
    doc.free()
```

### Lists

Lists are index addressable sets of values.  These values can be any scalar or object type.  You can manipulate lists with `insert()`, `set()`, `push()`, `splice()`, and `del()`.

```javascript
    let doc = create()
    let items = doc.set_object("_root", "items", [10,"box"])
                                                  // init a new list with two elements
    doc.push(items, true)                         // push `true` to the end of the list
    doc.set_object(items, 0, { hello: "world" })  // overwrite the value 10 with an object with a key and value
    doc.del(items, 1)                             // delete "box"
    doc.splice(items, 2, 0, ["bag", "brick"])     // splice in "bag" and "brick" at position 2
    doc.insert(items, 0, "bat")                   // insert "bat" to the beginning of the list

    doc.materialize(items)                        // returns [ "bat", { hello : "world" }, true, "bag", "brick"]
    doc.length(items)                             // returns 5
    doc.free()
```

### Text

Text is a specialized list type intended for modifying a text document.  The primary way to interact with a text document is via the slice operation.  Non text can be inserted into a text document and will be represented with the unicode object replacement character.

```javascript
    let doc = create("aaaaaa")
    let notes = doc.set_object("_root", "notes", "Hello world")
    doc.splice(notes, 6, 5, "everyone")

    doc.text(notes)      // returns "Hello everyone"

    let obj = doc.insert_object(text, 6, { hi: "there" });

    doc.text(text)       // returns "Hello \ufffceveryone"
    doc.value(text, 6)   // returns ["map", obj]
    doc.value(obj, "hi") // returns ["str", "there"]
    doc.free()
```


### Tables

Automerge's Table type is currently not implemented.

### Querying Data

When querying maps use the `value()` method with the object in question and the property to query.  This method returns a tuple with the data type and the data.  The `keys()` method will return all the keys on the object.  If you are interested in conflicted values from a merge use `values()` instead which returns an array of values instead of just the winner.

```javascript
    let doc1 = create("aabbcc")
    doc1.set("_root", "key1", "val1")
    let key2 = doc1.set_object("_root", "key2", [])

    doc1.value("_root", "key1") // returns ["str", "val1"]
    doc1.value("_root", "key1") // returns ["list", "2@aabbcc"]
    doc1.keys("_root")          // returns ["key1", "key2"]

    let doc2 = doc1.fork("ffaaff")
    
    // set a value concurrently
    doc1.set("_root","key3","doc1val")
    doc2.set("_root","key3","doc2val")

    doc1.merge(doc2)

    doc1.value("_root","key3")   // returns ["str", "doc2val"]
    doc1.values("_root","key3")  // returns [[ "str", "doc1val"], ["str", "doc2val"]]
    doc1.free(); doc2.free() 
```

### Counters

// TODO

### Transactions

// TODO

### Viewing Old Versions of the Document

All query functions can take an optional argument of `heads` which allow you to query a prior document state. Heads are a set of change hashes that uniquly identify a point in the document history.  The `getHeads()` method can retrieve these at any point.

```javascript
    let doc = create() 
    doc.set("_root", "key", "val1")
    let heads1 = doc.getHeads()
    doc.set("_root", "key", "val2")
    let heads2 = doc.getHeads()
    doc.set("_root", "key", "val3")

    doc.value("_root","key")          // returns ["str","val3"]
    doc.value("_root","key",heads2)   // returns ["str","val2"]
    doc.value("_root","key",heads1)   // returns ["str","val1"]
    doc.value("_root","key",[])       // returns null
```

This works for `value()`, `values()`, `keys()`, `length()`, `text()`, and `materialize()`

### Forking and Merging

### Saving and Loading

### Syncing

### Glossary: Actors

Some basic concepts you will need to know to better understand the api are Actors and Object Ids.

Actors are ids that need to be unique to each process writing to a document.  This is normally one actor per device.  Or for a web app one actor per tab per browser would be needed.  It can be a uuid, or a public key, or a certificate, as your application demands.  All that matters is that its bytes are unique.  Actors are always expressed in this api as a hex string.

Methods that create new documents will generate random actors automatically - if you wish to supply your own it is always taken as an optional argument.  This is true for the following functions.

```
  import { create, loadDoc } from "automerge-wasm"

  let doc1 = create()  // random actorid
  let doc2 = create("aabbccdd")
  let doc3 = doc1.fork()  // random actorid
  let doc4 = doc2.for("ccdd0011")
  let doc5 = loadDoc(doc3.save()) // random actorid
  let doc6 = loadDoc(doc4.save(), "00aabb11")

  let actor = doc1.getActor()

  doc1.free(); doc2.free(); doc3.free(); doc4.free(); doc5.free(); doc6.free()
```

### Glossary: Object Id's

Object Id's uniquly identify an object within a document.  They are represented as strings in the format of `{counter}@{actor}`.  The root object is a special case and can be referred to as `_root`.  The counter in an ever increasing integer, starting at 1, that is always one higher than the highest counter seen in the document thus far.  Object Id's do not change when the object is modified but they do if it is overwritten with a new object.

```
  let doc = create("aabbcc")
  let o1 = doc.set_object("_root", "o1", {})
  let o2 = doc.set_object("_root", "o2", {})
  doc.set(o1, "hello", "world")

  assert.deepEqual(doc.materialize("_root"), { "o1": { hello: "world" }, "o2": {} })
  assert.equal(o1, "1@aabbcc")
  assert.equal(o2, "2@aabbcc")

  let o1v2 = doc.set_object("_root", "o1", {})

  doc.set(o1, "a", "b")    // modifying an overwritten object - does nothing
  doc.set(o1v2, "x", "y")  // modifying the new "o1" object

  assert.deepEqual(doc.materialize("_root"), { "o1": { x: "y" }, "o2": {} })

```

### Glossary: Heads

// FIXME
loadDoc()
forkAt()
set_object() -> setObject()
materialize(heads)
