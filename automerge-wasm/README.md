## Automerge WASM Low Level Interface

This package is a low level interface to the [automerge rust](https://github.com/automerge/automerge-rs/tree/experiment) CRDT.  The api is intended to be a "close to the metal" as possible only a few ease of use accomodations.  This library is used as the underpinnings for the [Automerge JS wrapper](https://github.com/automerge/automerge-rs/tree/experiment/automerge-js) and can be used as is or as a basis for another higher level expression of a CRDT.

### Why CRDT?

// TODO

### Terminology

The term Actor, Object Id and Heads are used through this documentation.  Detailed explanations are in the glossary at the end of this readme.  But the most basic definition would be...

An Actor is a unique id that distinguishes a single writer to a document.  It can be any hex string.

An Object id uniquely identifies a Map, List or Text object within a document.  This id comes as a string in the form on `{number}@{actor}` - so `"10@aabbcc"` for example.  The string `"_root"` or `"/"` can also be used to refer to the document root.  These strings are durable and can be used on any decendant or copy of the document that generated them.

Heads refers to a set of hashes that uniquly identifies a point in time in a documents history.  Heads are useful for comparing documents state or retrieving past states from the document.

### Using the Library and Creating a Document

This is a rust/wasm package and will work in a node or web environment.  Node is able to load wasm syncronously but a web environment is not.  The default import of the package is a function that returns a promise that resolves once the wasm is loaded.

This creates a document in node.  The memory allocated is handled by wasm and isn't managed by the javascript garbage collector and thus needs to be manually freed.

```
  import { create } from "automerge-wasm"

  let doc = create()

  doc.free()

```

While this will work in both node and in a web context

```
  import init, { create } from "automerge-wasm"

  init().then(_ => {
    let doc = create()
    doc.free()
  })

```

The examples below will assume a node context for brevity.

### Automerge Scalar Types

Automerge has many scalar types.  Methods like `set()` and `insert()` take an optional datatype parameter.  Normally the type can be inferred but in some cases, such as telling the difference between int, uint and a counter, it cannot.

These are sets without a datatype

```
  import { create } from "automerge-wasm"

  let doc = create()
  doc.set("/", "prop1", 100) // int
  doc.set("/", "prop2", 3.14)
  doc.set("/", "prop3", "hello world")
  doc.set("/", "prop4", new Date())
  doc.set("/", "prop5", new Uint8Array([1,2,3]))
  doc.set("/", "prop6", true)
  doc.set("/", "prop7", null)
  doc.free()
```

Sets with a datatype and examples of all the supported datatypes.

While int vs uint vs f64 matters little in javascript, Automerge is a cross platform library where these distinctions matter.

```
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

Automerge WASM supports 3 object types.  Maps, lists, and text.  Maps are key value stores where the values can be any scalar type or any object type.  Lists are numerically indexed set of data that can hold any scalar or any object type.  Text is numerically indexed sets of graphmeme clusters.

```
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

```
  import { create } from "automerge-wasm"

  let doc = create()

  let config = doc.set_object("/", "config", { align: "left", archived: false, cycles: [10, 19, 21] })

  doc.set(config, "align", "right")
```

Anywhere Object Id's are being used a path can also be used.  The following two statements are equivelent:

```
  // get the id then use it

  let id = doc.value("/", "config")[1]
  doc.set(id, "align", "right")

  // use a path instead

  doc.set("/config", "align", "right")
```

Using the id directly is always faster (as it prevents the path to id conversion internally) so it is preferred for performance critical code.

### Maps

### Lists

### Text

### Tables

Automerge's Table type is currently not implemented

### Counters

### Viewing Old Versions of the Document

### Forking and Merging

### Saving and Loading

### Syncing

### Glossery: Actors

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
```

### Glossery: Object Id's
### Glossery: Heads
