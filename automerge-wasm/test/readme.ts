import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import init, { create, load } from '..'

describe('Automerge', () => {
  describe('Readme Examples', () => {
    it('Using the Library and Creating a Document (1)', () => {
      let doc = create()
      doc.free()
    })
    it('Using the Library and Creating a Document (2)', (done) => {
      init().then((_:any) => {
        let doc = create()
        doc.free()
        done()
      })
    })
    it('Automerge Scalar Types (1)', () => {
      let doc = create()
      doc.put("/", "prop1", 100)  // int
      doc.put("/", "prop2", 3.14) // f64
      doc.put("/", "prop3", "hello world")
      doc.put("/", "prop4", new Date(0))
      doc.put("/", "prop5", new Uint8Array([1,2,3]))
      doc.put("/", "prop6", true)
      doc.put("/", "prop7", null)

      assert.deepEqual(doc.materialize("/"), {
        prop1: 100,
        prop2: 3.14,
        prop3: "hello world",
        prop4: new Date(0),
        prop5: new Uint8Array([1,2,3]),
        prop6: true,
        prop7: null
      })

      doc.free()
    })
    it('Automerge Scalar Types (2)', () => {
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
      doc.free()
    })
    it('Automerge Object Types (1)', () => {
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

      doc.free()
    })
    it('Automerge Object Types (2)', () => {
      let doc = create()

      let config = doc.putObject("/", "config", { align: "left", archived: false, cycles: [10, 19, 21] })

      doc.put(config, "align", "right")

      // Anywhere Object Ids are being used a path can also be used.
      // The following two statements are equivalent:

      let id = doc.value("/", "config")
      if (id && id[0] === 'map') {
        doc.put(id[1], "align", "right")
      }

      doc.put("/config", "align", "right")

      assert.deepEqual(doc.materialize("/"), {
         config: { align: "right", archived: false, cycles: [ 10, 19, 21 ] }
      })

      doc.free()
    })
    it('Maps (1)', () => {
      let doc = create()
      let mymap = doc.putObject("_root", "mymap", { foo: "bar"})
                                // make a new map with the foo key

      doc.put(mymap, "bytes", new Uint8Array([1,2,3]))
                                // assign a byte array to key `bytes` of the mymap object

      let submap = doc.putObject(mymap, "sub", {})
                                // make a new empty object and assign it to the key `sub` of mymap

      assert.deepEqual(doc.keys(mymap),["bytes","foo","sub"])
      assert.deepEqual(doc.materialize("_root"), { mymap: { bytes: new Uint8Array([1,2,3]), foo: "bar", sub: {} }})

      doc.free()
    })
    it('Lists (1)', () => {
      let doc = create()
      let items = doc.putObject("_root", "items", [10,"box"])
                                                    // init a new list with two elements
      doc.push(items, true)                         // push `true` to the end of the list
      doc.putObject(items, 0, { hello: "world" })  // overwrite the value 10 with an object with a key and value
      doc.delete(items, 1)                             // delete "box"
      doc.splice(items, 2, 0, ["bag", "brick"])     // splice in "bag" and "brick" at position 2
      doc.insert(items, 0, "bat")                   // insert "bat" to the beginning of the list
      doc.insertObject(items, 1, [ 1, 2 ])             // insert a list with 2 values at pos 1

      assert.deepEqual(doc.materialize(items),[ "bat", [ 1 ,2 ], { hello : "world" }, true, "bag", "brick" ])
      assert.deepEqual(doc.length(items),6)

      doc.free()
    })
    it('Text (1)', () => {
      let doc = create("aaaaaa")
      let notes = doc.putObject("_root", "notes", "Hello world")
      doc.splice(notes, 6, 5, "everyone")

      assert.deepEqual(doc.text(notes), "Hello everyone")

      let obj = doc.insertObject(notes, 6, { hi: "there" })

      assert.deepEqual(doc.text(notes), "Hello \ufffceveryone")
      assert.deepEqual(doc.value(notes, 6), ["map", obj])
      assert.deepEqual(doc.value(obj, "hi"), ["str", "there"])

      doc.free()
    })
    it('Querying Data (1)', () => {
      let doc1 = create("aabbcc")
      doc1.put("_root", "key1", "val1")
      let key2 = doc1.putObject("_root", "key2", [])

      assert.deepEqual(doc1.value("_root", "key1"), ["str", "val1"])
      assert.deepEqual(doc1.value("_root", "key2"), ["list", "2@aabbcc"])
      assert.deepEqual(doc1.keys("_root"), ["key1", "key2"])

      let doc2 = doc1.fork("ffaaff")

      // set a value concurrently
      doc1.put("_root","key3","doc1val")
      doc2.put("_root","key3","doc2val")

      doc1.merge(doc2)

      assert.deepEqual(doc1.value("_root","key3"), ["str", "doc2val"])
      assert.deepEqual(doc1.values("_root","key3"),[[ "str", "doc1val", "3@aabbcc"], ["str", "doc2val", "3@ffaaff"]])

      doc1.free(); doc2.free()
    })
    it('Counters (1)', () => {
      let doc1 = create("aaaaaa")
      doc1.put("_root", "number", 0)
      doc1.put("_root", "total", 0, "counter")

      let doc2 = doc1.fork("bbbbbb")
      doc2.put("_root", "number", 10)
      doc2.increment("_root", "total", 11)

      doc1.put("_root", "number", 20)
      doc1.increment("_root", "total", 22)

      doc1.merge(doc2)

      assert.deepEqual(doc1.materialize("_root"), { number: 10, total: 33 })

      doc1.free(); doc2.free()
    })
    it('Transactions (1)', () => {
      let doc = create()

      doc.put("_root", "key", "val1")

      assert.deepEqual(doc.value("_root", "key"),["str","val1"])
      assert.deepEqual(doc.pendingOps(),1)

      doc.rollback()

      assert.deepEqual(doc.value("_root", "key"),null)
      assert.deepEqual(doc.pendingOps(),0)

      doc.put("_root", "key", "val2")

      assert.deepEqual(doc.pendingOps(),1)

      doc.commit("test commit 1")

      assert.deepEqual(doc.value("_root", "key"),["str","val2"])
      assert.deepEqual(doc.pendingOps(),0)

      doc.free()
    })
    it('Viewing Old Versions of the Document (1)', () => {
      let doc = create()

      doc.put("_root", "key", "val1")
      let heads1 = doc.getHeads()

      doc.put("_root", "key", "val2")
      let heads2 = doc.getHeads()

      doc.put("_root", "key", "val3")

      assert.deepEqual(doc.value("_root","key"), ["str","val3"])
      assert.deepEqual(doc.value("_root","key",heads2), ["str","val2"])
      assert.deepEqual(doc.value("_root","key",heads1), ["str","val1"])
      assert.deepEqual(doc.value("_root","key",[]), null)

      doc.free()
    })
    it('Forking And Merging (1)', () => {
      let doc1 = create()
      doc1.put("_root", "key1", "val1")

      let doc2 = doc1.fork()

      doc1.put("_root", "key2", "val2")
      doc2.put("_root", "key3", "val3")

      doc1.merge(doc2)

      assert.deepEqual(doc1.materialize("_root"), { key1: "val1", key2: "val2", key3: "val3" })
      assert.deepEqual(doc2.materialize("_root"), { key1: "val1", key3: "val3" })

      doc1.free(); doc2.free()
    })
    it('Saving And Loading (1)', () => {
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

      assert.deepEqual(doc1.materialize("_root"), { key1: "value1", key2: "value2" })
      assert.deepEqual(doc2.materialize("_root"), { key1: "value1", key2: "value2" })
      assert.deepEqual(doc3.materialize("_root"), { key1: "value1", key2: "value2" })
      assert.deepEqual(doc4.materialize("_root"), { key1: "value1", key2: "value2" })

      doc1.free(); doc2.free(); doc3.free(); doc4.free()
    })
    it.skip('Syncing (1)', () => { })
  })
})
