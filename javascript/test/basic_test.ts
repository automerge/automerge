import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"
import { mismatched_heads } from "./helpers.js"
import { PatchSource } from "../src/types.js"
import { IMMUTABLE_STRING } from "../src/constants.js"
import { readFile } from "fs/promises"
import { join } from "path"
import { fileURLToPath } from "url"

describe("Automerge", () => {
  describe("basics", () => {
    it("should init clone and free", () => {
      let doc1 = Automerge.init()
      let doc2 = Automerge.clone(doc1)

      // this is only needed if weakrefs are not supported
      Automerge.free(doc1)
      Automerge.free(doc2)
    })

    it("should be able to make a view with specifc heads", () => {
      let doc1 = Automerge.init<any>()
      let doc2 = Automerge.change(doc1, d => (d.value = 1))
      let heads2 = Automerge.getHeads(doc2)
      let doc3 = Automerge.change(doc2, d => (d.value = 2))
      let doc2_v2 = Automerge.view(doc3, heads2)
      assert.deepEqual(doc2, doc2_v2)
      let doc2_v2_clone = Automerge.clone(doc2, "aabbcc")
      assert.deepEqual(doc2, doc2_v2_clone)
      assert.equal(Automerge.getActorId(doc2_v2_clone), "aabbcc")
    })

    it("should allow you to change a clone of a view", () => {
      let doc1 = Automerge.init<any>()
      doc1 = Automerge.change(doc1, d => (d.key = "value"))
      let heads = Automerge.getHeads(doc1)
      doc1 = Automerge.change(doc1, d => (d.key = "value2"))
      let fork = Automerge.clone(Automerge.view(doc1, heads))
      assert.deepEqual(fork, { key: "value" })
      fork = Automerge.change(fork, d => (d.key = "value3"))
      assert.deepEqual(fork, { key: "value3" })
    })

    it("handle basic set and read on root object", () => {
      let doc1 = Automerge.init<any>()
      let doc2 = Automerge.change(doc1, d => {
        d.hello = "world"
        d.big = "little"
        d.zip = "zop"
        d.app = "dap"
        assert.deepEqual(d, {
          hello: "world",
          big: "little",
          zip: "zop",
          app: "dap",
        })
      })
      assert.deepEqual(doc2, {
        hello: "world",
        big: "little",
        zip: "zop",
        app: "dap",
      })
    })

    it("should be able to insert and delete a large number of properties", () => {
      let doc = Automerge.init<any>()

      doc = Automerge.change(doc, doc => {
        doc["k1"] = true
      })

      for (let idx = 1; idx <= 200; idx++) {
        doc = Automerge.change(doc, doc => {
          delete doc["k" + idx]
          doc["k" + (idx + 1)] = true
          assert(Object.keys(doc).length == 1)
        })
      }
    })

    it("can detect an automerge doc with isAutomerge()", () => {
      const doc1 = Automerge.from({ sub: { object: true } })
      assert(Automerge.isAutomerge(doc1))
      assert(!Automerge.isAutomerge(doc1.sub))
      assert(!Automerge.isAutomerge("String"))
      assert(!Automerge.isAutomerge({ sub: { object: true } }))
      assert(!Automerge.isAutomerge(undefined))
      const jsObj = Automerge.toJS(doc1)
      assert(!Automerge.isAutomerge(jsObj))
      assert.deepEqual(jsObj, doc1)
    })

    it("it should recursively freeze the document if requested", () => {
      let doc1 = Automerge.init<any>({ freeze: true })
      let doc2 = Automerge.init<any>()

      assert(Object.isFrozen(doc1))
      assert(!Object.isFrozen(doc2))

      // will also freeze sub objects
      doc1 = Automerge.change(
        doc1,
        doc => (doc.book = { title: "how to win friends" }),
      )
      doc2 = Automerge.merge(doc2, doc1)
      assert(Object.isFrozen(doc1))
      assert(Object.isFrozen(doc1.book))
      assert(!Object.isFrozen(doc2))
      assert(!Object.isFrozen(doc2.book))

      // works on from
      let doc3 = Automerge.from({ sub: { obj: "inner" } }, { freeze: true })
      assert(Object.isFrozen(doc3))
      assert(Object.isFrozen(doc3.sub))

      // works on load
      let doc4 = Automerge.load<any>(Automerge.save(doc3), { freeze: true })
      assert(Object.isFrozen(doc4))
      assert(Object.isFrozen(doc4.sub))

      // follows clone
      let doc5 = Automerge.clone(doc4)
      assert(Object.isFrozen(doc5))
      assert(Object.isFrozen(doc5.sub))

      // toJS does not freeze
      let exported = Automerge.toJS(doc5)
      assert(!Object.isFrozen(exported))
    })

    it("handle basic sets over many changes", () => {
      let doc1 = Automerge.init<any>()
      let timestamp = new Date()
      let counter = new Automerge.Counter(100)
      let bytes = new Uint8Array([10, 11, 12])
      let doc2 = Automerge.change(doc1, d => {
        d.hello = "world"
      })
      let doc3 = Automerge.change(doc2, d => {
        d.counter1 = counter
      })
      let doc4 = Automerge.change(doc3, d => {
        d.timestamp1 = timestamp
      })
      let doc5 = Automerge.change(doc4, d => {
        d.app = null
      })
      let doc6 = Automerge.change(doc5, d => {
        d.bytes1 = bytes
      })
      let doc7 = Automerge.change(doc6, d => {
        d.uint = new Automerge.Uint(1)
        d.int = new Automerge.Int(-1)
        d.float64 = new Automerge.Float64(5.5)
        d.number1 = 100
        d.number2 = -45.67
        d.true = true
        d.false = false
      })

      assert.deepEqual(doc7, {
        hello: "world",
        true: true,
        false: false,
        int: -1,
        uint: 1,
        float64: 5.5,
        number1: 100,
        number2: -45.67,
        counter1: counter,
        timestamp1: timestamp,
        bytes1: bytes,
        app: null,
      })

      let changes = Automerge.getAllChanges(doc7)
      let t1 = Automerge.init()
      let [t2] = Automerge.applyChanges(t1, changes)
      assert.deepEqual(doc7, t2)
    })

    it("handle overwrites to values", () => {
      let doc1 = Automerge.init<any>()
      let doc2 = Automerge.change(doc1, d => {
        d.hello = "world1"
      })
      let doc3 = Automerge.change(doc2, d => {
        d.hello = "world2"
      })
      let doc4 = Automerge.change(doc3, d => {
        d.hello = "world3"
      })
      let doc5 = Automerge.change(doc4, d => {
        d.hello = "world4"
      })
      assert.deepEqual(doc5, { hello: "world4" })
    })

    it("handle set with object value", () => {
      let doc1 = Automerge.init<any>()
      let doc2 = Automerge.change(doc1, d => {
        d.subobj = { hello: "world", subsubobj: { zip: "zop" } }
      })
      assert.deepEqual(doc2, {
        subobj: { hello: "world", subsubobj: { zip: "zop" } },
      })
    })

    it("handle simple list creation", () => {
      let doc1 = Automerge.init<any>()
      let doc2 = Automerge.change(doc1, d => (d.list = []))
      assert.deepEqual(doc2, { list: [] })
    })

    it("handle simple lists", () => {
      let doc1 = Automerge.init<any>()
      let doc2 = Automerge.change(doc1, d => {
        d.list = [1, 2, 3]
      })
      assert.deepEqual(doc2.list.length, 3)
      assert.deepEqual(doc2.list[0], 1)
      assert.deepEqual(doc2.list[1], 2)
      assert.deepEqual(doc2.list[2], 3)
      assert.deepEqual(doc2, { list: [1, 2, 3] })
      // assert.deepStrictEqual(Automerge.toJS(doc2), { list: [1,2,3] })

      let doc3 = Automerge.change(doc2, d => {
        d.list[1] = "a"
      })

      assert.deepEqual(doc3.list.length, 3)
      assert.deepEqual(doc3.list[0], 1)
      assert.deepEqual(doc3.list[1], "a")
      assert.deepEqual(doc3.list[2], 3)
      assert.deepEqual(doc3, { list: [1, "a", 3] })
    })
    it("handle simple lists", () => {
      let doc1 = Automerge.init<any>()
      let doc2 = Automerge.change(doc1, d => {
        d.list = [1, 2, 3]
      })
      let changes = Automerge.getChanges(doc1, doc2)
      let docB1 = Automerge.init()
      let [docB2] = Automerge.applyChanges(docB1, changes)
      assert.deepEqual(docB2, doc2)
    })
    it("handle text", () => {
      let doc1 = Automerge.init<any>()
      let doc2 = Automerge.change(doc1, d => {
        d.list = "hello"
        Automerge.splice(d, ["list"], 2, 0, "Z")
      })
      let changes = Automerge.getChanges(doc1, doc2)
      let docB1 = Automerge.init()
      let [docB2] = Automerge.applyChanges(docB1, changes)
      assert.deepEqual(docB2, doc2)
    })

    it("have many list methods", () => {
      let doc1 = Automerge.from({ list: [1, 2, 3] })
      assert.deepEqual(doc1, { list: [1, 2, 3] })
      let doc2 = Automerge.change(doc1, d => {
        d.list.splice(1, 1, 9, 10)
      })
      assert.deepEqual(doc2, { list: [1, 9, 10, 3] })
      let doc3 = Automerge.change(doc2, d => {
        d.list.push(11, 12)
      })
      assert.deepEqual(doc3, { list: [1, 9, 10, 3, 11, 12] })
      let doc4 = Automerge.change(doc3, d => {
        d.list.unshift(2, 2)
      })
      assert.deepEqual(doc4, { list: [2, 2, 1, 9, 10, 3, 11, 12] })
      let doc5 = Automerge.change(doc4, d => {
        d.list.shift()
      })
      assert.deepEqual(doc5, { list: [2, 1, 9, 10, 3, 11, 12] })
      let doc6 = Automerge.change(doc5, d => {
        Automerge.insertAt(d.list, 3, 100, 101)
      })
      assert.deepEqual(doc6, { list: [2, 1, 9, 100, 101, 10, 3, 11, 12] })
    })

    it("allows access to the backend", () => {
      let doc = Automerge.from({ hello: "world" })
      assert.deepEqual(Automerge.getBackend(doc).materialize(), {
        hello: "world",
      })
    })

    it("lists and text have indexof", () => {
      let doc = Automerge.from({
        list: [0, 1, 2, 3, 4, 5, 6],
        text: "hello world",
      })
      assert.deepEqual(doc.list.indexOf(5), 5)
      assert.deepEqual(doc.text.indexOf("world"), 6)
    })
    it("get change metadata", () => {
      let doc = Automerge.from<any>({ text: "hello world" })
      let heads = Automerge.getHeads(doc)
      doc = Automerge.change(doc, d => {
        d.foo = "bar"
      })
      doc = Automerge.change(doc, d => {
        d.zip = "zop"
      })
      let changes = Automerge.getChangesSince(doc, heads).map(
        Automerge.decodeChange,
      )
      let meta = Automerge.getChangesMetaSince(doc, heads)
      assert.equal(changes.length, 2)
      assert.equal(meta.length, 2)
      for (let i = 0; i < 2; i++) {
        assert.equal(changes[i].actor, meta[i].actor)
        assert.equal(changes[i].hash, meta[i].hash)
        assert.equal(changes[i].message, meta[i].message)
        assert.equal(changes[i].time, meta[i].time)
        assert.deepEqual(changes[i].deps, meta[i].deps)
        assert.deepEqual(changes[i].startOp, meta[i].startOp)
      }
    })
  })

  describe("explicitly allowing missing dependencies when loading", () => {
    it("should work in unstable", () => {
      const doc1 = Automerge.init<any>()
      const doc2 = Automerge.change(doc1, d => {
        d.list = [1, 2, 3]
      })
      const doc3 = Automerge.change(doc2, d => {
        d.list.push(4)
      })
      const changes = Automerge.getChanges(doc2, doc3)
      assert.equal(changes.length, 1)
      Automerge.load(changes[0], { allowMissingChanges: true })
    })
  })

  describe("merge", () => {
    it("it should handle conflicts the same in merges as with loads", () => {
      type DocShape = { sub: { x: number; y: number } }
      let doc1 = Automerge.from({ sub: { x: 0, y: 0 } })
      let doc2 = Automerge.clone(doc1)
      let doc3 = Automerge.clone(doc1)
      let doc4 = Automerge.clone(doc1)

      // same counter - different actors
      doc1 = Automerge.change(doc1, d => (d.sub.x = 1))
      doc2 = Automerge.change(doc2, d => (d.sub.x = 2))
      doc3 = Automerge.change(doc3, d => (d.sub.x = 3))
      doc4 = Automerge.change(doc4, d => (d.sub.x = 4))

      // differrent counter and different actors
      doc1 = Automerge.change(doc1, d => (d.sub.y = 1))

      doc2 = Automerge.change(doc2, d => (d.sub.y = 2))
      doc2 = Automerge.change(doc2, d => (d.sub.y = 3))

      doc3 = Automerge.change(doc3, d => (d.sub.y = 4))
      doc3 = Automerge.change(doc3, d => (d.sub.y = 5))
      doc3 = Automerge.change(doc3, d => (d.sub.y = 6))

      doc4 = Automerge.change(doc4, d => (d.sub.y = 7))
      doc4 = Automerge.change(doc4, d => (d.sub.y = 8))
      doc4 = Automerge.change(doc4, d => (d.sub.y = 9))
      doc4 = Automerge.change(doc4, d => (d.sub.y = 10))

      let docM = Automerge.init<DocShape>()
      docM = Automerge.merge(docM, doc1)
      docM = Automerge.merge(docM, doc2)
      docM = Automerge.merge(docM, doc3)
      docM = Automerge.merge(docM, doc4)

      let docL = Automerge.load<DocShape>(Automerge.save(docM))

      assert.deepEqual(docM.sub.x, docL.sub.x)
      assert.deepEqual(docM.sub.y, docL.sub.y)
    })
  })

  describe("clone", () => {
    it("should not copy the patchcallback", () => {
      const patches: Automerge.Patch[][] = []
      let doc = Automerge.init<{ foo: string | undefined }>({
        patchCallback: p => patches.push(p),
      })
      let doc2 = Automerge.clone(doc)
      doc2 = Automerge.change(doc2, d => (d.foo = "bar"))
      assert.deepEqual(patches.length, 0)
    })
  })

  describe("emptyChange", () => {
    it("should generate a hash", () => {
      let doc = Automerge.init()
      doc = Automerge.change<any>(doc, d => {
        d.key = "value"
      })
      Automerge.save(doc)
      let headsBefore = Automerge.getHeads(doc)
      headsBefore.sort()
      doc = Automerge.emptyChange(doc, "empty change")
      let headsAfter = Automerge.getHeads(doc)
      headsAfter.sort()
      assert.notDeepEqual(headsBefore, headsAfter)
    })
  })

  describe("proxy lists", () => {
    it("behave like arrays", () => {
      let doc = Automerge.from({
        chars: ["a", "b", "c"],
        numbers: [20, 3, 100],
        repeats: [20, 20, 3, 3, 3, 3, 100, 100],
      })
      let r1: Array<number> = []
      doc = Automerge.change(doc, d => {
        assert.deepEqual((d.chars as any[]).concat([1, 2]), [
          "a",
          "b",
          "c",
          1,
          2,
        ])
        assert.deepEqual(
          d.chars.map(n => n + "!"),
          ["a!", "b!", "c!"],
        )
        assert.deepEqual(
          d.numbers.map(n => n + 10),
          [30, 13, 110],
        )
        assert.deepEqual(d.numbers.toString(), "20,3,100")
        assert.deepEqual(d.numbers.toLocaleString(), "20,3,100")
        assert.deepEqual(
          d.numbers.forEach((n: number) => r1.push(n)),
          undefined,
        )
        assert.deepEqual(
          d.numbers.every(n => n > 1),
          true,
        )
        assert.deepEqual(
          d.numbers.every(n => n > 10),
          false,
        )
        assert.deepEqual(
          d.numbers.filter(n => n > 10),
          [20, 100],
        )
        assert.deepEqual(
          d.repeats.find(n => n < 10),
          3,
        )
        assert.deepEqual(
          d.repeats.find(n => n < 10),
          3,
        )
        assert.deepEqual(
          d.repeats.find(n => n < 0),
          undefined,
        )
        assert.deepEqual(
          d.repeats.findIndex(n => n < 10),
          2,
        )
        assert.deepEqual(
          d.repeats.findIndex(n => n < 0),
          -1,
        )
        assert.deepEqual(
          d.repeats.findIndex(n => n < 10),
          2,
        )
        assert.deepEqual(
          d.repeats.findIndex(n => n < 0),
          -1,
        )
        assert.deepEqual(d.numbers.includes(3), true)
        assert.deepEqual(d.numbers.includes(-3), false)
        assert.deepEqual(d.numbers.join("|"), "20|3|100")
        assert.deepEqual(d.numbers.join(), "20,3,100")
        assert.deepEqual(
          d.numbers.some(f => f === 3),
          true,
        )
        assert.deepEqual(
          d.numbers.some(f => f < 0),
          false,
        )
        assert.deepEqual(
          d.numbers.reduce((sum, n) => sum + n, 100),
          223,
        )
        assert.deepEqual(
          d.repeats.reduce((sum, n) => sum + n, 100),
          352,
        )
        assert.deepEqual(
          d.chars.reduce((sum, n) => sum + n, "="),
          "=abc",
        )
        assert.deepEqual(
          d.chars.reduceRight((sum, n) => sum + n, "="),
          "=cba",
        )
        assert.deepEqual(
          d.numbers.reduceRight((sum, n) => sum + n, 100),
          223,
        )
        assert.deepEqual(d.repeats.lastIndexOf(3), 5)
        assert.deepEqual(d.repeats.lastIndexOf(3, 3), 3)
      })
      doc = Automerge.change(doc, d => {
        assert.deepEqual(d.numbers.fill(-1, 1, 2), [20, -1, 100])
        assert.deepEqual(d.chars.fill("z", 1, 100), ["a", "z", "z"])
      })
      assert.deepEqual(r1, [20, 3, 100])
      assert.deepEqual(doc.numbers, [20, -1, 100])
      assert.deepEqual(doc.chars, ["a", "z", "z"])
    })
  })

  it("should obtain the same conflicts, regardless of merge order", () => {
    let s1 = Automerge.init<any>()
    let s2 = Automerge.init<any>()
    s1 = Automerge.change(s1, doc => {
      doc.x = 1
      doc.y = 2
    })
    s2 = Automerge.change(s2, doc => {
      doc.x = 3
      doc.y = 4
    })
    const m1 = Automerge.merge(Automerge.clone(s1), Automerge.clone(s2))
    const m2 = Automerge.merge(Automerge.clone(s2), Automerge.clone(s1))
    assert.deepStrictEqual(
      Automerge.getConflicts(m1, "x"),
      Automerge.getConflicts(m2, "x"),
    )
  })

  describe("getObjectId", () => {
    let s1 = Automerge.from({
      string: "string",
      number: 1,
      null: null,
      date: new Date(),
      counter: new Automerge.Counter(),
      bytes: new Uint8Array(10),
      text: "",
      list: [],
      map: {},
    })

    it("should return null for scalar values", () => {
      assert.equal(Automerge.getObjectId(s1.string), null)
      assert.equal(Automerge.getObjectId(s1.number), null)
      assert.equal(Automerge.getObjectId(s1.null!), null)
      assert.equal(Automerge.getObjectId(s1.date), null)
      assert.equal(Automerge.getObjectId(s1.counter), null)
      assert.equal(Automerge.getObjectId(s1.bytes), null)
    })

    it("should return _root for the root object", () => {
      assert.equal(Automerge.getObjectId(s1), "_root")
    })

    it("should return non-null for map, list, text, and objects", () => {
      assert.equal(Automerge.getObjectId(s1.text), null)
      assert.notEqual(Automerge.getObjectId(s1.list), null)
      assert.notEqual(Automerge.getObjectId(s1.map), null)
    })
  })
  describe("load", () => {
    it("can load a doc without checking the heads", () => {
      assert.throws(() => {
        Automerge.load(mismatched_heads)
      }, /mismatching heads/)
      let doc = Automerge.load(mismatched_heads, { unchecked: true })
      assert.deepEqual(doc, { count: 260 })
    })
  })
  describe("diff", () => {
    it("can diff a document with before and hafter heads", () => {
      let doc = Automerge.from({ value: "" })
      doc = Automerge.change(doc, d => (d.value = "aaa"))
      let heads1 = Automerge.getHeads(doc)
      doc = Automerge.change(doc, d => (d.value = "bbb"))
      let heads2 = Automerge.getHeads(doc)
      let patch12 = Automerge.diff(doc, heads1, heads2)
      let patch21 = Automerge.diff(doc, heads2, heads1)
      assert.deepEqual(patch12, [
        { action: "put", path: ["value"], value: "" },
        { action: "splice", path: ["value", 0], value: "bbb" },
      ])
      assert.deepEqual(patch21, [
        { action: "put", path: ["value"], value: "" },
        { action: "splice", path: ["value", 0], value: "aaa" },
      ])
    })
  })
  describe("cursor", () => {
    it("should indicate that the op is not present when resolving a cursor in a previous version of the document", () => {
      const doc = Automerge.from({
        value: "world",
      })

      const doc1 = Automerge.change(doc, d => {
        Automerge.splice(d, ["value"], 0, 0, "hello ")
      })

      assert.deepEqual(doc1.value, "hello world")

      const cursor = Automerge.getCursor(doc1, ["value"], 0)
      const index = Automerge.getCursorPosition(doc, ["value"], cursor)

      assert.deepEqual(index, null)
    })
  })
  describe("saveSince", () => {
    it("should be the same as saveIncremental since heads of the last saveIncremental", () => {
      let doc = Automerge.init<any>()
      doc = Automerge.change(doc, d => (d.a = "b"))
      Automerge.saveIncremental(doc)
      const heads = Automerge.getHeads(doc)

      doc = Automerge.change(doc, d => (d.c = "d"))
      let incremental = Automerge.saveIncremental(doc)
      let since = Automerge.saveSince(doc, heads)
      assert.deepEqual(incremental, since)
    })
  })
  describe("any function which takes a path should not mutate the argument path", () => {
    let doc: Automerge.Doc<{ wrapper: { text: string } }>
    let path: Automerge.Prop[]
    let pathCopy: Automerge.Prop[]
    beforeEach(() => {
      doc = Automerge.init<{ wrapper: { text: string } }>()
      doc = Automerge.change(doc, d => {
        d.wrapper = { text: "hello world" }
      })
      path = ["wrapper", "text"]
      pathCopy = path.slice()
    })

    it("splice", () => {
      doc = Automerge.change(doc, d => {
        Automerge.splice(d, path, 0, 0, "z")
      })
      assert.deepEqual(path, pathCopy)
    })

    it("updateText", () => {
      doc = Automerge.change(doc, d => {
        Automerge.updateText(d, path, "hello earth")
      })
      assert.deepEqual(path, pathCopy)
    })

    it("getCursor", () => {
      Automerge.getCursor(doc, path, 0)
      assert.deepEqual(path, pathCopy)
    })

    it("getCursorPosition", () => {
      const c = Automerge.getCursor(doc, path, 0)
      Automerge.getCursorPosition(doc, path, c)
      assert.deepEqual(path, pathCopy)
    })

    it("mark/unmark", () => {
      doc = Automerge.change(doc, d => {
        Automerge.mark(
          d,
          path,
          { expand: "none", start: 0, end: 2 },
          "bold",
          true,
        )
      })
      assert.deepEqual(path, pathCopy)
      doc = Automerge.change(doc, d => {
        Automerge.unmark(d, path, { expand: "none", start: 0, end: 2 }, "bold")
      })
      assert.deepEqual(path, pathCopy)
    })

    it("marks", () => {
      Automerge.marks(doc, path)
      assert.deepEqual(path, pathCopy)
    })

    it("marksAt", () => {
      Automerge.marksAt(doc, path, 0)
      assert.deepEqual(path, pathCopy)
    })
  })

  describe("the hasHeads function", () => {
    it("should return true if the document in question has all the heads", () => {
      let doc = Automerge.init<any>()
      doc = Automerge.change(doc, d => (d.a = "b"))
      let heads = Automerge.getHeads(doc)
      assert(Automerge.hasHeads(doc, heads))
    })

    it("should return false if the document does not have the heads", () => {
      let doc = Automerge.init<any>()
      doc = Automerge.change(doc, d => (d.a = "b"))
      let heads = Automerge.getHeads(doc)

      let otherDoc = Automerge.init<any>()
      assert(!Automerge.hasHeads(otherDoc, heads))
    })
  })

  describe("the topoHistoryTraversal function", () => {
    it("should return the correct history", () => {
      let doc = Automerge.from({ a: "a" }, { actor: "aaaaaa" })
      let hash1 = Automerge.decodeChange(
        Automerge.getLastLocalChange(doc)!,
      ).hash

      let doc2 = Automerge.clone(doc, { actor: "bbbbbb" })

      doc = Automerge.change(doc, d => (d.a = "b"))
      let hash2 = Automerge.decodeChange(
        Automerge.getLastLocalChange(doc)!,
      ).hash

      doc2 = Automerge.change(doc2, d => (d.a = "c"))
      let hash3 = Automerge.decodeChange(
        Automerge.getLastLocalChange(doc2)!,
      ).hash

      doc = Automerge.merge(doc, doc2)

      let hashes = [hash1, hash2, hash3]
      let topo = Automerge.topoHistoryTraversal(doc)
      assert.deepStrictEqual(topo, hashes)
    })
  })

  describe("the inspectChange function", () => {
    it("should return a decoded representation of the change", () => {
      let doc = Automerge.init<{ a: string | null }>({ actor: "aaaaaa" })
      doc = Automerge.change(doc, { time: 123 }, d => (d.a = "a"))
      let hash1 = Automerge.topoHistoryTraversal(doc)[0]

      const change = Automerge.inspectChange(doc, hash1)
      assert.deepStrictEqual(change, {
        actor: "aaaaaa",
        deps: [],
        hash: hash1,
        message: null,
        ops: [
          {
            action: "makeText",
            key: "a",
            obj: "_root",
            pred: [],
          },
          {
            action: "set",
            elemId: "_head",
            insert: true,
            obj: "1@aaaaaa",
            pred: [],
            value: "a",
          },
        ],
        seq: 1,
        startOp: 1,
        time: 123,
      })
    })
  })

  describe("the stats function", () => {
    it("should return stats about the document", () => {
      let doc = Automerge.init<{ a: number }>()
      doc = Automerge.change(doc, d => (d.a = 1))
      doc = Automerge.change(doc, d => (d.a = 2))
      const stats = Automerge.stats(doc)
      assert.equal(stats.numChanges, 2)
      assert.equal(stats.numOps, 2)
      assert.equal(typeof stats.cargoPackageName, "string")
      assert.equal(typeof stats.cargoPackageVersion, "string")
      assert.equal(typeof stats.rustcVersion, "string")
    })
  }),
    describe("the toJS function", () => {
      it("should return the document at its correct heads", () => {
        const doc = Automerge.from<any>({ x: 1 })

        const doc1 = Automerge.change(doc, doc => {
          doc.a = 123
          doc.b = 456
        })

        assert.deepStrictEqual(Automerge.toJS(doc), { x: 1 })
        assert.deepStrictEqual(Automerge.toJS(doc1), { a: 123, b: 456, x: 1 })
      })
    })

  describe("When handling ImmutableString", () => {
    it("should treat any class which has the correct symbol as a ImmutableString", () => {
      // Exactly the same as `ImmutableString`
      class FakeImmutableString {
        val: string;
        [IMMUTABLE_STRING] = true
        constructor(val: string) {
          this.val = val
        }

        /**
         * Returns the content of the ImmutableString object as a simple string
         */
        toString(): string {
          return this.val
        }
      }

      let doc = Automerge.from<{ foo: FakeImmutableString | null }>({
        foo: null,
      })
      doc = Automerge.change(doc, d => {
        d.foo = new FakeImmutableString("something")
      })
      assert.deepStrictEqual(
        doc.foo,
        new Automerge.ImmutableString("something"),
      )
    })

    it("should export RawString and isRawString for backwards compatibility", () => {
      // Check the predicate is the same
      assert.equal(Automerge.isImmutableString, Automerge.isRawString)
      // Check the types are the same
      const _dummy: Automerge.ImmutableString = new Automerge.RawString("xyz")
    })
  })

  it("should export a predicate to check if something is an immutablestring", () => {
    let doc = Automerge.from({
      foo: new Automerge.ImmutableString("someval2"),
      bar: "notanimmutablestring",
    })
    assert.strictEqual(Automerge.isImmutableString(doc.foo), true)
    assert.strictEqual(Automerge.isImmutableString(doc.bar), false)

    doc = Automerge.change(doc, d => {
      assert.strictEqual(Automerge.isImmutableString(d.foo), true)
      assert.strictEqual(Automerge.isImmutableString(d.bar), false)
    })
  })
  it("it should be able to roll back a transaction", () => {
    let doc1 = Automerge.from<any>({ foo: "bar" })
    let save1 = Automerge.save(doc1)
    assert.throws(() => {
      let doc2 = Automerge.change(doc1, d => {
        d.key = "value"
        throw new RangeError("no")
      })
    })
    let save2 = Automerge.save(doc1)
    assert.deepEqual(save1, save2)
  })

  it("it should be able to handle ints and floats at their limits", () => {
    let imax = BigInt("9223372036854775807")
    let imin = BigInt("-9223372036854775808")
    let umax = BigInt("18446744073709551615")
    let inf = Infinity
    let ninf = -Infinity
    let nan = NaN;
    let base = { nan, inf, ninf, imax, imin, umax }
    let doc1 = Automerge.from<any>(base)
    assert.deepEqual(doc1, base)
    let doc2 = Automerge.load<any>(Automerge.save(doc1));
    assert.deepEqual(doc2, base)
    let doc3 = Automerge.change(Automerge.init<any>(), d => {
        d.imax = imax;
        d.umax = umax;
        d.imin = imin;
        d.nan = nan;
        d.inf = inf;
        d.ninf = ninf;
    })
    assert.deepEqual(doc3, base)
    assert.throws(() => {
      let doc4 = Automerge.from<any>({ bad: umax + BigInt("1") })
    }, /larger than/)
    assert.throws(() => {
      let doc4 = Automerge.from<any>({ bad: imin - BigInt("1") })
    }, /smaller than/)
  })
})
