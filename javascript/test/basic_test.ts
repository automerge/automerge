import * as assert from "assert"
import { next as Automerge } from "../src"
import * as oldAutomerge from "../src/stable"
import * as WASM from "@automerge/automerge-wasm"
import { mismatched_heads } from "./helpers"
import { PatchSource } from "../src/types"

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

    it("handle non-text strings", () => {
      let doc1 = WASM.create({ text_v1: true })
      doc1.put("_root", "text", "hello world")
      let doc2 = Automerge.load<any>(doc1.save())
      assert.throws(() => {
        Automerge.change(doc2, d => {
          Automerge.splice(d, ["text"], 1, 0, "Z")
        })
      }, /Cannot splice/)
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

    it("should work in stable", () => {
      const doc1 = oldAutomerge.init<any>()
      const doc2 = oldAutomerge.change(doc1, d => {
        d.list = [1, 2, 3]
      })
      const doc3 = oldAutomerge.change(doc2, d => {
        d.list.push(4)
      })
      const changes = oldAutomerge.getChanges(doc2, doc3)
      assert.equal(changes.length, 1)
      oldAutomerge.load(changes[0], { allowMissingChanges: true })
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
    it("can use cursors in splice calls", () => {
      let doc = Automerge.from({
        value: "The sly fox jumped over the lazy dog",
      })
      let cursor = Automerge.getCursor(doc, ["value"], 19)
      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["value"], 0, 3, "Has the")
      })
      assert.deepEqual(doc.value, "Has the sly fox jumped over the lazy dog")
      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["value"], cursor, 0, "right ")
      })
      assert.deepEqual(
        doc.value,
        "Has the sly fox jumped right over the lazy dog",
      )
      Automerge.getCursorPosition(doc, ["value"], cursor)
    })

    it("should be able to pass a doc to from() to make a shallow copy", () => {
      let state = {
        text: "The sly fox jumped over the lazy dog",
        x: 5,
        y: new Date(),
        z: [1, 2, 3, { alpha: "bravo" }],
      }
      let doc1 = Automerge.from(state)
      assert.deepEqual(doc1, state)
      let doc2 = Automerge.from(doc1)
      assert.deepEqual(doc1, doc2)
    })

    it("can use cursors in common text operations", () => {
      let doc = Automerge.from({
        value: "The sly fox jumped over the lazy dog",
      })
      let doc2 = Automerge.clone(doc)

      let cursor = Automerge.getCursor(doc, ["value"], 8)

      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["value"], cursor, 0, "o")
        Automerge.splice(d, ["value"], cursor, 0, "l")
        Automerge.splice(d, ["value"], cursor, 0, "e")
      })
      doc2 = Automerge.change(doc2, d => {
        Automerge.splice(d, ["value"], 3, -3, "A")
      })
      doc = Automerge.merge(doc, doc2)
      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["value"], cursor, -1, "d")
        Automerge.splice(d, ["value"], cursor, 0, " ")
      })
      assert.deepEqual(doc.value, "A sly old fox jumped over the lazy dog")
    })

    it("should use javascript string indices", () => {
      let doc = Automerge.from({
        value: "🇬🇧🇩🇪",
      })

      let cursor = Automerge.getCursor(doc, ["value"], doc.value.indexOf("🇩🇪"))
      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["value"], cursor, -2, "")
        Automerge.splice(d, ["value"], cursor, -2, "")
        Automerge.splice(d, ["value"], cursor, 0, "🇫🇷")
      })

      assert.deepEqual(doc.value, "🇫🇷🇩🇪")
    })

    it("patch callbacks inform where they came from", () => {
      type DocShape = {
        hello: string
        a?: string
        b?: string
        x?: string
        n?: string
      }
      let callbacks: Array<PatchSource> = []
      let patchCallback = (_p, meta) => callbacks.push(meta.source)
      let doc1 = Automerge.from<DocShape>({ hello: "world" }, { patchCallback })
      let heads1 = Automerge.getHeads(doc1)
      let doc2 = Automerge.clone(doc1, { patchCallback })
      doc2 = Automerge.change(doc2, d => (d.a = "b"))
      doc2 = Automerge.changeAt(doc2, heads1, d => (d.b = "c")).newDoc
      doc1 = Automerge.merge(doc1, doc2)
      doc2 = Automerge.change(doc2, d => (d.x = "y"))
      doc1 = Automerge.loadIncremental(doc1, Automerge.saveIncremental(doc2))
      doc2 = Automerge.change(doc2, d => (d.n = "m"))
      let s1 = Automerge.initSyncState()
      let s2 = Automerge.initSyncState()
      let message
      ;[s2, message] = Automerge.generateSyncMessage(doc1, s2)
      ;[doc2, s1] = Automerge.receiveSyncMessage(doc2, s1, message)
      ;[s1, message] = Automerge.generateSyncMessage(doc2, s1)
      ;[doc1, s2] = Automerge.receiveSyncMessage(doc1, s2, message, {
        patchCallback,
      })
      assert.deepEqual(callbacks, [
        "from",
        "change",
        "changeAt",
        "merge",
        "change",
        "loadIncremental",
        "change",
        "receiveSyncMessage",
      ])
    })

    it("should allow dates from an existing document to be used in another document", () => {
      let originalDoc: any = Automerge.change(Automerge.init(), (doc: any) => {
        doc.date = new Date()
        doc.dates = [new Date()]
      })

      Automerge.change(originalDoc, (doc: any) => {
        doc.anotherDate = originalDoc.date
        doc.dates[0] = originalDoc.dates[0]
      })
    })
  })
})
