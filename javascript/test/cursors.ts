import * as assert from "assert"
import * as Automerge from "../src/index.js"

describe("cursors", () => {
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
    let callbacks: Array<Automerge.PatchSource> = []
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
    do { // sometimes sync takes more than one cycle
      ;[s2, message] = Automerge.generateSyncMessage(doc1, s2)
      if (message) {
        ;[doc2, s1] = Automerge.receiveSyncMessage(doc2, s1, message)
      }
      ;[s1, message] = Automerge.generateSyncMessage(doc2, s1)
      if (message) {
        ;[doc1, s2] = Automerge.receiveSyncMessage(doc1, s2, message, {
          patchCallback,
        })
      }
    } while(message != null)
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

  describe("when working with Automerge.view", () => {
    it("getCursorPosition should work", () => {
      let doc = Automerge.from({ text: "abc" })
      const cursor = Automerge.getCursor(doc, ["text"], 1)

      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["text"], 1, 0, "x")
      })
      const heads = Automerge.getHeads(doc)

      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["text"], 1, 0, "y")
      })
      const view = Automerge.view(doc, heads)

      const position = Automerge.getCursorPosition(view, ["text"], cursor)
      assert.equal(position, 2)
    })

    it("getCursor should respect heads", () => {
      let doc = Automerge.from({ text: "aaa@bbb" })
      let heads = Automerge.getHeads(doc)

      doc = Automerge.change(doc, d => {
        // aaa~~~bbb
        Automerge.splice(d, ["text"], 3, 1, "~~~")
      })
      const view = Automerge.view(doc, heads)
      const before = Automerge.getCursor(view, ["text"], 3, "before")
      const after = Automerge.getCursor(view, ["text"], 3, "after")
      const start = Automerge.getCursor(view, ["text"], "start")
      const end = Automerge.getCursor(view, ["text"], "end")

      // aaa~~~bbb
      // ^ ^   ^  ^
      // s b   a  e

      assert.equal(Automerge.getCursorPosition(doc, ["text"], start), 0)
      assert.equal(Automerge.getCursorPosition(doc, ["text"], before), 2)
      assert.equal(Automerge.getCursorPosition(doc, ["text"], after), 6)
      assert.equal(Automerge.getCursorPosition(doc, ["text"], end), 9)
    })
  })

  it("should allow for usage of start/end cursors", () => {
    let doc = Automerge.from({ text: "abc" })

    const end = Automerge.getCursor(doc, ["text"], "end")
    const start = Automerge.getCursor(doc, ["text"], "start")

    doc = Automerge.change(doc, d => {
      Automerge.splice(d, ["text"], end, 0, "def")
    })

    assert.equal(doc.text, "abcdef")

    doc = Automerge.change(doc, d => {
      Automerge.splice(d, ["text"], start, 0, "hello")
    })

    assert.equal(doc.text, "helloabcdef")
  })

  it("should allow for usage of move before/after", () => {
    let doc = Automerge.from({ text: "aaa@bbb" })

    const before = Automerge.getCursor(doc, ["text"], 3, "before")
    const after = Automerge.getCursor(doc, ["text"], 3, "after")

    doc = Automerge.change(doc, d => {
      // aaa~~~bbb
      Automerge.splice(d, ["text"], 3, 1, "~~~")
    })

    assert.equal(Automerge.getCursorPosition(doc, ["text"], before), 2)
    assert.equal(Automerge.getCursorPosition(doc, ["text"], after), 6)
  })

  it("should convert negative indices into a start cursor", () => {
    let doc = Automerge.from({ text: "is awesome" })
    const cursor = Automerge.getCursor(doc, ["text"], -1)

    doc = Automerge.change(doc, d => {
      Automerge.splice(d, ["text"], cursor, 0, "Automerge ")
    })

    assert.equal(doc.text, "Automerge is awesome")
  })

  it("should convert indices >= string length into an end cursor", () => {
    const doc = Automerge.from({ text: "Alex" })
    const cursor1 = Automerge.getCursor(doc, ["text"], 1337)
    const cursor2 = Automerge.getCursor(doc, ["text"], 4)

    const doc1 = Automerge.change(doc, d => {
      Automerge.splice(d, ["text"], cursor1, 0, " Good")
    })

    const doc2 = Automerge.change(Automerge.clone(doc), d => {
      Automerge.splice(d, ["text"], cursor2, 0, " Good")
    })

    assert.equal(doc1.text, "Alex Good")
    assert.equal(doc2.text, "Alex Good")
  })

  describe("on lists", () => {
    it("should follow an element through inserts before it", () => {
      let doc = Automerge.from({ list: ["a", "b", "c"] })
      const cursor = Automerge.getCursor(doc, ["list"], 1)
      assert.equal(Automerge.getCursorPosition(doc, ["list"], cursor), 1)

      doc = Automerge.change(doc, d => {
        Automerge.insertAt(d.list, 0, "x", "y")
        d.list.push("z")
      })
      assert.deepEqual(doc.list, ["x", "y", "a", "b", "c", "z"])
      const position = Automerge.getCursorPosition(doc, ["list"], cursor)
      assert.equal(position, 3)
      assert.equal(doc.list[position], "b")
    })

    it("should follow an element through concurrent inserts", () => {
      let doc1 = Automerge.from({ list: ["a", "b", "c"] })
      let doc2 = Automerge.clone(doc1)
      const cursor = Automerge.getCursor(doc1, ["list"], 2)

      doc1 = Automerge.change(doc1, d => {
        Automerge.insertAt(d.list, 0, "x")
      })
      doc2 = Automerge.change(doc2, d => {
        Automerge.insertAt(d.list, 1, "y")
        Automerge.insertAt(d.list, 3, "z")
      })
      doc1 = Automerge.merge(doc1, doc2)
      doc2 = Automerge.merge(doc2, doc1)

      assert.deepEqual(doc1.list, ["x", "a", "y", "b", "z", "c"])
      assert.equal(Automerge.getCursorPosition(doc1, ["list"], cursor), 5)
      assert.equal(Automerge.getCursorPosition(doc2, ["list"], cursor), 5)
    })

    it("should resolve before/after cursors to a deleted element", () => {
      let doc = Automerge.from({ list: [1, 2, 3, 4, 5] })
      const before = Automerge.getCursor(doc, ["list"], 2, "before")
      const after = Automerge.getCursor(doc, ["list"], 2, "after")
      const deflt = Automerge.getCursor(doc, ["list"], 2)

      doc = Automerge.change(doc, d => {
        Automerge.deleteAt(d.list, 2)
      })
      assert.deepEqual(doc.list, [1, 2, 4, 5])
      assert.equal(Automerge.getCursorPosition(doc, ["list"], before), 1)
      assert.equal(Automerge.getCursorPosition(doc, ["list"], after), 2)
      assert.equal(Automerge.getCursorPosition(doc, ["list"], deflt), 2)

      doc = Automerge.change(doc, d => {
        d.list.splice(0, 4)
      })
      assert.equal(Automerge.getCursorPosition(doc, ["list"], before), 0)
      assert.equal(Automerge.getCursorPosition(doc, ["list"], after), 0)
    })

    it("should support start and end cursors", () => {
      let doc = Automerge.from({ list: [1, 2, 3] })
      const start = Automerge.getCursor(doc, ["list"], "start")
      const end = Automerge.getCursor(doc, ["list"], "end")
      const negative = Automerge.getCursor(doc, ["list"], -1)
      const pastEnd = Automerge.getCursor(doc, ["list"], 3)

      doc = Automerge.change(doc, d => {
        d.list.unshift(0)
        d.list.push(4)
      })
      for (const c of [start, negative]) {
        assert.equal(Automerge.getCursorPosition(doc, ["list"], c), 0)
      }
      for (const c of [end, pastEnd]) {
        assert.equal(Automerge.getCursorPosition(doc, ["list"], c), 5)
      }
    })

    it("should work on nested lists", () => {
      let doc = Automerge.from({
        outer: [["a"], ["b", "c", "d"]],
      })
      const cursor = Automerge.getCursor(doc, ["outer", 1], 2)
      const toList = Automerge.getCursor(doc, ["outer"], 1)

      doc = Automerge.change(doc, d => {
        Automerge.insertAt(d.outer[1], 0, "x")
        Automerge.insertAt(d.outer, 0, ["y"])
      })
      assert.deepEqual(doc.outer, [["y"], ["a"], ["x", "b", "c", "d"]])
      assert.equal(Automerge.getCursorPosition(doc, ["outer"], toList), 2)
      assert.equal(Automerge.getCursorPosition(doc, ["outer", 2], cursor), 3)
    })

    it("should work with cursors to objects in a list", () => {
      type Todo = { title: string }
      let doc = Automerge.from<{ todos: Todo[] }>({
        todos: [{ title: "one" }, { title: "two" }],
      })
      const cursor = Automerge.getCursor(doc, ["todos"], 1)

      doc = Automerge.change(doc, d => {
        d.todos.unshift({ title: "zero" })
        // resolve the cursor inside a change and use it with a list method
        const index = Automerge.getCursorPosition(d, ["todos"], cursor)
        d.todos[index].title = "TWO"
        Automerge.insertAt(d.todos, index, { title: "one and a half" })
      })
      assert.deepEqual(
        doc.todos.map(t => t.title),
        ["zero", "one", "one and a half", "TWO"],
      )
      assert.equal(Automerge.getCursorPosition(doc, ["todos"], cursor), 3)
    })

    it("should work with Automerge.view", () => {
      let doc = Automerge.from({ list: ["a", "b", "c"] })
      const heads = Automerge.getHeads(doc)
      doc = Automerge.change(doc, d => {
        Automerge.insertAt(d.list, 0, "x")
        Automerge.deleteAt(d.list, 3)
      })
      const view = Automerge.view(doc, heads)
      assert.deepEqual(view.list, ["a", "b", "c"])

      // a cursor created at old heads
      const cursor = Automerge.getCursor(view, ["list"], 1)
      assert.equal(Automerge.getCursorPosition(view, ["list"], cursor), 1)
      assert.equal(Automerge.getCursorPosition(doc, ["list"], cursor), 2)
      assert.equal(cursor, Automerge.getCursor(doc, ["list"], 2))

      // a cursor to an element deleted since
      const before = Automerge.getCursor(view, ["list"], 2, "before")
      const after = Automerge.getCursor(view, ["list"], 2, "after")
      assert.equal(Automerge.getCursorPosition(doc, ["list"], before), 2)
      assert.equal(Automerge.getCursorPosition(doc, ["list"], after), 3)
      assert.equal(Automerge.getCursorPosition(view, ["list"], before), 2)

      // a cursor to an element added since is invalid in the view
      const x = Automerge.getCursor(doc, ["list"], 0)
      assert.throws(() => Automerge.getCursorPosition(view, ["list"], x))
    })

    it("should reject maps", () => {
      const doc = Automerge.from({ map: { a: 1 }, list: [1] })
      const cursor = Automerge.getCursor(doc, ["list"], 0)
      assert.throws(
        () => Automerge.getCursor(doc, ["map"], 0),
        /Cannot getCursor: RangeError: cursors are only valid on lists and text - obj type: map/,
      )
      assert.throws(
        () => Automerge.getCursor(doc, [], 0),
        /cursors are only valid on lists and text - obj type: map/,
      )
      assert.throws(
        () => Automerge.getCursorPosition(doc, ["map"], cursor),
        /Cannot getCursorPosition: RangeError: cursors are only valid on lists and text - obj type: map/,
      )
    })
  })
})
