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
})
