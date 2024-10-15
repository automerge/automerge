import * as assert from "assert"
import { next as Automerge, PatchSource } from "../src/index.js"

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
      let doc = Automerge.from({ text: "abc" })
      let heads = Automerge.getHeads(doc)

      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["text"], 1, 0, "x")
      })
      const cursor = Automerge.getCursor(doc, ["text"], 3)
      assert.notEqual(cursor, undefined)

      // First check that trying to get a cursor from a view where the sequence is shorter then the
      // attempted index throws an error
      assert.throws(() => {
        const oldCursor = Automerge.getCursor(
          Automerge.view(doc, heads),
          ["text"],
          3,
        )
      }, /index 3 is out of bounds/)

      // Now check that if we delete characters, the getCursor call will still work when viewing
      // the document with heads that have the deleted characters
      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["text"], 0, 4, "")
      })
      const view = Automerge.view(doc, heads)
      const cursor2 = Automerge.getCursor(view, ["text"], 2)
      assert.equal(cursor, cursor2)
    })
  })

})
