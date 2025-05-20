import * as assert from "assert"
import * as Automerge from "../src/index.js"
import { assertEqualsOneOf } from "./helpers.js"

type DocType = {
  text: string
  [key: string]: any
}

describe("Automerge.Text", () => {
  let s1: Automerge.Doc<DocType>, s2: Automerge.Doc<DocType>
  beforeEach(() => {
    s1 = Automerge.change(Automerge.init<DocType>(), doc => (doc.text = ""))
    s2 = Automerge.merge(Automerge.init<DocType>(), s1)
  })

  it("should support insertion", () => {
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, ["text"], 0, 0, "a"))
    assert.strictEqual(s1.text.length, 1)
    assert.strictEqual(s1.text[0], "a")
    assert.strictEqual(s1.text, "a")
    //assert.strictEqual(s1.text.getElemId(0), `2@${Automerge.getActorId(s1)}`)
  })

  it("should support deletion", () => {
    s1 = Automerge.change(s1, doc =>
      Automerge.splice(doc, ["text"], 0, 0, "abc"),
    )
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, ["text"], 1, 1))
    assert.strictEqual(s1.text.length, 2)
    assert.strictEqual(s1.text[0], "a")
    assert.strictEqual(s1.text[1], "c")
    assert.strictEqual(s1.text, "ac")
  })

  it("should support implicit and explicit deletion", () => {
    s1 = Automerge.change(s1, doc =>
      Automerge.splice(doc, ["text"], 0, 0, "abc"),
    )
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, ["text"], 1, 1))
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, ["text"], 1, 0))
    assert.strictEqual(s1.text.length, 2)
    assert.strictEqual(s1.text[0], "a")
    assert.strictEqual(s1.text[1], "c")
    assert.strictEqual(s1.text, "ac")
  })

  it("should handle concurrent insertion", () => {
    s1 = Automerge.change(s1, doc =>
      Automerge.splice(doc, ["text"], 0, 0, "abc"),
    )
    s2 = Automerge.change(s2, doc =>
      Automerge.splice(doc, ["text"], 0, 0, "xyz"),
    )
    s1 = Automerge.merge(s1, s2)
    assert.strictEqual(s1.text.length, 6)
    assertEqualsOneOf(s1.text, "abcxyz", "xyzabc")
  })

  it("should handle text and other ops in the same change", () => {
    s1 = Automerge.change(s1, doc => {
      doc.foo = "bar"
      Automerge.splice(doc, ["text"], 0, 0, "a")
    })
    assert.strictEqual(s1.foo, "bar")
    assert.strictEqual(s1.text, "a")
    assert.strictEqual(s1.text, "a")
  })

  it("should serialize to JSON as a simple string", () => {
    s1 = Automerge.change(s1, doc =>
      Automerge.splice(doc, ["text"], 0, 0, 'a"b'),
    )
    assert.strictEqual(JSON.stringify(s1), '{"text":"a\\"b"}')
  })

  it("should allow modification after an object is assigned to a document", () => {
    s1 = Automerge.change(Automerge.init(), doc => {
      doc.text = ""
      Automerge.splice(doc, ["text"], 0, 0, "abcd")
      Automerge.splice(doc, ["text"], 2, 1)
      assert.strictEqual(doc.text, "abd")
    })
    assert.strictEqual(s1.text, "abd")
  })

  it("should not allow modification outside of a change callback", () => {
    assert.throws(
      () => Automerge.splice(s1, ["text"], 0, 0, "a"),
      /object cannot be modified outside of a change block/,
    )
  })

  describe("with initial value", () => {
    it("should initialize text in Automerge.from()", () => {
      let s1 = Automerge.from({ text: "init" })
      assert.strictEqual(s1.text.length, 4)
      assert.strictEqual(s1.text[0], "i")
      assert.strictEqual(s1.text[1], "n")
      assert.strictEqual(s1.text[2], "i")
      assert.strictEqual(s1.text[3], "t")
      assert.strictEqual(s1.text, "init")
    })

    it("should encode the initial value as a change", () => {
      const s1 = Automerge.from({ text: "init" })
      const changes = Automerge.getAllChanges(s1)
      assert.strictEqual(changes.length, 1)
      const [s2] = Automerge.applyChanges(Automerge.init<DocType>(), changes)
      assert.strictEqual(s2.text, "init")
      assert.strictEqual(s2.text, "init")
    })
  })

  it("should support unicode when creating text", () => {
    s1 = Automerge.from({
      text: "🐦",
    })
    assert.strictEqual(s1.text, "🐦")
  })

  it("should allow splicing into text in arrays", () => {
    let doc = Automerge.from({ dom: [["world"]] })

    doc = Automerge.change(doc, d => {
      Automerge.splice(d.dom, [0, 0], 0, 0, "Hello ")
    })
    assert.strictEqual(doc.dom[0][0], "Hello world")
  })

  describe("updateText", () => {
    it("should calculate a diff when updating text", () => {
      let doc1 = Automerge.from({ text: "Hello world!" }, { actor: "aaaaaa" })

      let doc2 = Automerge.clone(doc1, { actor: "bbbbbb" })
      doc2 = Automerge.change(doc2, d => {
        Automerge.updateText(d, ["text"], "Goodbye world!")
      })

      doc1 = Automerge.change(doc1, d => {
        Automerge.updateText(d, ["text"], "Hello friends!")
      })

      const merged = Automerge.merge(doc1, doc2)
      assert.strictEqual(merged.text, "Goodbye friends!")
    })

    it("should handle multi character grapheme clusters", () => {
      let doc1 = Automerge.from({ text: "left👨‍👩‍👦right" }, { actor: "aaaaaa" })

      let doc2 = Automerge.clone(doc1, { actor: "bbbbbb" })
      doc2 = Automerge.change(doc2, d => {
        Automerge.updateText(d, ["text"], "left👨‍👩‍👧right")
      })

      doc1 = Automerge.change(doc1, d => {
        Automerge.updateText(d, ["text"], "left👨‍👩‍👦‍👦right")
      })

      const merged = Automerge.merge(doc1, doc2)
      assert.strictEqual(merged.text, "left👨‍👩‍👧👨‍👩‍👦‍👦right")
    })
  })
})
