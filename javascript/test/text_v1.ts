import * as assert from "assert"
import * as Automerge from "../src"
import { assertEqualsOneOf } from "./helpers"

type DocType = { text: Automerge.Text; [key: string]: any }

describe("Automerge.Text", () => {
  let s1: Automerge.Doc<DocType>, s2: Automerge.Doc<DocType>
  beforeEach(() => {
    s1 = Automerge.change(
      Automerge.init<DocType>(),
      doc => (doc.text = new Automerge.Text()),
    )
    s2 = Automerge.merge(Automerge.init(), s1)
  })

  it("should support insertion", () => {
    s1 = Automerge.change(s1, doc => doc.text.insertAt(0, "a"))
    assert.strictEqual(s1.text.length, 1)
    assert.strictEqual(s1.text.get(0), "a")
    assert.strictEqual(s1.text.toString(), "a")
    //assert.strictEqual(s1.text.getElemId(0), `2@${Automerge.getActorId(s1)}`)
  })

  it("should support deletion", () => {
    s1 = Automerge.change(s1, doc => doc.text.insertAt(0, "a", "b", "c"))
    s1 = Automerge.change(s1, doc => doc.text.deleteAt(1, 1))
    assert.strictEqual(s1.text.length, 2)
    assert.strictEqual(s1.text.get(0), "a")
    assert.strictEqual(s1.text.get(1), "c")
    assert.strictEqual(s1.text.toString(), "ac")
  })

  it("should support implicit and explicit deletion", () => {
    s1 = Automerge.change(s1, doc => doc.text.insertAt(0, "a", "b", "c"))
    s1 = Automerge.change(s1, doc => doc.text.deleteAt(1))
    s1 = Automerge.change(s1, doc => doc.text.deleteAt(1, 0))
    assert.strictEqual(s1.text.length, 2)
    assert.strictEqual(s1.text.get(0), "a")
    assert.strictEqual(s1.text.get(1), "c")
    assert.strictEqual(s1.text.toString(), "ac")
  })

  it("should handle concurrent insertion", () => {
    s1 = Automerge.change(s1, doc => doc.text.insertAt(0, "a", "b", "c"))
    s2 = Automerge.change(s2, doc => doc.text.insertAt(0, "x", "y", "z"))
    s1 = Automerge.merge(s1, s2)
    assert.strictEqual(s1.text.length, 6)
    assertEqualsOneOf(s1.text.toString(), "abcxyz", "xyzabc")
    assertEqualsOneOf(s1.text.join(""), "abcxyz", "xyzabc")
  })

  it("should handle text and other ops in the same change", () => {
    s1 = Automerge.change(s1, doc => {
      doc.foo = "bar"
      doc.text.insertAt(0, "a")
    })
    assert.strictEqual(s1.foo, "bar")
    assert.strictEqual(s1.text.toString(), "a")
    assert.strictEqual(s1.text.join(""), "a")
  })

  it("should serialize to JSON as a simple string", () => {
    s1 = Automerge.change(s1, doc => doc.text.insertAt(0, "a", '"', "b"))
    assert.strictEqual(JSON.stringify(s1), '{"text":"a\\"b"}')
  })

  it("should allow modification before an object is assigned to a document", () => {
    s1 = Automerge.change(Automerge.init(), doc => {
      const text = new Automerge.Text()
      text.insertAt(0, "abcd")
      text.deleteAt(2)
      doc.text = text
      assert.strictEqual(doc.text.toString(), "abd")
      assert.strictEqual(doc.text.join(""), "abd")
    })
    assert.strictEqual(s1.text.toString(), "abd")
    assert.strictEqual(s1.text.join(""), "abd")
  })

  it("should allow modification after an object is assigned to a document", () => {
    s1 = Automerge.change(Automerge.init(), doc => {
      const text = new Automerge.Text()
      doc.text = text
      doc.text.insertAt(0, "a", "b", "c", "d")
      doc.text.deleteAt(2)
      assert.strictEqual(doc.text.toString(), "abd")
      assert.strictEqual(doc.text.join(""), "abd")
    })
    assert.strictEqual(s1.text.join(""), "abd")
  })

  it("should not allow modification outside of a change callback", () => {
    assert.throws(
      () => s1.text.insertAt(0, "a"),
      /object cannot be modified outside of a change block/,
    )
  })

  describe("with initial value", () => {
    it("should accept a string as initial value", () => {
      let s1 = Automerge.change(
        Automerge.init<DocType>(),
        doc => (doc.text = new Automerge.Text("init")),
      )
      assert.strictEqual(s1.text.length, 4)
      assert.strictEqual(s1.text.get(0), "i")
      assert.strictEqual(s1.text.get(1), "n")
      assert.strictEqual(s1.text.get(2), "i")
      assert.strictEqual(s1.text.get(3), "t")
      assert.strictEqual(s1.text.toString(), "init")
    })

    it("should accept an array as initial value", () => {
      let s1 = Automerge.change(
        Automerge.init<DocType>(),
        doc => (doc.text = new Automerge.Text(["i", "n", "i", "t"])),
      )
      assert.strictEqual(s1.text.length, 4)
      assert.strictEqual(s1.text.get(0), "i")
      assert.strictEqual(s1.text.get(1), "n")
      assert.strictEqual(s1.text.get(2), "i")
      assert.strictEqual(s1.text.get(3), "t")
      assert.strictEqual(s1.text.toString(), "init")
    })

    it("should initialize text in Automerge.from()", () => {
      let s1 = Automerge.from({ text: new Automerge.Text("init") })
      assert.strictEqual(s1.text.length, 4)
      assert.strictEqual(s1.text.get(0), "i")
      assert.strictEqual(s1.text.get(1), "n")
      assert.strictEqual(s1.text.get(2), "i")
      assert.strictEqual(s1.text.get(3), "t")
      assert.strictEqual(s1.text.toString(), "init")
    })

    it("should encode the initial value as a change", () => {
      const s1 = Automerge.from({ text: new Automerge.Text("init") })
      const changes = Automerge.getAllChanges(s1)
      assert.strictEqual(changes.length, 1)
      const [s2] = Automerge.applyChanges(Automerge.init<DocType>(), changes)
      assert.strictEqual(s2.text instanceof Automerge.Text, true)
      assert.strictEqual(s2.text.toString(), "init")
      assert.strictEqual(s2.text.join(""), "init")
    })

    it("should allow immediate access to the value", () => {
      Automerge.change(Automerge.init<DocType>(), doc => {
        const text = new Automerge.Text("init")
        assert.strictEqual(text.length, 4)
        assert.strictEqual(text.get(0), "i")
        assert.strictEqual(text.toString(), "init")
        doc.text = text
        assert.strictEqual(doc.text.length, 4)
        assert.strictEqual(doc.text.get(0), "i")
        assert.strictEqual(doc.text.toString(), "init")
      })
    })

    it("should allow pre-assignment modification of the initial value", () => {
      let s1 = Automerge.change(Automerge.init<DocType>(), doc => {
        const text = new Automerge.Text("init")
        text.deleteAt(3)
        assert.strictEqual(text.join(""), "ini")
        doc.text = text
        assert.strictEqual(doc.text.join(""), "ini")
        assert.strictEqual(doc.text.toString(), "ini")
      })
      assert.strictEqual(s1.text.toString(), "ini")
      assert.strictEqual(s1.text.join(""), "ini")
    })

    it("should allow post-assignment modification of the initial value", () => {
      let s1 = Automerge.change(Automerge.init<DocType>(), doc => {
        const text = new Automerge.Text("init")
        doc.text = text
        doc.text.deleteAt(0)
        doc.text.insertAt(0, "I")
        assert.strictEqual(doc.text.join(""), "Init")
        assert.strictEqual(doc.text.toString(), "Init")
      })
      assert.strictEqual(s1.text.join(""), "Init")
      assert.strictEqual(s1.text.toString(), "Init")
    })
  })

  describe("non-textual control characters", () => {
    let s1: Automerge.Doc<DocType>
    beforeEach(() => {
      s1 = Automerge.change(Automerge.init<DocType>(), doc => {
        doc.text = new Automerge.Text()
        doc.text.insertAt(0, "a")
        doc.text.insertAt(1, { attribute: "bold" })
      })
    })

    it("should allow fetching non-textual characters", () => {
      assert.deepEqual(s1.text.get(1), { attribute: "bold" })
      //assert.strictEqual(s1.text.getElemId(1), `3@${Automerge.getActorId(s1)}`)
    })

    it("should include control characters in string length", () => {
      assert.strictEqual(s1.text.length, 2)
      assert.strictEqual(s1.text.get(0), "a")
    })

    it("should replace control characters from toString()", () => {
      assert.strictEqual(s1.text.toString(), "a\uFFFC")
    })

    it("should allow control characters to be updated", () => {
      const s2 = Automerge.change(
        s1,
        doc => (doc.text.get(1)!.attribute = "italic"),
      )
      const s3 = Automerge.load<DocType>(Automerge.save(s2))
      assert.strictEqual(s1.text.get(1).attribute, "bold")
      assert.strictEqual(s2.text.get(1).attribute, "italic")
      assert.strictEqual(s3.text.get(1).attribute, "italic")
    })

    describe("spans interface to Text", () => {
      it("should return a simple string as a single span", () => {
        let s1 = Automerge.change(Automerge.init<DocType>(), doc => {
          doc.text = new Automerge.Text("hello world")
        })
        assert.deepEqual(s1.text.toSpans(), ["hello world"])
      })
      it("should return an empty string as an empty array", () => {
        let s1 = Automerge.change(Automerge.init<DocType>(), doc => {
          doc.text = new Automerge.Text()
        })
        assert.deepEqual(s1.text.toSpans(), [])
      })
      it("should split a span at a control character", () => {
        let s1 = Automerge.change(Automerge.init<DocType>(), doc => {
          doc.text = new Automerge.Text("hello world")
          doc.text.insertAt(5, { attributes: { bold: true } })
        })
        assert.deepEqual(s1.text.toSpans(), [
          "hello",
          { attributes: { bold: true } },
          " world",
        ])
      })
      it("should allow consecutive control characters", () => {
        let s1 = Automerge.change(Automerge.init<DocType>(), doc => {
          doc.text = new Automerge.Text("hello world")
          doc.text.insertAt(5, { attributes: { bold: true } })
          doc.text.insertAt(6, { attributes: { italic: true } })
        })
        assert.deepEqual(s1.text.toSpans(), [
          "hello",
          { attributes: { bold: true } },
          { attributes: { italic: true } },
          " world",
        ])
      })
      it("should allow non-consecutive control characters", () => {
        let s1 = Automerge.change(Automerge.init<DocType>(), doc => {
          doc.text = new Automerge.Text("hello world")
          doc.text.insertAt(5, { attributes: { bold: true } })
          doc.text.insertAt(12, { attributes: { italic: true } })
        })
        assert.deepEqual(s1.text.toSpans(), [
          "hello",
          { attributes: { bold: true } },
          " world",
          { attributes: { italic: true } },
        ])
      })
    })
  })

  it("should support unicode when creating text", () => {
    s1 = Automerge.from({
      text: new Automerge.Text("🐦"),
    })
    assert.strictEqual(s1.text.get(0), "🐦")
    assert.strictEqual(s1.text.toString(), "🐦")

    // this tests the wasm::materialize path
    s2 = Automerge.load(Automerge.save(s1))
    assert.strictEqual(s2.text.toString(), "🐦")

    // this tests the observe_init_state path
    let s3 = Automerge.init<DocType>()
    s3 = Automerge.merge(s3, s2)
    assert.strictEqual(s3.text.toString(), "🐦")

    // this tests the diff_incremental path
    let s4 = Automerge.from({ some: "value" })
    // @ts-ignore
    s4 = Automerge.merge(s4, s2)
    // @ts-ignore
    assert.strictEqual(s4.text.toString(), "🐦")
  })

  it("should let you insert strings", () => {
    s1 = Automerge.from({
      text: new Automerge.Text(""),
    })

    s1 = Automerge.change(s1, d => {
      d.text.insertAt(0, "four")
    })

    assert.strictEqual(s1.text.length, 4)
  })

  it("should index by unicode code points", () => {
    s1 = Automerge.from({
      text: new Automerge.Text(""),
    })

    s1 = Automerge.change(s1, d => {
      d.text.insertAt(0, "🇬🇧")
      d.text.insertAt(2, "four")
    })

    assert.strictEqual(s1.text.length, 6)
    assert.strictEqual(s1.text.toString(), "🇬🇧four")
  })

  it("should allow initiializing with multiple codepoint characters", () => {
    s1 = Automerge.from({
      text: new Automerge.Text("🇺🇸"),
    })
  })

  it("should support slice", () => {
    assert.strictEqual(s1.text.slice(0).toString(), s1.text.toString())
  })
})
