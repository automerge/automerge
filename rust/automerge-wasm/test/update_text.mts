import { describe, it } from 'mocha';
import assert from 'assert'
import { create } from '../nodejs/automerge_wasm.cjs'

describe('update_text', () => {
  it("should calculate a diff when updating text", () => {
    const doc1 = create()
    const text = doc1.putObject("_root", "text", "");
    doc1.splice(text, 0, 0, "Hello world!")

    const doc2 = doc1.fork()
    doc2.updateText(text, "Goodbye world!")

    doc1.updateText(text, "Hello friends!")
    doc1.merge(doc2)
    assert.strictEqual(doc1.text(text), "Goodbye friends!")
  })

  it("should handle multi character grapheme clusters", () => {
    const doc1 = create({actor: "aaaaaa"})
    const text = doc1.putObject("_root", "text", "");
    doc1.splice(text, 0, 0, "left👨‍👩‍👦right")

    const doc2 = doc1.fork("bbbbbb")
    doc2.updateText(text, "left👨‍👩‍👧right");

    doc1.updateText(text, "left👨‍👩‍👦‍👦right");
    doc1.merge(doc2)
    assert.strictEqual(doc1.text(text), "left👨‍👩‍👧👨‍👩‍👦‍👦right")
  })
})
