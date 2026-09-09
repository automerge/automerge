import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

describe("anonymize", () => {
  it("returns a loadable document with anonymized data and matching history shape", () => {
    let source = Automerge.from(
      {
        privateName: "Alice",
        privateNotes: ["Monday meeting", "secret follow-up"],
      },
      { actor: "01020304" },
    )
    source = Automerge.change(
      source,
      { message: "private commit message", time: 1_700_000_000 },
      doc => {
        doc.privateName = "Bob"
        doc.privateNotes.push("confidential")
      },
    )

    const anonymized = Automerge.anonymize(source)

    assert.deepStrictEqual(source, {
      privateName: "Bob",
      privateNotes: ["Monday meeting", "secret follow-up", "confidential"],
    })
    assert.notDeepStrictEqual(anonymized, source)
    assert(!Object.keys(anonymized).includes("privateName"))
    assert(!Object.keys(anonymized).includes("privateNotes"))

    const sourceChanges = Automerge.getAllChanges(source).map(
      Automerge.decodeChange,
    )
    const anonymizedChanges = Automerge.getAllChanges(anonymized).map(
      Automerge.decodeChange,
    )
    assert.strictEqual(anonymizedChanges.length, sourceChanges.length)
    assert.deepStrictEqual(
      anonymizedChanges.map(change => change.ops.length),
      sourceChanges.map(change => change.ops.length),
    )
    assert.deepStrictEqual(
      anonymizedChanges.map(change => change.deps.length),
      sourceChanges.map(change => change.deps.length),
    )
    for (const [changeIndex, sourceChange] of sourceChanges.entries()) {
      for (const [opIndex, sourceOp] of sourceChange.ops.entries()) {
        const anonymizedOp = anonymizedChanges[changeIndex].ops[opIndex]
        if (
          typeof sourceOp.value === "string" &&
          sourceOp.value.trim().length > 0
        ) {
          assert.notStrictEqual(anonymizedOp.value, sourceOp.value)
        }
      }
    }
    assert(
      anonymizedChanges.every(
        (change, index) =>
          sourceChanges[index].message === null ||
          change.message !== sourceChanges[index].message,
      ),
    )

    const reloaded = Automerge.load(Automerge.save(anonymized))
    assert.deepStrictEqual(reloaded, anonymized)
  })
})
