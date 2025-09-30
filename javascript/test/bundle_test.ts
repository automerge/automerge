import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

describe("the bundle format", () => {
  it("should allow saving and loading a bundle", () => {
    let doc = Automerge.from({ foo: "bar" })
    let startDoc = Automerge.clone(doc)
    const startHeads = Automerge.getHeads(doc)
    doc = Automerge.change(doc, d => {
      d.foo = "baz"
    })
    doc = Automerge.change(doc, d => {
      d.foo = "qux"
    })
    const changeHashes = Automerge.getChangesMetaSince(doc, startHeads).map(
      c => c.hash,
    )
    assert.equal(changeHashes.length, 2)

    const bundle = Automerge.saveBundle(doc, changeHashes)

    startDoc = Automerge.loadIncremental(startDoc, bundle)

    assert.deepStrictEqual(startDoc.foo, "qux")
  })

  it("should allow getting the list of changes in a bundle", () => {
    let doc = Automerge.from({ foo: "bar" })
    const startHeads = Automerge.getHeads(doc)
    doc = Automerge.change(doc, d => {
      d.foo = "baz"
    })
    doc = Automerge.change(doc, d => {
      d.foo = "qux"
    })
    const changeHashes = Automerge.getChangesMetaSince(doc, startHeads).map(
      c => c.hash,
    )
    assert.equal(changeHashes.length, 2)

    const bundle = Automerge.saveBundle(doc, changeHashes)
    const { changes } = Automerge.readBundle(bundle)

    const changesByHash = new Map()
    for (const change of changes) {
      changesByHash.set(change.hash, change)
    }

    for (const hash of changeHashes) {
      const actualChange = Automerge.inspectChange(doc, hash)
      const bundleChange = changesByHash.get(hash)
      assert.deepStrictEqual(bundleChange, actualChange)
    }
  })

  it("should show the dependencies of a bundle", () => {
    let doc = Automerge.from({ foo: "bar" })
    const startHeads = Automerge.getHeads(doc)
    doc = Automerge.change(doc, d => {
      d.foo = "baz"
    })
    const changeHashes = Automerge.getChangesMetaSince(doc, startHeads).map(
      c => c.hash,
    )

    const bundle = Automerge.saveBundle(doc, changeHashes)
    const { deps } = Automerge.readBundle(bundle)

    assert.deepStrictEqual(deps, startHeads)
  })
})
