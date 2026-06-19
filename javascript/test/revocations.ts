import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

describe("revocations", () => {
  const author = "07".repeat(32)

  it("hides and restores changes by author", () => {
    let doc = Automerge.init<any>({ author })
    doc = Automerge.change(doc, d => {
      d.value = "visible"
    })

    const revoked = Automerge.revoke(doc, author, [])
    assert.equal(revoked.value, undefined)
    assert.deepEqual(Automerge.getHeads(revoked), Automerge.getHeads(doc))

    const unrevoked = Automerge.unrevoke(revoked, author)
    assert.equal(unrevoked.value, "visible")
  })
})
