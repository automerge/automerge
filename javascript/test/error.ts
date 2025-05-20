import * as Automerge from "../src/index.js"
import { default as assert } from "assert"

describe("Automerge errors", () => {
  it("proxy handler throws an error, not a string", () => {
    let error
    try {
      Automerge.change(
        Automerge.from({ d: ["test"] }),
        doc => (doc.d[2] = "oops"),
      )
    } catch (err) {
      error = err
    }

    assert(error instanceof Error)
  })
})
