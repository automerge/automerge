import * as Automerge from "../src/index.js"
import { next as AutomergeNext } from "../src/index.js"

describe("The next export", () => {
  it("should expose a next export to maintain backwards compatiblity with 2.0", () => {
    const _doc = AutomergeNext.init()
  })

  it("should have the same types as the main export", () => {
    const _dummy: Omit<typeof Automerge, "next"> = AutomergeNext
  })
})
