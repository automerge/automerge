import assert from "assert"
import { beelay } from "../src/entrypoints/fullfat_node.js"

describe("the beelay", () => {
  it("should create a beelay instance", async () => {
    const storage = beelay.createMemoryStorageAdapter()
    const signer = beelay.createMemorySigner()
    const repo = await beelay.loadBeelay({ storage, signer })
    assert.ok(repo)
    repo.stop()
  })
})
