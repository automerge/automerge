import assert from "assert"
import { next as A } from "../src/index.js"

describe("the beelay", () => {
  it("should save and load a document", async () => {
    const beelay = new A.beelay.Beelay({
      peerId: "abcdef",
      storage: new DummyStorageAdapter(),
    })
    const doc = A.from({ foo: "bar" })
    const docId = await beelay.createDocument()
    const commits = A.getAllChanges(doc).map(change => {
      const c = A.decodeChange(change)
      return {
        parents: c.deps,
        hash: c.hash,
        contents: change,
      }
    })
    await beelay.addCommits({ docId, commits })
    const content = (await beelay.loadDocument(docId)).map(
      (i: A.beelay.Commit | A.beelay.Bundle) => {
        return i.contents
      },
    )

    let loaded = A.init<{ foo: string }>()
    for (const change of content) {
      loaded = A.loadIncremental(doc, change)
    }
    assert.deepStrictEqual(loaded.foo, "bar")
  })
})

class DummyStorageAdapter {
  private storage = new Map<string, Uint8Array>()

  async load(key: string[]): Promise<Uint8Array | undefined> {
    return this.storage.get(key.join("/"))
  }

  async save(key: string[], data: Uint8Array): Promise<void> {
    this.storage.set(key.join("/"), data)
  }

  async remove(key: string[]): Promise<void> {
    this.storage.delete(key.join("/"))
  }

  async loadRange(
    prefix: string[],
  ): Promise<{ key: string[]; data: Uint8Array | undefined }[]> {
    const prefixStr = prefix.join("/")
    const results: { key: string[]; data: Uint8Array | undefined }[] = []

    for (const [key, value] of this.storage.entries()) {
      if (key.startsWith(prefixStr)) {
        results.push({
          key: key.split("/"),
          data: value,
        })
      }
    }

    return results
  }
}
