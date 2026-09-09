import * as assert from "assert"
import * as Automerge from "../src/index.js"

describe("Automerge", () => {
  describe("changeAt", () => {
    it("should be able to change a doc at a prior state", () => {
      let doc1 = Automerge.init<{ text: string }>()
      doc1 = Automerge.change(doc1, d => (d.text = "aaabbbccc"))
      const heads1 = Automerge.getHeads(doc1)
      doc1 = Automerge.change(doc1, d => {
        Automerge.splice(d, ["text"], 3, 3, "BBB")
      })
      assert.deepEqual(doc1.text, "aaaBBBccc")
      doc1 = Automerge.changeAt(doc1, heads1, d => {
        assert.deepEqual(d.text, "aaabbbccc")
        Automerge.splice(d, ["text"], 2, 3, "XXX")
        assert.deepEqual(d.text, "aaXXXbccc")
      }).newDoc
      assert.deepEqual(doc1.text, "aaXXXBBBccc")
    })

    it("should leave multiple heads intact on empty changes", () => {
      let doc1 = Automerge.init<{ text: string; [key: string]: string }>()
      doc1 = Automerge.change(doc1, d => (d.text = "aaabbbccc"))
      const headsBeforeFork = Automerge.getHeads(doc1)

      // Create a fork
      let doc2 = Automerge.clone(doc1)
      doc2 = Automerge.change(doc2, d => (d.doc2 = "doc2"))

      doc1 = Automerge.change(doc1, d => (d.doc1 = "doc1"))

      // Merge the fork back in
      doc1 = Automerge.merge(doc1, doc2)

      // We have a forked history
      assert.equal(Automerge.getHeads(doc1).length, 2)

      // now make an empty changeAt
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      doc1 = Automerge.changeAt(doc1, headsBeforeFork, _d => {}).newDoc

      // We didn't do anything, heads shouldn't have changed
      assert.equal(Automerge.getHeads(doc1).length, 2)
    })

    it("should return the heads of the change document from changeAt", () => {
      let doc1 = Automerge.init<{ text: string; [key: string]: string }>()
      doc1 = Automerge.change(doc1, d => (d.text = "aaabbbccc"))

      // Create a fork
      let doc2 = Automerge.clone(doc1)
      doc2 = Automerge.change(doc2, d => (d.doc2 = "doc2"))
      const headsOnFork = Automerge.getHeads(doc2)

      doc1 = Automerge.change(doc1, d => (d.doc1 = "doc1"))
      const doc1Heads = Automerge.getHeads(doc1)

      // Merge the fork back in
      doc1 = Automerge.merge(doc1, doc2)

      // We now have a forked history, we want to changeAt on the first head
      const { newDoc, newHeads } = Automerge.changeAt(doc1, doc1Heads, d => {
        d.text = "changed"
      })
      doc1 = newDoc

      // The heads of the document should now be the heads returned from changeAt,
      // plus the heads of the unchanged fork
      const expectedHeads = new Set([...headsOnFork, ...newHeads!])
      const actualHeads = new Set(Automerge.getHeads(doc1))
      assert.deepEqual(actualHeads, expectedHeads)
    })

    describe("materialization", () => {
      it("materializes a historical nested edit after a concurrent list insertion", () => {
        let doc = Automerge.from({
          items: [
            { id: "a", n: 1 },
            { id: "b", n: 2 },
          ],
        })

        const oldHeads = Automerge.getHeads(doc)

        // Shift "a" from index 0 to index 1 in the live document.
        doc = Automerge.change(doc, d => {
          d.items.unshift({ id: "z", n: 0 })
        })

        // Make a concurrent change based on the historical state, where "a"
        // was still at index 0.
        const result = Automerge.changeAt(doc, oldHeads, d => {
          d.items[0].n = 99
        })

        // The CRDT history contains the correct merged value.
        const rematerialized = Automerge.view(
          result.newDoc,
          Automerge.getHeads(result.newDoc),
        )

        assert.deepStrictEqual(rematerialized.items, [
          { id: "z", n: 0 },
          { id: "a", n: 99 },
          { id: "b", n: 2 },
        ])

        // The snapshot returned by changeAt should contain the same value.
        assert.deepStrictEqual(result.newDoc.items, [
          { id: "z", n: 0 },
          { id: "a", n: 99 },
          { id: "b", n: 2 },
        ])
      })
    })
  })
})
