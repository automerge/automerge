import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

describe("view inside change", () => {
  it("keeps edits made before view() in the object returned by change()", () => {
    let doc = Automerge.from<{
      formState: Record<string, { a?: number; b?: number }>
    }>({ formState: {} })
    const heads = Automerge.getHeads(doc)

    doc = Automerge.change(doc, "probe", (draft: any) => {
      draft.formState.before = { a: 1 }
      Automerge.view(draft, heads)
      draft.formState.after = { b: 2 }
    })

    assert.deepStrictEqual(doc.formState, {
      before: { a: 1 },
      after: { b: 2 },
    })
  })

  it("sees earlier edits of the same change when view() is called without heads", () => {
    let doc = Automerge.from<{ links: { id: number }[] }>({
      links: [{ id: 1 }, { id: 2 }, { id: 3 }],
    })

    doc = Automerge.change(doc, "remove links 1 and 2", (draft: any) => {
      ;[1, 2].forEach(id => {
        const view = (Automerge as any).view(draft)
        const index = view.links.findIndex((link: any) => link.id === id)
        draft.links.splice(index, 1)
      })
    })

    assert.deepStrictEqual(doc.links, [{ id: 3 }])
  })

  it("keeps changeAt edits when view() runs before the first write", () => {
    let doc = Automerge.from<{ list: string[] }>({ list: ["a", "b", "c"] })
    const heads = Automerge.getHeads(doc)

    doc = Automerge.change(doc, "later", (d: any) => {
      d.list.splice(0, 1)
    })

    const { newDoc } = Automerge.changeAt(
      doc,
      heads,
      "at old heads",
      (d: any) => {
        ;(Automerge as any).view(d)
        d.list.splice(2, 1)
      },
    )

    assert.deepStrictEqual(newDoc.list, ["b"])
  })
})
