import * as assert from "assert"
import * as Automerge from "../src/index.js"

describe("conflicts", () => {
  it("should not allow updating values inside a conflict outside of the change callback", () => {
    // This test is basically checking that we don't accidentally return proxies
    // which can modify the document when outside of a change callback

    // Create a conflicted document
    let doc = Automerge.from({ user: { name: "alice" } })
    let doc2 = Automerge.clone(doc)
    doc = Automerge.change(doc, d => {
      d.user = { name: "bob" }
    })
    doc2 = Automerge.change(doc2, d => {
      d.user = { name: "charlie" }
    })
    doc = Automerge.merge(doc, doc2)

    // Load the conflicts
    let conflicts = Automerge.getConflicts(doc, "user")
    if (!conflicts || !(typeof conflicts === "object")) {
      throw new Error("unable to get conflicts")
    }

    // Modify the conflicted objects returned from getConflicts
    for (const value of Object.values(conflicts)) {
      if (!value || !(typeof value === "object")) {
        continue
      }
      if (Reflect.get(value, "name") === "bob") {
        try {
          Reflect.set(value, "name", "Attila")
        } catch {}
      }
    }

    // Make sure that the conflicts inside the document have not changed
    conflicts = Automerge.getConflicts(doc, "user")
    if (!conflicts || !(typeof conflicts === "object")) {
      throw new Error("unable to get conflicst")
    }
    // We can't use assert.deepStrictEqual directly as it doesn't respect proxy traps
    // so instead we get the names out of the conflicted objects, one of which will
    // have changed in the Reflect.set call above if we did erroneously return proxies
    // from getConflicts above
    let names = Object.values(conflicts).map(c => {
      if (!c || !(typeof c === "object")) {
        throw new Error("conflict should be an object")
      }
      return Reflect.get(c, "name")
    })
    assert.deepStrictEqual(new Set(names), new Set(["charlie", "bob"]))
  })

  it("should allow updating  values inside a conflicted map", () => {
    let doc = Automerge.from({ user: {} })
    let doc2 = Automerge.clone(doc)
    doc2 = Automerge.change(doc2, d => {
      d.user = { name: "alice" }
    })
    let doc3 = Automerge.clone(doc)
    doc3 = Automerge.change(doc3, d => {
      d.user = { name: "charlie" }
    })

    doc = Automerge.change(doc, d => {
      d.user = { name: "bob" }
    })

    doc = Automerge.merge(doc, doc2)
    doc = Automerge.merge(doc, doc3)

    let conflictsBefore = Automerge.getConflicts(doc, "user")
    assert.deepStrictEqual(conflictsBefore, {
      [`2@${Automerge.getActorId(doc)}`]: { name: "bob" },
      [`2@${Automerge.getActorId(doc2)}`]: { name: "alice" },
      [`2@${Automerge.getActorId(doc3)}`]: { name: "charlie" },
    })

    doc = Automerge.change(doc, d => {
      let conflicts = Automerge.getConflicts(d, "user")
      if (conflicts) {
        for (const conflict of Object.values(conflicts)) {
          if (conflict && typeof conflict === "object") {
            conflict["name"] = "Attila"
          }
        }
      }
    })

    let conflictsAfter = Automerge.getConflicts(doc, "user")
    assert.deepStrictEqual(conflictsAfter, {
      [`2@${Automerge.getActorId(doc)}`]: { name: "Attila" },
      [`2@${Automerge.getActorId(doc2)}`]: { name: "Attila" },
      [`2@${Automerge.getActorId(doc3)}`]: { name: "Attila" },
    })
  })

  it("should allow updating  values inside a conflicted list", () => {
    let doc = Automerge.from({ users: [{ name: "ignored" }] })
    let doc2 = Automerge.clone(doc)
    doc2 = Automerge.change(doc2, d => {
      d.users[0] = { name: "alice" }
    })
    let doc3 = Automerge.clone(doc)
    doc3 = Automerge.change(doc3, d => {
      d.users[0] = { name: "charlie" }
    })

    doc = Automerge.change(doc, d => {
      d.users[0] = { name: "bob" }
    })

    doc = Automerge.merge(doc, doc2)
    doc = Automerge.merge(doc, doc3)

    let conflictsBefore = Automerge.getConflicts(doc.users, 0)
    assert.deepStrictEqual(conflictsBefore, {
      [`11@${Automerge.getActorId(doc)}`]: { name: "bob" },
      [`11@${Automerge.getActorId(doc2)}`]: { name: "alice" },
      [`11@${Automerge.getActorId(doc3)}`]: { name: "charlie" },
    })

    doc = Automerge.change(doc, d => {
      let conflicts = Automerge.getConflicts(d.users, 0)
      if (conflicts) {
        for (const conflict of Object.values(conflicts)) {
          if (conflict && typeof conflict === "object") {
            Reflect.set(conflict, "name", "Attila")
          }
        }
      }
    })

    let conflictsAfter = Automerge.getConflicts(doc.users, 0)
    assert.deepStrictEqual(conflictsAfter, {
      [`11@${Automerge.getActorId(doc)}`]: { name: "Attila" },
      [`11@${Automerge.getActorId(doc2)}`]: { name: "Attila" },
      [`11@${Automerge.getActorId(doc3)}`]: { name: "Attila" },
    })
  })
})
