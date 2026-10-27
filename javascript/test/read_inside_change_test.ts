import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"
import type { Doc, Heads, Patch } from "../src/entrypoints/fullfat_node.js"

type TestDoc = { f: Record<string, number>; g?: number }

type Context = {
  // The heads the change started from
  heads: Heads
  // The committed changes before the change
  changes: Automerge.Change[]
}

type Read = {
  name: string
  read: (target: Doc<TestDoc>, ctx: Context) => void
  // How a call inside the callback behaves
  //  - "reads": returns without committing the pending operations
  //  - "throws": throws a RangeError and leaves the change alone
  //  - "splits": commits the pending operations, so the edits before it
  //    become a change of their own
  behaviour: "reads" | "throws" | "splits"
  // Whether the call relies on materialize() not committing, see
  // materializeCommits()
  materializes?: boolean
}

// Until https://github.com/automerge/automerge/pull/1606 lands (#1598),
// materialize() (used by view() and toJS()) commits the pending operations and
// moves the diff cursor past them, so the document returned by change() loses
// the edits made before the call. The tests of those calls are skipped until
// then.
function materializeCommits(): boolean {
  const handle = Automerge.getBackend(Automerge.init())
  handle.put("_root", "x", 1)
  handle.materialize("/")
  return handle.pendingOps() === 0
}

const reads: Read[] = [
  {
    name: "getHeads",
    read: (target, ctx) => {
      assert.deepStrictEqual(Automerge.getHeads(target), ctx.heads)
    },
    behaviour: "reads",
  },
  {
    name: "getAllChanges",
    read: (target, ctx) => {
      assert.deepStrictEqual(Automerge.getAllChanges(target), ctx.changes)
    },
    behaviour: "reads",
  },
  {
    name: "getChanges",
    read: (target, ctx) => {
      assert.deepStrictEqual(
        Automerge.getChanges(Automerge.init(), target),
        ctx.changes,
      )
    },
    behaviour: "reads",
  },
  {
    name: "getChangesSince",
    read: (target, ctx) => {
      assert.deepStrictEqual(Automerge.getChangesSince(target, []), ctx.changes)
    },
    behaviour: "reads",
  },
  {
    name: "save",
    read: target => Automerge.save(target),
    behaviour: "throws",
  },
  {
    name: "saveIncremental",
    read: target => Automerge.saveIncremental(target),
    behaviour: "throws",
  },
  {
    name: "saveSince",
    read: (target, ctx) => Automerge.saveSince(target, ctx.heads),
    behaviour: "throws",
  },
  {
    name: "getLastLocalChange",
    read: target => Automerge.getLastLocalChange(target),
    behaviour: "throws",
  },
  {
    name: "clone",
    read: target => Automerge.clone(target),
    behaviour: "throws",
  },
  {
    name: "view",
    read: (target, ctx) => Automerge.view(target, ctx.heads),
    behaviour: "reads",
    materializes: true,
  },
  {
    name: "toJS",
    read: target => Automerge.toJS(target),
    behaviour: "reads",
    materializes: true,
  },
  {
    name: "getBackend(d).getHeads",
    read: target => Automerge.getBackend(target).getHeads(),
    behaviour: "splits",
  },
]

const NESTED = /cannot be nested inside a change callback/

// The changes in `doc` which are not dependencies of `heads`
function changesSince(doc: Doc<TestDoc>, heads: Heads) {
  return Automerge.getChangesSince(doc, heads).map(Automerge.decodeChange)
}

function patchedPaths(patches: Patch[]): string[] {
  return patches.map(p => p.path.join("/")).sort()
}

describe("reading inside a change callback", () => {
  for (const { name, read, behaviour, materializes } of reads) {
    for (const on of ["draft", "doc"] as const) {
      describe(`${name} on the ${on}`, () => {
        // Run `read` between two writes, check that the result contains both
        // writes and return the new changes and the patches
        function check(
          doc: Doc<TestDoc>,
          run: (
            options: Automerge.ChangeOptions<TestDoc>,
            callback: Automerge.ChangeFn<TestDoc>,
          ) => Doc<TestDoc>,
          heads: Heads,
        ) {
          const patches: Patch[] = []
          const ctx = { heads, changes: Automerge.getAllChanges(doc) }
          const headsBefore = Automerge.getHeads(doc)
          const expected = behaviour
          const newDoc = run(
            {
              message: "probe",
              time: 1234,
              patchCallback: p => patches.push(...p),
            },
            d => {
              d.f.a = 1
              const target = on === "draft" ? d : doc
              if (expected === "throws") {
                assert.throws(() => read(target, ctx), NESTED)
              } else {
                read(target, ctx)
              }
              d.f.b = 2
            },
          )

          assert.deepStrictEqual(newDoc.f, { a: 1, b: 2 })
          // changeAt also reports leaving and re-entering the current heads
          // (removing and restoring `g`)
          assert.deepStrictEqual(
            patchedPaths(patches).filter(p => p.startsWith("f")),
            ["f/a", "f/b"],
          )

          const changes = changesSince(newDoc, headsBefore)
          if (expected === "splits") {
            assert.deepStrictEqual(
              changes.map(c => c.message),
              [null, "probe"],
            )
            assert.strictEqual(changes[1].time, 1234)
            assert.deepStrictEqual(changes[1].deps, [changes[0].hash])
            assert.deepStrictEqual(changes[0].deps, heads)
          } else {
            assert.deepStrictEqual(
              changes.map(c => [c.message, c.time]),
              [["probe", 1234]],
            )
            assert.deepStrictEqual(changes[0].deps, heads)
          }
          return { newDoc, changes }
        }

        it("inside change", function () {
          if (materializes && materializeCommits()) this.skip()
          const doc = Automerge.from<TestDoc>({ f: {} })
          const heads = Automerge.getHeads(doc)
          const { newDoc, changes } = check(
            doc,
            (options, callback) => Automerge.change(doc, options, callback),
            heads,
          )
          assert.deepStrictEqual(Automerge.getHeads(newDoc), [
            changes[changes.length - 1].hash,
          ])
        })

        it("inside changeAt", function () {
          if (materializes && materializeCommits()) this.skip()
          let doc = Automerge.from<TestDoc>({ f: {} })
          const scope = Automerge.getHeads(doc)
          doc = Automerge.change(doc, d => {
            d.g = 1
          })
          const headsBefore = Automerge.getHeads(doc)
          let newHeads: Heads | null = null
          const { newDoc, changes } = check(
            doc,
            (options, callback) => {
              const result = Automerge.changeAt(doc, scope, options, callback)
              newHeads = result.newHeads
              return result.newDoc
            },
            scope,
          )
          assert.strictEqual(newDoc.g, 1)
          assert.deepStrictEqual(newHeads, [changes[changes.length - 1].hash])
          assert.deepStrictEqual(
            [...Automerge.getHeads(newDoc)].sort(),
            [...headsBefore, ...newHeads!].sort(),
          )
        })

        it("in a callback which only reads", () => {
          const doc = Automerge.from<TestDoc>({ f: { a: 1 } })
          const ctx = {
            heads: Automerge.getHeads(doc),
            changes: Automerge.getAllChanges(doc),
          }
          const expected = behaviour
          const newDoc = Automerge.change(doc, d => {
            const target = on === "draft" ? d : doc
            if (expected === "throws") {
              assert.throws(() => read(target, ctx), NESTED)
            } else {
              read(target, ctx)
            }
          })
          assert.strictEqual(newDoc, doc)
          assert.deepStrictEqual(Automerge.getHeads(doc), ctx.heads)
          assert.deepStrictEqual(Automerge.getAllChanges(doc), ctx.changes)
        })
      })
    }
  }

  it("keeps the edits and the message in the reproduction of #1607", () => {
    let doc = Automerge.from<TestDoc>({ f: {} })
    doc = Automerge.change(doc, { message: "probe" }, d => {
      d.f.a = 1
      Automerge.getHeads(d)
    })
    assert.deepStrictEqual(doc.f, { a: 1 })
    assert.deepStrictEqual(
      Automerge.getAllChanges(doc).map(c => Automerge.decodeChange(c).message),
      [null, "probe"],
    )
  })

  it("does not split the change when saving between writes", () => {
    let doc = Automerge.from<TestDoc>({ f: {} })
    doc = Automerge.change(doc, "probe", d => {
      d.f.a = 1
      assert.throws(() => Automerge.save(d), NESTED)
      d.f.b = 2
    })
    assert.deepStrictEqual(doc.f, { a: 1, b: 2 })
    assert.deepStrictEqual(
      Automerge.getAllChanges(doc).map(c => Automerge.decodeChange(c).message),
      [null, "probe"],
    )
    // Saving works again after the change
    const loaded = Automerge.load<TestDoc>(Automerge.save(doc))
    assert.deepStrictEqual(loaded.f, { a: 1, b: 2 })
  })

  it("keeps the edits when the callback commits all of them itself", () => {
    const doc = Automerge.from<TestDoc>({ f: {} })
    const patches: Patch[] = []
    const heads = Automerge.getHeads(doc)
    const newDoc = Automerge.change(
      doc,
      { message: "probe", patchCallback: p => patches.push(...p) },
      d => {
        d.f.a = 1
        Automerge.getBackend(d).commit("early")
      },
    )
    assert.notStrictEqual(newDoc, doc)
    assert.deepStrictEqual(newDoc.f, { a: 1 })
    assert.deepStrictEqual(patchedPaths(patches), ["f/a"])
    assert.deepStrictEqual(
      changesSince(newDoc, heads).map(c => c.message),
      ["early"],
    )
  })

  it("returns the heads of the last change from changeAt when the callback commits", () => {
    let doc = Automerge.from<TestDoc>({ f: {} })
    const scope = Automerge.getHeads(doc)
    doc = Automerge.change(doc, d => {
      d.g = 1
    })
    const heads = Automerge.getHeads(doc)
    const { newDoc, newHeads } = Automerge.changeAt(doc, scope, "probe", d => {
      d.f.a = 1
      Automerge.getBackend(d).commit("early")
    })
    const changes = changesSince(newDoc, heads)
    assert.deepStrictEqual(
      changes.map(c => c.message),
      ["early"],
    )
    assert.deepStrictEqual(changes[0].deps, scope)
    assert.deepStrictEqual(newHeads, [changes[0].hash])
    assert.deepStrictEqual(newDoc.f, { a: 1 })
    assert.strictEqual(newDoc.g, 1)
  })

  it("rolls back only the uncommitted edits when the callback throws", () => {
    const doc = Automerge.from<TestDoc>({ f: {} })
    assert.throws(() => {
      Automerge.change(doc, d => {
        d.f.a = 1
        assert.deepStrictEqual(Automerge.getHeads(d), Automerge.getHeads(doc))
        throw new Error("boom")
      })
    }, /boom/)
    assert.deepStrictEqual(doc.f, {})
    // The document is still usable, and getHeads no longer pretends to be
    // inside a change
    const newDoc = Automerge.change(doc, d => {
      d.f.b = 2
    })
    assert.deepStrictEqual(newDoc.f, { b: 2 })
    assert.deepStrictEqual(Automerge.getAllChanges(newDoc).length, 2)
    assert.doesNotThrow(() => Automerge.save(newDoc))
  })

  it("does not affect other documents", () => {
    const other = Automerge.from<TestDoc>({ f: { x: 1 } })
    let doc = Automerge.from<TestDoc>({ f: {} })
    doc = Automerge.change(doc, d => {
      d.f.a = 1
      assert.doesNotThrow(() => Automerge.save(other))
      assert.doesNotThrow(() => Automerge.clone(other))
      assert.strictEqual(Automerge.getAllChanges(other).length, 1)
    })
    assert.strictEqual(Automerge.getAllChanges(doc).length, 2)
  })

  it("leaves the errors of nested changes alone", () => {
    const doc = Automerge.from<TestDoc>({ f: {} })
    const heads = Automerge.getHeads(doc)
    const newDoc = Automerge.change(doc, "probe", d => {
      d.f.a = 1
      assert.throws(
        () => Automerge.change(d, d2 => (d2.f.b = 2)),
        /Calls to Automerge.change cannot be nested/,
      )
      assert.throws(
        () => Automerge.change(doc, d2 => (d2.f.b = 2)),
        /Attempting to change an outdated document/,
      )
      assert.throws(
        () => Automerge.emptyChange(d),
        /Calls to Automerge.change cannot be nested/,
      )
      assert.throws(
        () => Automerge.emptyChange(doc),
        /Attempting to change an outdated document/,
      )
      d.f.b = 2
    })
    assert.deepStrictEqual(newDoc.f, { a: 1, b: 2 })
    assert.deepStrictEqual(
      changesSince(newDoc, heads).map(c => c.message),
      ["probe"],
    )
    const emptied = Automerge.emptyChange(newDoc, "empty")
    assert.deepStrictEqual(
      changesSince(emptied, Automerge.getHeads(newDoc)).map(c => [
        c.message,
        c.ops.length,
      ]),
      [["empty", 0]],
    )
  })
})
