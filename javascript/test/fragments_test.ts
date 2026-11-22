import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

type CounterDoc = { counter: number }

// Fixed timestamps keep the change hashes, and so the fragment levels,
// the same on every run: these tests need the head to be a level-0 commit.
function makeDoc(
  numChanges = 1500,
  auditMode = true,
): Automerge.Doc<CounterDoc> {
  let doc = Automerge.init<CounterDoc>({ actor: "0123456789abcdef", auditMode })
  for (let i = 0; i <= numChanges; i++) {
    doc = Automerge.change(doc, { time: 0 }, d => {
      d.counter = i
    })
  }
  return doc
}

describe("the fragments API", () => {
  it("returns fragment metadata with level filtering and lookup", () => {
    const doc = makeDoc()

    const allFragments = Automerge.getFragments(doc, { bytes: false })
    const commits = Automerge.getFragments(doc, { levels: 0, bytes: false })
    const bundledFragments = Automerge.getFragments(doc, {
      levels: { start: 1 },
      bytes: false,
    })

    assert.ok(allFragments.length > 0)
    assert.ok(commits.length > 0)
    assert.ok(bundledFragments.length > 0)
    assert.equal(allFragments.length, commits.length + bundledFragments.length)
    assert.ok(commits.every(fragment => fragment.level === 0))
    assert.ok(bundledFragments.every(fragment => fragment.level > 0))

    for (const fragment of allFragments) {
      assert.equal(fragment.head.length, 64)
      assert.equal(fragment.level, leadingZeroBytes(fragment.head))
      assert.ok(fragment.members.includes(fragment.head))
      assert.deepEqual(
        Automerge.getFragment(doc, fragment.head, { bytes: false }),
        fragment,
      )
    }

    assert.equal(Automerge.getFragment(doc, "ff".repeat(32)), null)
  })

  it("exports fragments with matching bytes", () => {
    const doc = makeDoc()

    const fragmentMetadata = Automerge.getFragments(doc, { bytes: false })
    const fragments = Automerge.getFragments(doc)

    assert.equal(fragments.length, fragmentMetadata.length)
    assert.ok(fragments.some(fragment => fragment.level === 0))
    assert.ok(fragments.some(fragment => fragment.level > 0))

    for (let i = 0; i < fragments.length; i++) {
      const { bytes, ...metadata } = fragments[i]
      assert.deepEqual(metadata, fragmentMetadata[i])
      assert.deepEqual(
        bytes,
        Automerge.getBackend(doc).bundleFragmentMetadata([
          fragmentMetadata[i],
        ])[0],
      )
      assert.deepEqual(Automerge.getFragment(doc, fragments[i].head), {
        ...metadata,
        bytes,
      })
    }
  })

  it("excludes fragment heads and boundaries from checkpoints", () => {
    const doc = makeDoc()
    const fragments = Automerge.getFragments(doc, { levels: { start: 1 } })

    assert.ok(fragments.length > 0)
    assert.ok(fragments.some(fragment => fragment.boundary.length > 0))
    for (const fragment of fragments) {
      assert.ok(fragment.members.includes(fragment.head))
      assert.ok(!fragment.checkpoints.includes(fragment.head))
      for (const boundary of fragment.boundary) {
        assert.ok(!fragment.checkpoints.includes(boundary))
      }
      assert.deepEqual(
        Automerge.getFragment(doc, fragment.head)?.checkpoints,
        fragment.checkpoints,
      )
    }
  })

  it("can reconstruct a document from its fragments", () => {
    const doc = makeDoc()

    const loaded = Automerge.applyFragments(
      Automerge.init<CounterDoc>(),
      Automerge.getFragments(doc),
    )

    assert.deepEqual(
      Automerge.getHeads(loaded).sort(),
      Automerge.getHeads(doc).sort(),
    )
    assert.equal(loaded.counter, 1500)
  })

  for (const auditMode of [true, false]) {
    it(`brings a document up to date from the fragments after its heads (audit mode ${auditMode})`, () => {
      const doc = makeDoc(1500, auditMode)
      assert.equal(Automerge.isAuditMode(doc), auditMode)
      let behind = Automerge.clone(doc, { actor: "fedcba9876543210" })
      const heads = Automerge.getHeads(behind)
      let ahead = doc
      for (let i = 0; i < 300; i++) {
        ahead = Automerge.change(ahead, { time: 0 }, d => {
          d.counter = 2000 + i
        })
      }
      behind = Automerge.change(behind, { time: 0 }, d => {
        d.counter = -1
      })

      assert.deepEqual(
        Automerge.getFragments(ahead, { after: Automerge.getHeads(ahead) }),
        [],
      )
      const fragments = Automerge.getFragments(ahead, { after: heads })
      assert.ok(fragments.length > 0)
      const { bytes: _bytes, ...first } = fragments[0]
      assert.deepEqual(
        Automerge.getFragments(ahead, { after: heads, bytes: false })[0],
        first,
      )

      behind = Automerge.applyFragments(behind, fragments)
      const merged = Automerge.merge(
        Automerge.clone(ahead),
        Automerge.clone(behind),
      )
      assert.deepEqual(
        Automerge.getHeads(behind).sort(),
        Automerge.getHeads(merged).sort(),
      )
    })
  }

  it("lists every fragment after empty heads", () => {
    const doc = makeDoc()
    assert.deepEqual(
      Automerge.getFragments(doc, { after: [], bytes: false })
        .map(f => f.head)
        .sort(),
      Automerge.getFragments(doc, { bytes: false })
        .map(f => f.head)
        .sort(),
    )
  })

  for (const auditMode of [true, false]) {
    it(`lists each change after heads once when partial (auditMode: ${auditMode})`, () => {
      let doc = makeDoc(700, auditMode)
      const heads = Automerge.getHeads(doc)
      const behind = Automerge.clone(doc)
      for (let i = 0; i < 800; i++) {
        doc = Automerge.change(doc, { time: 0 }, d => {
          d.counter = 1000 + i
        })
      }
      const members = (partial: boolean) =>
        Automerge.getFragments(doc, {
          after: heads,
          partial,
          bytes: false,
        }).flatMap(f => f.members)
      const once = members(true)
      assert.equal(once.length, 800)
      assert.equal(new Set(once).size, once.length)
      assert.ok(members(false).length > 800, "a fragment straddles heads")

      const fragments = Automerge.getFragments(doc, {
        after: heads,
        partial: true,
      })
      assert.deepEqual(
        fragments.flatMap(f => f.members),
        once,
      )
      const caughtUp = Automerge.applyFragments(behind, fragments)
      assert.deepEqual(Automerge.getHeads(caughtUp), Automerge.getHeads(doc))
    })
  }

  it("rejects levels combined with after or partial", () => {
    const doc = makeDoc(10)
    assert.throws(
      () =>
        // @ts-expect-error levels and after are exclusive
        Automerge.getFragments(doc, { levels: 0, after: [] }),
      /levels cannot be combined/,
    )
  })

  for (const auditMode of [true, false]) {
    it(`converges two peers through syncFragments (auditMode: ${auditMode})`, () => {
      let a = makeDoc(700, auditMode)
      let b = Automerge.clone(a, { actor: "fedcba9876543210" })
      for (let i = 0; i < 600; i++) {
        a = Automerge.change(a, { time: 0 }, d => {
          d.counter = 2000 + i
        })
        b = Automerge.change(b, { time: 0 }, d => {
          d.counter = 3000 + i
        })
      }
      const sources: Automerge.PatchSource[] = []
      const patchCallback = (
        _: unknown,
        info: Automerge.PatchInfo<CounterDoc>,
      ) => sources.push(info.source)

      let toB: Automerge.Fragment[], toA: Automerge.Fragment[]
      ;[a, toB] = Automerge.syncFragments(a, [])
      ;[b, toA] = Automerge.syncFragments(b, toB, { patchCallback })
      ;[a, toB] = Automerge.syncFragments(a, toA, { patchCallback })
      ;[b, toA] = Automerge.syncFragments(b, toB)

      assert.deepEqual(
        Automerge.getHeads(a).sort(),
        Automerge.getHeads(b).sort(),
      )
      assert.equal(a.counter, b.counter)
      assert.deepEqual(toA, [])
      assert.deepEqual(sources, ["syncFragments", "syncFragments"])
    })
  }

  it("reports applyFragments as the patch source", () => {
    const doc = makeDoc()
    const sources: Automerge.PatchSource[] = []

    const loaded = Automerge.applyFragments(
      Automerge.init<CounterDoc>(),
      Automerge.getFragments(doc),
      { patchCallback: (_patches, info) => sources.push(info.source) },
    )

    assert.deepEqual(sources, ["applyFragments"])
    assert.equal(loaded.counter, 1500)
  })
})

function leadingZeroBytes(hash: string): number {
  let level = 0
  for (let i = 0; i < hash.length; i += 2) {
    if (hash.slice(i, i + 2) !== "00") {
      break
    }
    level++
  }
  return level
}
