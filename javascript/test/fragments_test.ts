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

    const allFragments = Automerge.getFragmentMetadata(doc)
    const commits = Automerge.getFragmentMetadata(doc, 0)
    const bundledFragments = Automerge.getFragmentMetadata(doc, { start: 1 })

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
      assert.deepEqual(Automerge.getFragmentMeta(doc, fragment.head), fragment)
    }

    assert.equal(Automerge.getFragmentMeta(doc, "ff".repeat(32)), null)
  })

  it("exports commit and fragment inputs with matching bytes", () => {
    const doc = makeDoc()

    const commitFragments = Automerge.getFragmentMetadata(doc, 0)
    const commits = Automerge.getCommits(doc)
    const fragmentMetadata = Automerge.getFragmentMetadata(doc, { start: 1 })
    const fragments = Automerge.getFragments(doc)

    assert.equal(commits.length, commitFragments.length)
    assert.equal(fragments.length, fragmentMetadata.length)
    assert.ok(fragments.length > 0)

    for (let i = 0; i < commits.length; i++) {
      assert.equal(commits[i].head, commitFragments[i].head)
      assert.deepEqual(commits[i].parents, commitFragments[i].boundary)
      assert.deepEqual(
        commits[i].bytes,
        Automerge.getBackend(doc).bundleFragmentMetadata([
          commitFragments[i],
        ])[0],
      )
    }

    for (let i = 0; i < fragments.length; i++) {
      const { bytes, ...metadata } = fragments[i]
      assert.deepEqual(metadata, fragmentMetadata[i])
      assert.deepEqual(
        bytes,
        Automerge.getBackend(doc).bundleFragmentMetadata([
          fragmentMetadata[i],
        ])[0],
      )
    }
  })

  it("excludes fragment heads and boundaries from checkpoints", () => {
    const doc = makeDoc()
    const fragments = Automerge.getFragments(doc)

    assert.ok(fragments.length > 0)
    assert.ok(fragments.some(fragment => fragment.boundary.length > 0))
    for (const fragment of fragments) {
      assert.ok(fragment.members.includes(fragment.head))
      assert.ok(!fragment.checkpoints.includes(fragment.head))
      for (const boundary of fragment.boundary) {
        assert.ok(!fragment.checkpoints.includes(boundary))
      }
      assert.deepEqual(
        Automerge.getFragmentMeta(doc, fragment.head)?.checkpoints,
        fragment.checkpoints,
      )
    }
  })

  it("can reconstruct a document from fragments and commits", () => {
    const doc = makeDoc()
    const commits = Automerge.getCommits(doc)
    const fragments = Automerge.getFragments(doc)

    let loaded = Automerge.init<CounterDoc>()
    loaded = Automerge.addFragments(loaded, fragments)
    loaded = Automerge.addCommits(loaded, commits)

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
        Automerge.getFragmentsAfter(ahead, Automerge.getHeads(ahead)),
        [],
      )
      const fragments = Automerge.getFragmentsAfter(ahead, heads)
      assert.ok(fragments.length > 0)
      const { bytes: _bytes, ...first } = fragments[0]
      assert.deepEqual(
        Automerge.getFragmentMetadataAfter(ahead, heads)[0],
        first,
      )

      behind = Automerge.addFragments(behind, fragments)
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
      Automerge.getFragmentMetadataAfter(doc, [])
        .map(f => f.head)
        .sort(),
      Automerge.getFragmentMetadata(doc)
        .map(f => f.head)
        .sort(),
    )
  })

  it("rejects commits whose metadata does not match their bytes", () => {
    const doc = makeDoc()
    const [commit, other] = Automerge.getCommits(doc)
    const loaded = Automerge.addFragments(
      Automerge.init<CounterDoc>(),
      Automerge.getFragments(doc),
    )

    assert.throws(
      () => Automerge.addCommits(loaded, [{ ...commit, head: other.head }]),
      /head mismatch/,
    )
    assert.throws(
      () => Automerge.addCommits(loaded, [{ ...commit, parents: [] }]),
      /parents do not match/,
    )
  })

  it("reports addCommits and addFragments patch sources", () => {
    const doc = makeDoc()
    const commits = Automerge.getCommits(doc)
    const fragments = Automerge.getFragments(doc)
    const sources: Automerge.PatchSource[] = []

    let loaded = Automerge.init<CounterDoc>()
    loaded = Automerge.addFragments(loaded, fragments, {
      patchCallback: (_patches, info) => sources.push(info.source),
    })
    loaded = Automerge.addCommits(loaded, commits, {
      patchCallback: (_patches, info) => sources.push(info.source),
    })

    assert.ok(sources.includes("addFragments"))
    assert.ok(sources.includes("addCommits"))
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
