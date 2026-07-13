import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

type CounterDoc = { counter: number }

function makeDoc(numChanges = 1500): Automerge.Doc<CounterDoc> {
  let doc = Automerge.from<CounterDoc>({ counter: 0 })
  for (let i = 1; i <= numChanges; i++) {
    doc = Automerge.change(doc, d => {
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
      const headMeta = Automerge.getBackend(doc).getChangeMetaByHash(
        fragment.head,
      )
      assert.ok(headMeta != null)
      assert.ok(fragment.members.includes(`${headMeta.seq}@${headMeta.actor}`))
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
      assert.deepEqual(commits[i].bytes, Automerge.getBackend(doc).bundleFragmentMetadata([commitFragments[i]])[0])
    }

    for (let i = 0; i < fragments.length; i++) {
      const { bytes, ...metadata } = fragments[i]
      assert.deepEqual(metadata, fragmentMetadata[i])
      assert.deepEqual(bytes, Automerge.getBackend(doc).bundleFragmentMetadata([fragmentMetadata[i]])[0])
    }
  })

  it("can reconstruct a document from fragments and commits", () => {
    const doc = makeDoc()
    const commits = Automerge.getCommits(doc)
    const fragments = Automerge.getFragments(doc)

    let loaded = Automerge.init<CounterDoc>()
    loaded = Automerge.addFragments(loaded, fragments)
    loaded = Automerge.addCommits(loaded, commits)

    assert.deepEqual(Automerge.getHeads(loaded).sort(), Automerge.getHeads(doc).sort())
    assert.equal(loaded.counter, 1500)
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
