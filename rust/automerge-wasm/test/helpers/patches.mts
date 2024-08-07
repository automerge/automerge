import type { Patch, PutPatch, DelPatch, SpliceTextPatch, IncPatch, InsertPatch, MarkPatch, UnmarkPatch, ConflictPatch } from "../../nodejs/automerge_wasm.cjs"

export type SimplePatch =
  Omit<PutPatch, "taggedValue"> |
  DelPatch |
  SpliceTextPatch |
  IncPatch |
  Omit<InsertPatch, "taggedValues"> |
  MarkPatch |
  UnmarkPatch |
  ConflictPatch;

// Remove the 'taggedValue' and 'taggedValues' fields from the patches for ease
// of testing
export function simplePatches(patches: Patch[]): SimplePatch[] {
  return patches.map(simplePatch)
}

// Remove the 'taggedValue' and 'taggedValues' fields from the patches for ease
// of testing
export function simplePatch(patch: Patch): SimplePatch {
  if (patch.action === "put") {
    let result: SimplePatch = {
      action: "put",
      path: patch.path,
      value: patch.value,
    }
    if (patch.conflict !== undefined) {
      result.conflict = patch.conflict
    }
    return result
  } else if (patch.action === "insert") {
    let result: SimplePatch = {
      action: 'insert',
      path: patch.path,
      values: patch.values,
    }
    if (patch.marks !== undefined) {
      result.marks = patch.marks
    }
    if (patch.conflicts !== undefined) {
      result.conflicts = patch.conflicts
    }
    return result
  } else {
    return patch
  }
}
