import {
  Doc,
  isAutomerge,
  isCounter,
  mark,
  splice,
  unmark,
} from "./implementation.js"
import {
  DelPatch,
  IncPatch,
  InsertPatch,
  MarkPatch,
  Patch,
  Prop,
  PutPatch,
  SpliceTextPatch,
  UnmarkPatch,
} from "./wasm_types.js"

export function applyPatch(doc: Doc<unknown>, patch: Patch) {
  let path = resolvePath(doc, patch.path)
  if (patch.action === "put") {
    applyPutPatch(doc, path, patch)
  } else if (patch.action === "insert") {
    applyInsertPatch(doc, path, patch)
  } else if (patch.action === "del") {
    applyDelPatch(doc, path, patch)
  } else if (patch.action === "splice") {
    applySplicePatch(doc, path, patch)
  } else if (patch.action === "inc") {
    applyIncPatch(doc, path, patch)
  } else if (patch.action === "mark") {
    applyMarkPatch(doc, path, patch)
  } else if (patch.action === "unmark") {
    applyUnmarkPatch(doc, path, patch)
  } else if (patch.action === "conflict") {
    // Ignore conflict patches
  } else {
    throw new RangeError(`unsupported patch: ${patch}`)
  }
}

function applyPutPatch(
  doc: unknown,
  path: ResolvedPathElem[],
  patch: PutPatch,
) {
  let { obj: parent, prop } = pathElemAt(path, -1)
  parent[prop] = patch.value
}

function applyInsertPatch(
  doc: unknown,
  path: ResolvedPathElem[],
  patch: InsertPatch,
) {
  let { obj: parent, prop } = pathElemAt(path, -1)

  if (!Array.isArray(parent)) {
    throw new RangeError(`target is not an array for patch`)
  }
  if (!(typeof prop === "number")) {
    throw new RangeError(`index is not a number for patch`)
  }
  parent.splice(prop, 0, ...patch.values)
}

function applyDelPatch(
  doc: unknown,
  path: ResolvedPathElem[],
  patch: DelPatch,
) {
  let { obj: parent, prop, parentPath } = pathElemAt(path, -1)

  if (!(typeof prop === "number")) {
    throw new RangeError(`index is not a number for patch`)
  }
  if (Array.isArray(parent)) {
    parent.splice(prop, patch.length || 1)
  } else if (typeof parent === "string") {
    if (isAutomerge(doc)) {
      splice(doc as Doc<unknown>, parentPath, prop, patch.length || 1)
    } else {
      let { obj: grandParent, prop: grandParentProp } = pathElemAt(path, -2)
      if (typeof prop !== "number") {
        throw new RangeError(`index is not a number for patch`)
      }
      let target = grandParent[grandParentProp]
      if (target == null || typeof target !== "string") {
        throw new RangeError(`target is not a string for patch`)
      }
      let newString =
        target.slice(0, prop) + target.slice(prop + (patch.length || 1))
      grandParent[grandParentProp] = newString
    }
  } else {
    throw new RangeError(`target is not an array or string for patch`)
  }
}

function applySplicePatch(
  doc: unknown,
  path: ResolvedPathElem[],
  patch: SpliceTextPatch,
) {
  if (isAutomerge(doc)) {
    let { obj: parent, prop, parentPath } = pathElemAt(path, -1)
    if (!(typeof prop === "number")) {
      throw new RangeError(`index is not a number for patch`)
    }
    splice(doc as Doc<unknown>, parentPath, prop, 0, patch.value)
  } else {
    let { obj: parent, prop } = pathElemAt(path, -1)
    let { obj: grandParent, prop: grandParentProp } = pathElemAt(path, -2)
    if (typeof prop !== "number") {
      throw new RangeError(`index is not a number for patch`)
    }
    let target = grandParent[grandParentProp]
    if (target == null || typeof target !== "string") {
      throw new RangeError(`target is not a string for patch`)
    }
    let newString = target.slice(0, prop) + patch.value + target.slice(prop)
    grandParent[grandParentProp] = newString
  }
}

function applyIncPatch(
  doc: unknown,
  path: ResolvedPathElem[],
  patch: IncPatch,
) {
  let { obj: parent, prop } = pathElemAt(path, -1)
  const counter = parent[prop]
  if (isAutomerge(doc)) {
    if (!isCounter(counter)) {
      throw new RangeError(`target is not a counter for patch`)
    }
    counter.increment(patch.value)
  } else {
    if (!(typeof counter === "number")) {
      throw new RangeError(`target is not a number for patch`)
    }
    parent[prop] = counter + patch.value
  }
}

function applyMarkPatch(
  doc: unknown,
  path: ResolvedPathElem[],
  patch: MarkPatch,
) {
  let { obj: parent, prop } = pathElemAt(path, -1)
  if (!isAutomerge(doc)) {
    return
  }
  for (const markSpec of patch.marks) {
    mark(
      doc as Doc<unknown>,
      patch.path,
      // TODO: add mark expansion to patches. This will require emitting
      // the expand values in patches.
      { start: markSpec.start, end: markSpec.end, expand: "none" },
      markSpec.name,
      markSpec.value,
    )
  }
}

function applyUnmarkPatch(
  doc: unknown,
  path: ResolvedPathElem[],
  patch: UnmarkPatch,
) {
  if (!isAutomerge(doc)) {
    return
  }
  unmark(
    doc as Doc<unknown>,
    patch.path,
    { start: patch.start, end: patch.end, expand: "none" },
    patch.name,
  )
}

export function applyPatches(doc: Doc<unknown>, patches: Patch[]) {
  for (const patch of patches) {
    applyPatch(doc, patch)
  }
}

type ResolvedPathElem = {
  obj: any
  prop: Prop
  parentPath: Prop[]
}

/**
 * Walk through a path with an object and for each element in the path return a resolved path element.
 *
 * A resolved path element looks like this:
 *
 * ```typescript
 * {
 *   obj: any,           // The object that this element in the path is a property of
 *   prop: Prop,         // The property within `obj` that this path element points at
 *   parentPath: Prop[]  // The path to `obj` within the original `doc` passed to `resolvePath`
 * }
 * ````
 *
 * For example, given an object like this:
 *
 * ```typescript
 * {
 *      todos: [{ task: "remember the milk"}]
 * }
 * ```
 *
 * Then `resolvePath(doc, ["todos", 0, "task"])` would return:
 *
 * ```typescript
 * [
 *   { obj: { todos: [{ task: "remember the milk"}] }, prop: "todos", parentPath: [] },
 *   { obj: [{ task: "remember the milk"}], prop: 0, parentPath: ["todos"] },
 *   { obj: { task: "remember the milk" }, prop: "task", parentPath: ["todos", 0] }
 * ]
 * ```
 */
function resolvePath(doc: unknown, path: Prop[]): ResolvedPathElem[] {
  const result: ResolvedPathElem[] = []
  let current = doc
  let currentPath: Prop[] = []

  for (const [index, prop] of path.entries()) {
    result.push({ obj: current, prop, parentPath: currentPath.slice() })
    currentPath.push(prop)
    if (index !== path.length - 1) {
      if (current == null || typeof current != "object") {
        // If we're not the last item in the path then we need the current
        // object to be an object so we can access it in the next iteration
        throw new Error(`Invalid path: ${path}`)
      }
      current = current[prop]
    } else {
      break
    }
  }

  return result
}

/**
 * Get an element from a resolved path, throwing an exception if the element does not exist
 *
 * @param resolved - The path to lookup in
 * @param index    - The index of the element to lookup, negative indices search backwards
 */
function pathElemAt(
  resolved: ResolvedPathElem[],
  index: number,
): ResolvedPathElem {
  let result = resolved.at(index)
  if (result == undefined) {
    throw new Error("invalid path")
  }
  return result
}

function reversed<T>(array: T[]): T[] {
  return array.slice().reverse()
}
