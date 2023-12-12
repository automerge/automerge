import { Counter, type AutomergeValue } from "./types.ts"
import { Text } from "./text.ts"
import { type AutomergeValue as NextAutomergeValue } from "./next_types.ts"
import { type Target, Text1Target, Text2Target } from "./proxies.ts"
import { mapProxy, listProxy, ValueType } from "./proxies.ts"
import type { Prop, ObjID } from "https://deno.land/x/automerge_wasm@0.7.0/index.d.ts";
import { Automerge } from "https://deno.land/x/automerge_wasm@0.7.0/automerge_wasm.js";

export type ConflictsF<T extends Target> = { [key: string]: ValueType<T> }
export type Conflicts = ConflictsF<Text1Target>
export type UnstableConflicts = ConflictsF<Text2Target>

export function stableConflictAt(
  context: Automerge,
  objectId: ObjID,
  prop: Prop,
): Conflicts | undefined {
  return conflictAt<Text1Target>(
    context,
    objectId,
    prop,
    true,
    (context: Automerge, conflictId: ObjID): AutomergeValue => {
      return new Text(context.text(conflictId))
    },
  )
}

export function unstableConflictAt(
  context: Automerge,
  objectId: ObjID,
  prop: Prop,
): UnstableConflicts | undefined {
  return conflictAt<Text2Target>(
    context,
    objectId,
    prop,
    true,
    (context: Automerge, conflictId: ObjID): NextAutomergeValue => {
      return context.text(conflictId)
    },
  )
}

function conflictAt<T extends Target>(
  context: Automerge,
  objectId: ObjID,
  prop: Prop,
  textV2: boolean,
  handleText: (a: Automerge, conflictId: ObjID) => ValueType<T>,
): ConflictsF<T> | undefined {
  const values = context.getAll(objectId, prop)
  if (values.length <= 1) {
    return
  }
  const result: ConflictsF<T> = {}
  for (const fullVal of values) {
    switch (fullVal[0]) {
      case "map":
        result[fullVal[1]] = mapProxy<T>(context, fullVal[1], textV2, [prop])
        break
      case "list":
        result[fullVal[1]] = listProxy<T>(context, fullVal[1], textV2, [prop])
        break
      case "text":
        result[fullVal[1]] = handleText(context, fullVal[1] as ObjID)
        break
      case "str":
      case "uint":
      case "int":
      case "f64":
      case "boolean":
      case "bytes":
      case "null":
        result[fullVal[2]] = fullVal[1] as ValueType<T>
        break
      case "counter":
        result[fullVal[2]] = new Counter(fullVal[1]) as ValueType<T>
        break
      case "timestamp":
        result[fullVal[2]] = new Date(fullVal[1]) as ValueType<T>
        break
      default:
        throw RangeError(`datatype ${fullVal[0]} unimplemented`)
    }
  }
  return result
}
