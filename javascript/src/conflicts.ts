import { Counter, AutomergeValue } from "./types.js"
import { mapProxy, listProxy } from "./proxies.js"
import type { Automerge, Prop, ObjID } from "./wasm_types.js"

export type Conflicts = { [key: string]: AutomergeValue }

export function conflictAt(
  context: Automerge,
  objectId: ObjID,
  prop: Prop,
): Conflicts | undefined {
  const values = context.getAll(objectId, prop)
  if (values.length <= 1) {
    return
  }
  const result: Conflicts = {}
  for (const fullVal of values) {
    switch (fullVal[0]) {
      case "map":
        result[fullVal[1]] = mapProxy(context, fullVal[1], [prop])
        break
      case "list":
        result[fullVal[1]] = listProxy(context, fullVal[1], [prop])
        break
      case "text":
        result[fullVal[1]] = context.text(fullVal[1] as ObjID)
        break
      case "str":
      case "uint":
      case "int":
      case "f64":
      case "boolean":
      case "bytes":
      case "null":
        result[fullVal[2]] = fullVal[1] as AutomergeValue
        break
      case "counter":
        result[fullVal[2]] = new Counter(fullVal[1]) as AutomergeValue
        break
      case "timestamp":
        result[fullVal[2]] = new Date(fullVal[1]) as AutomergeValue
        break
      default:
        throw RangeError(`datatype ${fullVal[0]} unimplemented`)
    }
  }
  return result
}
