import { Counter, AutomergeValue } from "./types.js"
import { mapProxy, listProxy } from "./proxies.js"
import type { Automerge, Prop, ObjID, FullValue } from "./wasm_types.js"

export type Conflicts = { [key: string]: AutomergeValue }

/**
 * The conflicting values at a particular property in an object
 *
 * The return value of this function is a map. The values of the map are the
 * conflicting values and the keys are the op IDs which set those values. Most of
 * the time all you care about is the values.
 *
 * One important note is that the return type of this function differs based on
 * whether we are inside a change callback or not. Inside a change callback we
 * return proxies, just like anywhere else in the document. This allows the user to
 * make changes inside a conflicted value without being forced to first resolve the
 * conflict. Outside of a change callback we return frozen POJOs.
 *
 * @param context The underlying automerge-wasm document
 * @param objectId The object ID within which we are looking up conflicts
 * @param prop The property inside the object which we are looking up conflicts for
 * @param withinChangeCallback Whether we are inside a currently running change callback
 *
 * @returns A map from op ID to the value for that op ID
 */
export function conflictAt(
  context: Automerge,
  objectId: ObjID,
  prop: Prop,
  withinChangeCallback: boolean,
): Conflicts | undefined {
  const values = context.getAll(objectId, prop)
  if (values.length <= 1) {
    return
  }
  const result: Conflicts = {}
  for (const fullVal of values) {
    switch (fullVal[0]) {
      case "map":
        if (withinChangeCallback) {
          result[fullVal[1]] = mapProxy(context, fullVal[1], [prop])
        } else {
          result[fullVal[1]] = reifyFullValue(context, [fullVal[0], fullVal[1]])
        }
        break
      case "list":
        if (withinChangeCallback) {
          result[fullVal[1]] = listProxy(context, fullVal[1], [prop])
        } else {
          result[fullVal[1]] = reifyFullValue(context, [fullVal[0], fullVal[1]])
        }
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

function reifyFullValue(
  context: Automerge,
  fullValue: FullValue,
): AutomergeValue {
  switch (fullValue[0]) {
    case "map":
      const mapResult = {}
      for (const key of context.keys(fullValue[1])) {
        let subVal = context.getWithType(fullValue[1], key)
        if (!subVal) {
          throw new Error("unexpected null map value")
        }
        mapResult[key] = reifyFullValue(context, subVal)
      }
      return Object.freeze(mapResult)
    case "list":
      const listResult: AutomergeValue[] = []
      const length = context.length(fullValue[1])
      for (let i = 0; i < length; i++) {
        let subVal = context.getWithType(fullValue[1], i)
        if (!subVal) {
          throw new Error("unexpected null list element")
        }
        listResult.push(reifyFullValue(context, subVal))
      }
      return Object.freeze(listResult) as AutomergeValue
    case "text":
      return context.text(fullValue[1])
    case "str":
    case "uint":
    case "int":
    case "f64":
    case "boolean":
    case "bytes":
    case "null":
      return fullValue[1]
    case "counter":
      return new Counter(fullValue[1]) as AutomergeValue
    case "timestamp":
      return new Date(fullValue[1]) as AutomergeValue
    default:
      throw RangeError(`datatype ${fullValue[0]} unimplemented`)
  }
}
