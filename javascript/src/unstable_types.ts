import { Counter } from "./types"

export {
  Counter,
  type Doc,
  Int,
  Uint,
  Float64,
  type Patch,
  type PatchCallback,
  type Mark,
  type MarkRange,
  type MarkValue,
  type Cursor,
  type PatchInfo,
  type PatchSource,
} from "./types"

import { RawString } from "./raw_string"
export { RawString } from "./raw_string"

export type AutomergeValue =
  | ScalarValue
  | { [key: string]: AutomergeValue }
  | Array<AutomergeValue>
export type MapValue = { [key: string]: AutomergeValue }
export type ListValue = Array<AutomergeValue>
export type ScalarValue =
  | string
  | number
  | null
  | boolean
  | Date
  | Counter
  | Uint8Array
  | RawString
