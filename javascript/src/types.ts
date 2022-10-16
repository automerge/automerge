
import { Text } from "./text"
export { Text } from "./text"
export { Counter  } from "./counter"
export { Int, Uint, Float64  } from "./numbers"

import { Counter } from "./counter"

export type AutomergeValue = ScalarValue | { [key: string]: AutomergeValue } | Array<AutomergeValue> | Text
export type MapValue =  { [key: string]: AutomergeValue }
export type ListValue = Array<AutomergeValue> 
export type TextValue = Array<AutomergeValue>
export type ScalarValue = string | number | null | boolean | Date | Counter | Uint8Array
