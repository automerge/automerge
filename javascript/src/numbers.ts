// Convenience classes to allow users to strictly specify the number type they want

import { INT, UINT, F64 } from "./constants.js"

export class Int {
  value: number

  constructor(value: number) {
    if (
      !(
        Number.isInteger(value) &&
        value <= Number.MAX_SAFE_INTEGER &&
        value >= Number.MIN_SAFE_INTEGER
      )
    ) {
      throw new RangeError(`Value ${value} cannot be a uint`)
    }
    this.value = value
    Reflect.defineProperty(this, INT, { value: true })
    Object.freeze(this)
  }
}

export class Uint {
  value: number

  constructor(value: number) {
    if (
      !(
        Number.isInteger(value) &&
        value <= Number.MAX_SAFE_INTEGER &&
        value >= 0
      )
    ) {
      throw new RangeError(`Value ${value} cannot be a uint`)
    }
    this.value = value
    Reflect.defineProperty(this, UINT, { value: true })
    Object.freeze(this)
  }
}

export class Float64 {
  value: number

  constructor(value: number) {
    if (typeof value !== "number") {
      throw new RangeError(`Value ${value} cannot be a float64`)
    }
    this.value = value || 0.0
    Reflect.defineProperty(this, F64, { value: true })
    Object.freeze(this)
  }
}
