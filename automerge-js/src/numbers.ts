// Convience classes to allow users to stricly specify the number type they want

export class Int {
  value: number;

  constructor(value: number) {
    if (!(Number.isInteger(value) && value <= Number.MAX_SAFE_INTEGER && value >= Number.MIN_SAFE_INTEGER)) {
      throw new RangeError(`Value ${value} cannot be a uint`)
    }
    this.value = value
    Object.freeze(this)
  }
}

export class Uint {
  value: number;

  constructor(value: number) {
    if (!(Number.isInteger(value) && value <= Number.MAX_SAFE_INTEGER && value >= 0)) {
      throw new RangeError(`Value ${value} cannot be a uint`)
    }
    this.value = value
    Object.freeze(this)
  }
}

export class Float64 {
  value: number;

  constructor(value: number) {
    if (typeof value !== 'number') {
      throw new RangeError(`Value ${value} cannot be a float64`)
    }
    this.value = value || 0.0
    Object.freeze(this)
  }
}

