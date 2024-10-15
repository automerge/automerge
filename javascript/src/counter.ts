import type { Automerge, ObjID, Prop } from "./wasm_types.js"
import { COUNTER } from "./constants.js"
/**
 * The most basic CRDT: an integer value that can be changed only by
 * incrementing and decrementing. Since addition of integers is commutative,
 * the value trivially converges.
 */
export class Counter {
  value: number

  constructor(value?: number) {
    this.value = value || 0
    Reflect.defineProperty(this, COUNTER, { value: true })
  }

  /**
   * A peculiar JavaScript language feature from its early days: if the object
   * `x` has a `valueOf()` method that returns a number, you can use numerical
   * operators on the object `x` directly, such as `x + 1` or `x < 4`.
   * This method is also called when coercing a value to a string by
   * concatenating it with another string, as in `x + ''`.
   * https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/valueOf
   */
  valueOf(): number {
    return this.value
  }

  /**
   * Returns the counter value as a decimal string. If `x` is a counter object,
   * this method is called e.g. when you do `['value: ', x].join('')` or when
   * you use string interpolation: `value: ${x}`.
   */
  toString(): string {
    return this.valueOf().toString()
  }

  /**
   * Returns the counter value, so that a JSON serialization of an Automerge
   * document represents the counter simply as an integer.
   */
  toJSON(): number {
    return this.value
  }

  /**
   * Increases the value of the counter by `delta`. If `delta` is not given,
   * increases the value of the counter by 1.
   *
   * Will throw an error if used outside of a change callback.
   */
  increment(_delta: number): number {
    throw new Error(
      "Counters should not be incremented outside of a change callback",
    )
  }

  /**
   * Decreases the value of the counter by `delta`. If `delta` is not given,
   * decreases the value of the counter by 1.
   *
   * Will throw an error if used outside of a change callback.
   */
  decrement(_delta: number): number {
    throw new Error(
      "Counters should not be decremented outside of a change callback",
    )
  }
}

/**
 * An instance of this class is used when a counter is accessed within a change
 * callback.
 */
class WriteableCounter extends Counter {
  context: Automerge
  path: Prop[]
  objectId: ObjID
  key: Prop

  constructor(
    value: number,
    context: Automerge,
    path: Prop[],
    objectId: ObjID,
    key: Prop,
  ) {
    super(value)
    this.context = context
    this.path = path
    this.objectId = objectId
    this.key = key
  }

  /**
   * Increases the value of the counter by `delta`. If `delta` is not given,
   * increases the value of the counter by 1.
   */
  increment(delta: number): number {
    delta = typeof delta === "number" ? delta : 1
    this.context.increment(this.objectId, this.key, delta)
    this.value += delta
    return this.value
  }

  /**
   * Decreases the value of the counter by `delta`. If `delta` is not given,
   * decreases the value of the counter by 1.
   */
  decrement(delta: number): number {
    return this.increment(typeof delta === "number" ? -delta : -1)
  }
}

/**
 * Returns an instance of `WriteableCounter` for use in a change callback.
 * `context` is the proxy context that keeps track of the mutations.
 * `objectId` is the ID of the object containing the counter, and `key` is
 * the property name (key in map, or index in list) where the counter is
 * located.
 */
export function getWriteableCounter(
  value: number,
  context: Automerge,
  path: Prop[],
  objectId: ObjID,
  key: Prop,
): WriteableCounter {
  return new WriteableCounter(value, context, path, objectId, key)
}

//module.exports = { Counter, getWriteableCounter }
