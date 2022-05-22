import { UnknownObject } from './types';

export function isObject(obj: unknown) : obj is UnknownObject {
  return typeof obj === 'object' && obj !== null
}

/**
 * Returns a shallow copy of the object `obj`. Faster than `Object.assign({}, obj)`.
 * https://jsperf.com/cloning-large-objects/1
 */
/*
export function copyObject<T extends UnknownObject>(obj: T) : T {
  if (!isObject(obj)) throw RangeError(`Cannot copy object '${obj}'`) //return {}
  const copy : UnknownObject = {}
  for (const key of Object.keys(obj)) {
    copy[key] = obj[key]
  }
  return copy
}
*/

/**
 * Takes a string in the form that is used to identify operations (a counter concatenated
 * with an actor ID, separated by an `@` sign) and returns an object `{counter, actorId}`.
 */

interface OpIdObj {
  counter: number,
  actorId: string 
}

export function parseOpId(opId: string) : OpIdObj {
  const match = /^(\d+)@(.*)$/.exec(opId || '')
  if (!match) {
    throw new RangeError(`Not a valid opId: ${opId}`)
  }
  return {counter: parseInt(match[1], 10), actorId: match[2]}
}

/**
 * Returns true if the two byte arrays contain the same data, false if not.
 */
export function equalBytes(array1: Uint8Array, array2: Uint8Array) : boolean {
  if (!(array1 instanceof Uint8Array) || !(array2 instanceof Uint8Array)) {
    throw new TypeError('equalBytes can only compare Uint8Arrays')
  }
  if (array1.byteLength !== array2.byteLength) return false
  for (let i = 0; i < array1.byteLength; i++) {
    if (array1[i] !== array2[i]) return false
  }
  return true
}

