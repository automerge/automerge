import { default as assert } from "assert"
import { Encoder } from "./legacy/encoding.js"

const mismatched_heads_base64 =
  "hW9Kg5UNrBwA5wcGECHMwVs7qkheqCNbdnoIHqYQJ29iI2m8TlG5JFV/oCCjPBAn/BHQorRPAYz5Cmxmp4LXEGk3mwjwqU8QscL8Bi6OvosQfNoTH+LZTKaDF8vS2ruNthCWyiL7jGVD35LQ5FQxiXibAkq9pVwJR+aY+PV3dEbyCpfAQkLuNBL4MqIUhtQHQpAg5klZYF4yr0f8XnVkaFxHE72W0gKgSCi4eaXBwxxJo7wHARkDLxMMIwNAGUMoVgMKFQghQSMzNAJCA1YGX/QCgAEVgQE5gwEvfwXLAAEPBAUDLQIZAA8CBQAKAhkALQIPAH4BAMoAAX+2fw4Bf3IEAX98LAF/VBgBfxUOAX9eBAF/HwkBf1kYAX8QLAF/RQ4BugEBf3zPAAF/VA4BmQIAfwClAQF/Ag4BfwIJAX8CBAF/AhgBfwI7AX8ApQEBfmcaDgF+cRADAX58BgQBfnsGAwF+dwoZAX5nGisBflQuDQGZAgeZAgVjb3VudH8FywABDwQFAy0CGQAPAncAAgACAAIAAgAGAhkAfQIAAgIAAgJrAAIAAgACAAIAAgACAAIAAgACAAIABAJ/ABsCtgEBdwABAAEAAQABACABfAB/AgACAWsAAQABAAEAAQABAAEAAQABAAEAAQADAX5yDxoBmQKZAgHAABTZASQNy9N2ZQEURcE1T9u2bdu/FrVt24pt27yxbRt71HMJZ9bsOXPnzV+wcNHiJUuXLV+xctXqNWvXrd+wcdPmLVu3bd+xc9fuPXv37T9w8NDhI0ePHT9x8tTpM2fPnb9w8dLlK1evxSpO8UpQopKUrBSlKk3pylCmspStHOUqT/kqUKGKVKwSuVSqMpWrQpWqUrVqVKs61atBjWpSs1rUqja1q0Od6lK3etSrPvVrQIMa0rBGNKoxjWtCk5rStNxwxwNPvLjODW5yi9vc4S73uM8DHvKIxzzhKc94zgte8orXvOEt73jPBz7yic984Svf+M4PfvKL3/zhL//4jzc++OKHPwEEEmSCTYgJNWEmnAgiiSKaGGKJI54EEkkimRRSSSOdDDLJIpsccskjnwIKKaKYElyUGhdlppwKU2mqTLWpMbWmztSbBtNomkwzLbTaaaOdDjrpopseeumjnwEGGWKYEUYZY5wJJpliGjfH3fFwPB0vZwa0AQF/AisBfwAXAX8ABAF/ABoBfwDLAAEPBAUDLQIZAA8CdwACAAIAAgACAAcCGQACAn0AAgACAm0AAgACAAIAAgACAAIAAgACAAIAHwJ/ArQBAXUAAQABAAEAAQABACABfQABAAIBbQABAAEAAQABAAEAAQABAAEAAQAeAZgCiQI="
export const mismatched_heads = Buffer.from(mismatched_heads_base64, "base64")

// Assertion that succeeds if the first argument deepStrictEquals at least one of the
// subsequent arguments (but we don't care which one)
export function assertEqualsOneOf(actual, ...expected) {
  assert(expected.length > 0)
  for (let i = 0; i < expected.length; i++) {
    try {
      assert.deepStrictEqual(actual, expected[i])
      return // if we get here without an exception, that means success
    } catch (e) {
      if (e instanceof assert.AssertionError) {
        if (!e.name.match(/^AssertionError/) || i === expected.length - 1)
          throw e
      } else {
        throw e
      }
    }
  }
}

/**
 * Asserts that the byte array maintained by `encoder` contains the same byte
 * sequence as the array `bytes`.
 */
export function checkEncoded(encoder, bytes, detail?) {
  const encoded = encoder instanceof Encoder ? encoder.buffer : encoder
  const expected = new Uint8Array(bytes)
  const message =
    (detail ? `${detail}: ` : "") + `${encoded} expected to equal ${expected}`
  assert(encoded.byteLength === expected.byteLength, message)
  for (let i = 0; i < encoded.byteLength; i++) {
    assert(encoded[i] === expected[i], message)
  }
}
