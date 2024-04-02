# Automerge cross-platform Interop Validation Example

Use the exemplar file, or a direct copy of it, to validate cross-platform interoperation and interpretation of values.

The `exemplar` file is an Automerge document with the following contents:

- title: Scalar value string with the contents: "Hello 🇬🇧👨‍👨‍👧‍👦😀"
- notes: Automerge Text object with the contents "🇬🇧👨‍👨‍👧‍👦😀"
- timestamp: Scalar value timestamp with the ISO8601 value: `1941-04-26T08:17:00.123Z`
- location: Scalar value of a URL with the contents "https://automerge.org/"
- counter: Scalar value of an Automerge counter with the value: `5`.
- int: Scalar value integer with the value of `-4`.
- uint: Scalar value of an unsigned integer with the value of `18446744073709551615`
- fp: Scalar value floating point with the value of `3.14159267`
- bytes: Scalar value data with the contents in hex: `0x856f4a83`
- bool: Scalar value boolean with the value of `true`

Using the Automerge-cli tool to examine the file (`automerge export exemplar`):

```
{
  "bool": true,
  "bytes": [
    133,
    111,
    74,
    131
  ],
  "counter": 5,
  "fp": 3.14159267,
  "int": -4,
  "location": "https://automerge.org/",
  "notes": "🇬🇧👨‍👨‍👧‍👦😀",
  "timestamp": -905182979000,
  "title": "Hello 🇬🇧👨‍👨‍👧‍👦😀",
  "uint": 18446744073709551615
}
```
