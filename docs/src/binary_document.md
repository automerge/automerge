# Binary Document Format

The binary format of an automerge document compresses the changes into a compact column-oriented representation.
This makes the format very suitable for storing durably due to its compact size.

## Format

| Field                   | Byte Length     | Description                                          |
| ----------------------- | --------------- | ---------------------------------------------------- |
| Magic Bytes             | 4               | Just some magic bytes                                |
| Checksum                | 4               | First 4 bytes of the SHA256 of the encoded chunk     |
| Block Type              | 1               | A marker byte to distinguish the bytes as a document |
| Chunk length            | Variable (uLEB) | The length of the following chunk bytes              |
| [Chunk](.#chunk-format) | Variable        | The actual bytes for the chunk                       |

## Chunk format

| Field                                       | Byte Length     | Description                                       |
| ------------------------------------------- | --------------- | ------------------------------------------------- |
| Actors length                               | Variable (uLEB) | The number of following actors                    |
| Actors                                      | Variable        | The actor IDs in sorted order                     |
| Heads length                                | Variable (uLEB) | The number of following heads hashes              |
| Heads                                       | 32 \* number    | The head hashes of the hash graph in sorted order |
| [Change Info](.#change-information)         | Variable        | The change columns information                    |
| [Operations Info](.#operations-information) | Variable        | The operations columns information                |
| [Change bytes](.#change-bytes)              | Variable        | The actual bytes for the changes                  |
| [Operations bytes](.#operations-bytes)      | Variable        | The actual bytes for the operations               |

## Change information

Changes are encoded in causal order (a topological sort of the hash graph).

The change information contains the column ids that are present in the encoding.
Empty columns (those with no data) are not included.

For each included column the following is encoded:

| Field       | Byte Length     | Description                               |
| ----------- | --------------- | ----------------------------------------- |
| Column ID   | Variable (uLEB) | The ID of the column this data represents |
| Data length | Variable (uLEB) | The length of the data in this column     |

See [Change columns](.#change-columns) for the columns that may be included here.

## Change bytes

For each change we encode its information in the following columns (note the absence of operations which are encoded separately):

| Column     | Type of Data                                                    |
| ---------- | --------------------------------------------------------------- |
| Actor      | Position of the actor in the sorted actors list                 |
| Seq        | Value of the sequence counter for this change                   |
| Max Op     | The maximum sequence number of the operations in this change    |
| Time       | The timestamp this change was produced at                       |
| Message    | The message this change came with                               |
| Deps num   | The number of dependencies this change has                      |
| Deps index | The indices of the dependencies, as they appear in the document |
| Extra len  | Length of the extra bytes                                       |
| Extra raw  | The raw extra bytes                                             |

## Operations information

Operations are extracted from changes and grouped by the object that they manipulate.
Objects are then sorted by their IDs to make them appear in causal order too.

The operations informatino contains the column ids that are present in the encoding.
Empty columns (those with no data) are not included.

For each included column the following is encoded:

| Field       | Byte Length     | Description                               |
| ----------- | --------------- | ----------------------------------------- |
| Column ID   | Variable (uLEB) | The ID of the column this data represents |
| Data length | Variable (uLEB) | The length of the data in this column     |

See [Operations columns](.#operations-columns) for the columns that may be included here.

## Operations bytes

For each expanded operation we encode its information in the following columns:

| Column            | Type of Data                                                     |
| ----------------- | ---------------------------------------------------------------- |
| OpID Actor        | Position of the actor part of the OpID in the sorted actor list  |
| OpID Counter      | The counter part of this OpID                                    |
| Insert            | Whether this operation is an insert or not                       |
| Action            | Action type that this operation performs                         |
| Object ID actor   | The actor part of the object this operation manipulates          |
| Object ID counter | The counter part of the object this operation manipulates        |
| Key actor         | The actor part of this key (if a sequence index)                 |
| Key counter       | The counter part of this key (if a sequence index)               |
| Key string        | The string part of this key (if a map key)                       |
| Value ref counter | The counter part of the OpID this cursor refers to (cursor only) |
| Value ref actor   | The actor part of the OpID this cursor refers to (cursor only)   |
| Value length      | The length of the encoded raw value in bytes                     |
| Value raw         | The actual value                                                 |
| Successors number | The number of successors in this operation                       |
| Successor actor   | The actor part of the successor                                  |
| Successor counter | The counter part of the successor                                |

## Order of operations

In a change, operations appear in the order in which they were generated by the application.
In a whole document, operations must appear in a specific order, as follows:

* First sort by objectId, such that any operations for the same object are consecutive in the file.
  The null objectId (i.e. the root object) is sorted before all non-null objectIds.
  Non-null objectIds are sorted by Lamport timestamp ordering.
* Next, if the object is a map, sort the operations within that object lexicographically by key,
  so that all operations for the same key are consecutive. This sort order should be based on the
  UTF-8 byte sequence of the key. NOTE: the JavaScript implementation currently does not do this
  sorting correctly, since it sorts by JavaScript string comparison, which differs from UTF-8
  lexicographic ordering for characters beyond the basic multilingual plane.
* If the object is a list or text, sort the operations within that object by the position at which
  they occur in the sequence, so that all operations that relate to the same list element are
  consecutive. Tombstones are treated just like any other list element. To determine the list element
  that an operation relates to, the following rule applies: for insertions (operations where the
  insert column is true), the opId is the list element ID; for updates or deletes (where insert is
  false), the key (keyCtr and keyActor columns, known as elemId in the JSON representation) is the
  list element ID.
* Among the operations for the same key (for maps) or the same list element (for lists/text), sort
  the operations by their opId, using Lamport timestamp ordering. For list elements, note that the
  operation that inserted the operation will always have an opId that is lower than the opId of any
  operations that updates or deletes that list element, and therefore the insertion operation will
  always be the first operation for a given list element.

## Encodings

### uLEB

uLEB is an unsigned [little endian base 128](https://en.wikipedia.org/wiki/LEB128) value.
This is a variable length encoding to keep things compact when values are small.

### RLE

Run length encoding of raw values.

### Delta

Deltas between values are rle encoded.

### Boolean

Encodes the count of the same value with counts alternating false and true.

## Columns

### Change columns

| Name       | Encoding   | ID  |
| ---------- | ---------- | --- |
| Actor      | uLEB RLE   | 1   |
| Seq        | Delta      | 3   |
| Max Op     | Delta      | 19  |
| Time       | Delta      | 35  |
| Message    | String RLE | 53  |
| Deps num   | uLEB RLE   | 64  |
| Deps index | Delta      | 67  |
| Extra len  | uLEB RLE   | 86  |
| Extra raw  | None       | 87  |

### Operations columns

| Name              | Encoding   | ID  |
| ----------------- | ---------- | --- |
| OpID Actor        | uLEB RLE   | 33  |
| OpID Counter      | Delta      | 35  |
| Insert            | Boolean    | 52  |
| Action            | uLEB RLE   | 66  |
| Object ID actor   | uLEB RLE   | 1   |
| Object ID counter | uLEB RLE   | 2   |
| Key actor         | uLEB RLE   | 17  |
| Key counter       | Delta      | 19  |
| Key string        | String RLE | 21  |
| Value ref counter | uLEB RLE   | 98  |
| Value ref actor   | uLEB RLE   | 97  |
| Value length      | uLEB RLE   | 86  |
| Value raw         | None       | 87  |
| Successors number | uLEB RLE   | 128 |
| Successor actor   | uLEB RLE   | 129 |
| Successor counter | Delta      | 131 |
