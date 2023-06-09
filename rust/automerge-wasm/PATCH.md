
## Automerge Patchs

### Path

All patches have a path.  This path describes the set of properties that need to be traversed to reach the intended target.

For instances.  If a document looked like this

```js
{
  animals: [
      {  name: "Lion",
         genus: "Panthera",
      },
      {  name: "Chimpanzee",
         genus: "Panthera",
      }
  ]
}
```

And a patch came to change the genus of Chimpanzee to "Pan" it would look like this.

```js
{
  action: "put",
  path: [ "animals", 1, "genus" ],
  value: "Pan",
}
```

### Values

Patch values can be

```ts
type PatchValue = string | number | boolean | null | Date | Uint8Array | {} | []
```

These stand in for the different scalar types in Automerge plus an empty object or an empty list.

### Put

Changing a single key in a map or index in a list.  This never changes the length of a list.

```ts
type PutPatch = {
  action: 'put'
  path: Prop[],
  value: PatchValue,
  conflict?: boolean
}

let patch : PutPatch = {
  action: "put",
  path: [ "config", "enabled" ],
  value: true,
  conflict: true, // optional
}
```

The conflict field indicates that another conflict value is present which can be queried via an api call.
If the field is missing, it's assumed to be false.

### Insert

Inserts one or more values into a list.  This increases the length of the list by the given amount.

```ts

interface MarkSet  {
  [name : string]: Value;
}

type InsertPatch = {
  action: 'insert'
  path: Prop[],
  values: PatchValue[],
  marks?: MarkSet,
  conflicts?: boolean[]
}

let patch : InsertPatch = {
  action: "insert",
  path: [ "emoji", 3 ],
  values: [ "😍", "🔥", "🎆" ],
  conflicts: [ false, true, false ] // optional
  marks: { size: 24 },
}
```

If the `conflicts` field is missing, it's assumed to be all false. `marks` will be missing if there are none.

### Delete

Delete patches remove a key from a map or one or more elements from a list or text object.

```js

type DelPatch = {
  action: 'del'
  path: Prop[],
  length?: number,
}

let patch : DelPatch = {
  action: "del",
  path: [ "items", 3 ],
  length: 10, // optional
}
```

The length field is present only on sequences and when there is a run of consecutive deletes.

### Inc

Increment a number by 'value`

```ts
type IncPatch = {
  action: 'inc'
  path: Prop[],
  value: number
}

let patch : IncPatch = {
  action: "inc",
  path: [ "config", "logins" ],
  value: 1
}
```

### Splice

Splice a string into a text object

```ts
type SpliceTextPatch = {
  action: 'splice'
  path: Prop[],
  value: string,
  marks?: MarkSet,
}

let patch : SpliceTextPatch = {
  action: "inc",
  path: [ "text", 123 ],
  value: "and so it was done"
  marks: { bold: true },
}
```

The 'marks` field will be missing if there are none.

### Conflict

Signals that a field has become conflicted but its value has not changed.

```ts
type ConflictPatch = {
  action: 'conflict'
  path: Prop[],
}

let patch : ConflictPatch = {
  action: 'conflict',
  path: [ "keys", 11 ]
}
``

### Mark

One or more marks have been added to the document.  The start and end position, mark name and mark value are included.  A value of `null` means the mark has been removed.

```ts
type Mark = {
  name: string,
  value: Value,
  start: number,
  end: number,
}

type MarkPatch = {
  action: 'mark'
  path: Prop[],
  marks: Mark[]
}

let patch : MarkPatch = {
  action: 'mark',
  path: [ "keys", 11 ]
  marks: [
     { name: 'font-weight', value: 'bold', start: 0, end: 5 },
     { name: 'color', value: 'blue', start: 8, end: 11 },
  ]
}
```


