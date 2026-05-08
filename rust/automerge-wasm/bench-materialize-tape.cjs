const am = require("./bench-node/automerge_wasm.js")

const STATE = Symbol.for("_am_meta")
const OBJECT_ID = Symbol.for("_am_objectId")

function makeDoc() {
  const doc = am.create({})
  for (let i = 0; i < 400; i++) {
    doc.put("/", `k${i}`, i)
  }
  const items = doc.putObject("/", "items", [])
  for (let i = 0; i < 250; i++) {
    const item = doc.insertObject(items, i, {})
    doc.put(item, "id", i)
    doc.put(item, "name", `item-${i}`)
    doc.put(item, "active", i % 2 === 0)
    const scores = doc.putObject(item, "scores", [])
    for (let j = 0; j < 8; j++) {
      doc.insert(scores, j, i * 10 + j)
    }
  }
  const text = doc.putObject("/", "body", "")
  doc.splice(text, 0, 0, "hello ".repeat(2000))
  doc.commit(null, null)
  return doc
}

function materializeFromWasmReads(handle, obj = "/", heads, meta) {
  const info = handle.objInfo(obj, heads)
  let result
  if (info.type === "text") {
    result = handle.text(info.id, heads)
  } else if (info.type === "list") {
    result = []
    const length = handle.length(info.id, heads)
    for (let index = 0; index < length; index++) {
      result.push(materializeValueFromReads(handle, info.id, index, heads, meta))
    }
  } else {
    result = {}
    for (const key of handle.keys(info.id, heads)) {
      result[key] = materializeValueFromReads(handle, info.id, key, heads, meta)
    }
  }
  setMeta(result, info.id, meta)
  return result
}

function materializeValueFromReads(handle, obj, prop, heads, meta) {
  const value = handle.getWithType(obj, prop, heads)
  if (value == null) return undefined
  const [datatype, raw] = value
  if (datatype === "map" || datatype === "table" || datatype === "list" || datatype === "text") {
    return materializeFromWasmReads(handle, raw, heads, meta)
  }
  return raw
}

function materializeFromCompactTape(handle, obj = "/", heads, meta) {
  const tape = handle.materializeCompactTape(obj, heads)
  const objects = []
  const objectMeta = []
  const ops = tape.ops
  const strings = tape.strings
  const values = tape.values
  for (let offset = 0; offset < ops.length; offset += 8) {
    const op = ops[offset]
    if (op === 0) {
      const index = ops[offset + 1]
      const type = ops[offset + 2]
      objects[index] = makeCompactMaterializedObject(type)
      objectMeta[index] = { type }
      setMeta(objects[index], strings[ops[offset + 3]], meta)
    } else if (op === 1) {
      const parent = ops[offset + 1]
      const propKind = ops[offset + 2]
      const prop = ops[offset + 3]
      const type = ops[offset + 4]
      const index = ops[offset + 5]
      objects[index] = makeCompactMaterializedObject(type)
      objectMeta[index] = { type, parent, propKind, prop }
      setMeta(objects[index], strings[ops[offset + 6]], meta)
      if (type !== 2) {
        setCompactMaterializedValue(objects[parent], propKind, prop, objects[index], strings)
      }
    } else if (op === 2) {
      setCompactMaterializedValue(
        objects[ops[offset + 1]],
        ops[offset + 2],
        ops[offset + 3],
        values[ops[offset + 5]],
        strings,
      )
    } else if (op === 3) {
      objects[ops[offset + 1]].push(strings[ops[offset + 2]])
    }
  }
  for (let index = 0; index < objects.length; index++) {
    const meta = objectMeta[index]
    if (meta?.type === 2) {
      const value = objects[index].join("")
      objects[index] = value
      if (meta.parent != null && meta.propKind != null && meta.prop != null) {
        setCompactMaterializedValue(objects[meta.parent], meta.propKind, meta.prop, value, strings)
      }
    }
  }
  return objects[0]
}

function makeCompactMaterializedObject(type) {
  if (type === 1 || type === 2) return []
  return {}
}

function setCompactMaterializedValue(target, propKind, prop, value, strings) {
  target[propKind === 0 ? strings[prop] : prop] = value
}

function setMeta(target, objectId, meta) {
  if (target && typeof target === "object") {
    Object.defineProperties(target, {
      [OBJECT_ID]: { value: objectId },
      [STATE]: { value: meta },
    })
  }
}

function bench(name, fn, iterations) {
  for (let i = 0; i < 5; i++) fn()
  const start = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) fn()
  const elapsed = Number(process.hrtime.bigint() - start) / 1e6
  console.log(JSON.stringify({ name, iterations, total_ms: elapsed, per_iter_ms: elapsed / iterations }))
}

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical)
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map(key => [key, canonical(value[key])]),
    )
  }
  return value
}

const doc = makeDoc()
const meta = { handle: doc, heads: undefined }
const rust = doc.materialize("/", undefined, meta)
const reads = materializeFromWasmReads(doc, "/", undefined, meta)
const compactTape = materializeFromCompactTape(doc, "/", undefined, meta)

if (JSON.stringify(canonical(rust)) !== JSON.stringify(canonical(reads))) {
  throw new Error("per-read materialization did not match Rust materialization")
}
if (JSON.stringify(canonical(rust)) !== JSON.stringify(canonical(compactTape))) {
  throw new Error("compact tape materialization did not match Rust materialization")
}

bench("rust_materialize", () => doc.materialize("/", undefined, meta), 100)
bench("js_per_read_materialize", () => materializeFromWasmReads(doc, "/", undefined, meta), 100)
bench("compact_tape_materialize", () => materializeFromCompactTape(doc, "/", undefined, meta), 100)
