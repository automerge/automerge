import { v4 } from 'uuid'

function defaultFactory() {
  return v4().replace(/-/g, '')
}

let factory = defaultFactory

export function uuid() {
  return factory()
}

// @ts-ignore
uuid.setFactory = newFactory => { factory = newFactory }
// @ts-ignore
uuid.reset = () => { factory = defaultFactory }
