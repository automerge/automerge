import { v4 } from 'uuid'

function defaultFactory() {
  return v4().replace(/-/g, '')
}

let factory = defaultFactory

interface UUIDFactory extends Function {
  setFactory(f: typeof factory);
  reset();
}

export const uuid : UUIDFactory = () => {
  return factory()
}

uuid.setFactory = newFactory => { factory = newFactory }

uuid.reset = () => { factory = defaultFactory }

