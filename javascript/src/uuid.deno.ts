import * as v4 from "https://deno.land/x/uuid@v0.1.2/mod.ts"

function defaultFactory() {
  return v4.uuid().replace(/-/g, "")
}

let factory = defaultFactory

interface UUIDFactory extends Function {
  setFactory(f: typeof factory): void
  reset(): void
}

export const uuid: UUIDFactory = () => {
  return factory()
}

uuid.setFactory = newFactory => {
  factory = newFactory
}

uuid.reset = () => {
  factory = defaultFactory
}
