import * as v4 from "https://deno.land/x/uuid@v0.1.2/mod.ts"

// this file is a deno only port of the uuid module

function defaultFactory() {
  return v4.uuid().replace(/-/g, "")
}

let factory = defaultFactory

interface UUIDFactory {
  setFactory(f: typeof factory): void
  reset(): void
  (): string
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
