import { v4 } from "uuid"

function defaultFactory() {
  return v4().replace(/-/g, "")
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
