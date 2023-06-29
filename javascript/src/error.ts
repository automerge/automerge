export class AutomergeError extends Error {
  constructor(message: unknown) {
    super(`${message instanceof Error ? message.message : message}`)
    this.name = "AutomergeError"
  }
}
