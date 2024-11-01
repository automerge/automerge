import { LINK } from "./constants.js"

export class Link {
  target: string
  constructor(target: string) {
    this.target = target
    Reflect.defineProperty(this, LINK, { value: true })
  }
}
