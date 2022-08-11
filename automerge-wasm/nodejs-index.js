let wasm = require("./bindgen")
module.exports = wasm
module.exports.load = module.exports.loadDoc
delete module.exports.loadDoc
Object.defineProperty(module.exports, "__esModule", { value: true })
module.exports.init = () => (new Promise((resolve,reject) => { resolve(module.exports) }))
module.exports.default = module.exports.init
