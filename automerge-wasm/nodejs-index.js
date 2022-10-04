let wasm = require("./bindgen")
module.exports = wasm
module.exports.load = module.exports.loadDoc
delete module.exports.loadDoc
module.exports.init = () => (new Promise((resolve,reject) => { resolve(module.exports) }))
