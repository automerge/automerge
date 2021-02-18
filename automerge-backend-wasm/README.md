## automerge-backend-wasm

This is a wrapper for the rust implementation of [automerge-backend](https://github.com/automerge/automerge-rs/tree/master/automerge-backend) to be used with [Automerge](https://github.com/automerge/automerge).

### Using

You can require this syncronously as a CommonJS module or import it as a ES6 module

```js
let Automerge = require("automerge")
let Backend = require("automerge-backend-wasm")
Automerge.setDefaultBackend(Backend)
```

```js
import * as Automerge from "automerge"
import * as Backend from "automerge-backend-wasm"
Automerge.setDefaultBackend(Backend)
```

Note that the first uses a syncronous filesystem load of the wasm and will not be transferable to a browser bundle.  The second uses ES6 wasm import statements which should work in all modern browsers but require a '--experimental-wasm-modules' flag on nodejs (v13 on) unless you pack/bundle the code into compatible format.

