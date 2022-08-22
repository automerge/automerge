## Automerge JS

This is a reimplementation of Automerge as a JavaScript wrapper around the "automerge-wasm".

This package is in alpha and feedback in welcome.

The primary differences between using this package and "automerge" are as follows:

1. The low level api needs to plugged in via the use function. The only current implementation of "automerge-wasm" but another could used in theory.

```javascript
import * as Automerge from "automerge-js";
import * as wasm_api from "automerge-wasm";

// browsers require an async wasm load - see automerge-wasm docs
Automerge.use(wasm_api);
```

2. There is no front-end back-end split, and no patch format or patch observer. These concepts don't make sense with the wasm implementation.

3. The basic `Doc<T>` object is now a Proxy object and will behave differently in a repl environment.

4. The 'Text' class is currently very slow and needs to be re-worked.

Beyond this please refer to the Automerge [README](http://github.com/automerge/automerge/) for further information.
