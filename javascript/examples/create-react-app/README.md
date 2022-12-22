# Automerge + `create-react-app`

This is a little fiddly to get working. The problem is that `create-react-app`
hard codes a webpack configuration which does not support WASM modules, which we
require in order to bundle the WASM implementation of automerge. To get around
this we use [`craco`](https://github.com/dilanx/craco) which does some monkey
patching to allow us to modify the webpack config that `create-react-app`
bundles. Then we use a craco plugin called
[`craco-wasm`](https://www.npmjs.com/package/craco-wasm) to perform the
necessary modifications to the webpack config. It should be noted that this is
all quite fragile and ideally you probably don't want to use `create-react-app`
to do this in production.

## Setup

Assuming you have already run `create-react-app` and your working directory is
the project.

### Install craco and craco-wasm

```bash
yarn add craco craco-wasm
```

### Modify `package.json` to use `craco` for scripts

In `package.json` the `scripts` section will look like this:

```json
  "scripts": {
    "start": "craco start",
    "build": "craco build",
    "test": "craco test",
    "eject": "craco eject"
  },
```

Replace that section with:

```json
  "scripts": {
    "start": "craco start",
    "build": "craco build",
    "test": "craco test",
    "eject": "craco eject"
  },
```

### Create `craco.config.js`

In the root of the project add the following contents to `craco.config.js`

```javascript
const cracoWasm = require("craco-wasm")

module.exports = {
  plugins: [cracoWasm()],
}
```
