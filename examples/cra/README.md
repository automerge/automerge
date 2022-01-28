## Example CRA App using AutomergeWASM

### Creating this example app

```bash
   $ cd automerge-wasm && yarn pkg # this builds the npm package
   $ cd ../examples
   $ npx create-react-app cra --template typescript
   $ cd cra
   $ npm install ../../automerge-wasm/automerge-wasm-v0.1.0.tgz
```

Then I just needed to add the import "automerge-wasm" and `{ useEffect, useState }` code to `./src/App.tsx`

```bash
    $ npm start
```

### Open Issues 

The example app currently doesn't do anything useful.  Perhaps someone with some react experience and figure out the right way to wire everything up for an actual demo.

