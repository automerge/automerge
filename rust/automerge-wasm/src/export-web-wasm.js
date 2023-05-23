// This file is inserted into ./web by the build script

import WASM from "./automerge_wasm_bg.wasm";
import { initSync } from "./automerge_wasm.js";
initSync(WASM);
export * from "./automerge_wasm.js";
