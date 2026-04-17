.PHONY: rust
rust:
	cargo run --release

.PHONY: build-wasm
build-wasm:
	cd ../automerge-wasm && npm install
	cd ../automerge-wasm && npm run release

.PHONY: wasm
wasm: build-wasm
	node automerge-wasm.js

.PHONY: build-js
build-js: build-wasm
	cd ../automerge-js && npm install
	cd ../automerge-js && npm run build

.PHONY: js
js: build-js
	node automerge-js.js

.PHONY: baseline
baseline:
	node baseline.js
