.PHONY: rust
rust:
	cargo run --release

.PHONY: build-wasm
build-wasm:
	cd ../automerge-wasm && yarn
	cd ../automerge-wasm && yarn release

.PHONY: wasm
wasm: build-wasm
	node automerge-wasm.js

.PHONY: build-js
build-js: build-wasm
	cd ../automerge-js && yarn
	cd ../automerge-js && yarn build

.PHONY: js
js: build-js
	node automerge-js.js

.PHONY: baseline
baseline:
	node baseline.js
