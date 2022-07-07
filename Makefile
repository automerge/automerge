.PHONY: rust
rust:
	cd automerge && cargo test

.PHONY: wasm
wasm:
	cd automerge-wasm && yarn
	cd automerge-wasm && yarn build
	cd automerge-wasm && yarn test
	cd automerge-wasm && yarn link

.PHONY: js
js: wasm
	cd automerge-js && yarn
	cd automerge-js && yarn link "automerge-wasm"
	cd automerge-js && yarn test

.PHONY: clean
clean:
	git clean -x -d -f
