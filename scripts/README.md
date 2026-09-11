# Malformed test fixtures

Run `python3 scripts/generate-malformed-fixtures.py` from the repository root.
The script uses only the Python standard library and reproduces these files byte for byte:

- `rust/automerge/src/storage/bundle/fixtures/truncated-counter-bundle.bin`
- `rust/automerge/src/storage/bundle/fixtures/timeout-counter-bundle.bin`
- `rust/automerge/src/storage/bundle/fixtures/slow-counter-bundle.bin`
- `rust/automerge/tests/fixtures/change_null_value_with_payload.automerge`

The generator describes each encoded column alongside its construction.
