# Automerge CLI

## Anonymize a document

The `anonymize` command replaces document data while retaining its history and structural shape:

```sh
cargo run -p automerge-cli -- anonymize input.automerge --out output.automerge
```

Input and output can instead be piped through stdin and stdout. The result still reveals metadata
such as the change graph, object and value types, collection sizes, string lengths, and whitespace.
Review it before publishing.
