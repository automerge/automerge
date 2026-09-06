#!/usr/bin/env bash
set -euo pipefail

data_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
traces=("$data_dir"/*.json)

cargo run --release \
  --manifest-path "$data_dir/../Cargo.toml" \
  -- generate-egwalker-data "${traces[@]}"
