#!/usr/bin/env python3
"""Regenerate the malformed counter and value fixtures using only the stdlib.

Run from any directory: python3 scripts/generate-malformed-fixtures.py
The fixtures reduce fuzz failures to the columns needed to trigger them.
"""

from hashlib import sha256
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "rust/automerge"


def uleb(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    return bytes(result + bytes([value]))


def sleb(value):
    result = bytearray()
    while True:
        byte = value & 127
        value >>= 7
        if (value == 0 and byte < 64) or (value == -1 and byte >= 64):
            return bytes(result + bytes([byte]))
        result.append(byte | 128)


def string(value):
    return uleb(len(value)) + value


def columns(entries):
    metadata = uleb(len(entries))
    for spec, data in entries:
        metadata += uleb(spec) + uleb(len(data))
    return metadata + b"".join(data for _, data in entries)


def chunk(kind, body):
    hashed = bytes([kind]) + uleb(len(body)) + body
    return bytes.fromhex("85 6f 4a 83") + sha256(hashed).digest()[:4] + hashed


def bundle(counter_spec, counters):
    # No dependencies or changes; one actor and one operation ID.
    prefix = uleb(0) + uleb(1) + string(b"synthetic")
    return chunk(3, prefix + columns([]) + columns([
        (0x21, sleb(-1) + uleb(0)),  # One literal actor index.
        (counter_spec, counters),
    ]))


def malformed_change():
    prefix = (uleb(0) + string(b"synthetic") + uleb(1) + uleb(1)
              + sleb(0) + string(b"") + uleb(0))
    return chunk(1, prefix + columns([
        (0x15, sleb(-1) + string(b"value")),  # Map key.
        (0x34, uleb(1)),                     # One false insert flag.
        (0x42, sleb(-1) + uleb(1)),          # Put action.
        (0x56, sleb(-1) + uleb(297 << 4)),   # Null claiming 297 bytes.
        (0x57, bytes(369)),                  # Unread raw value bytes.
        (0x70, sleb(-1) + uleb(0)),          # No predecessors.
    ]))


if __name__ == "__main__":
    fixtures = ROOT / "src/storage/bundle/fixtures"
    # A literal delta with an unterminated signed LEB128 value.
    (fixtures / "truncated-counter-bundle.bin").write_bytes(
        bundle(0x23, sleb(-1) + b"\x80"))
    # Both counter formats must reject this run before allocating its rows.
    amplified = sleb(114_440_652) + sleb(-19)
    (fixtures / "timeout-counter-bundle.bin").write_bytes(bundle(0x23, amplified))
    (fixtures / "slow-counter-bundle.bin").write_bytes(bundle(0xB3, amplified))
    (ROOT / "tests/fixtures/change_null_value_with_payload.automerge").write_bytes(
        malformed_change())
