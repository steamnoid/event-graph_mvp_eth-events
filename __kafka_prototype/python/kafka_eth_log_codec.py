from __future__ import annotations

import json


def encode_raw_log(raw_log: dict) -> bytes:
    return json.dumps(raw_log, sort_keys=True, separators=(",", ":")).encode("utf-8")


def decode_raw_log(payload: bytes) -> dict:
    if payload is None:
        raise ValueError("payload is None")
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return json.loads(payload.decode("utf-8"))
