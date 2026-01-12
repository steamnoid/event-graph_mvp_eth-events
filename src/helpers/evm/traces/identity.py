from __future__ import annotations

import json
from typing import Sequence


def trace_event_id(*, tx_hash: str, trace_address: Sequence[int | str]) -> str:
    normalized = normalize_trace_address(trace_address)
    trace_address_str = json.dumps(normalized, separators=(",", ":"))
    return f"{tx_hash}:{trace_address_str}"


def parent_trace_address(trace_address: Sequence[int | str]) -> list[int] | None:
    normalized = normalize_trace_address(trace_address)
    if len(normalized) == 0:
        return None
    return list(normalized[:-1])


def normalize_trace_address(trace_address: Sequence[int | str]) -> list[int]:
    out: list[int] = []
    for item in trace_address:
        if isinstance(item, int):
            out.append(item)
            continue
        if isinstance(item, str):
            out.append(int(item, 16))
            continue
        raise TypeError(f"Unsupported traceAddress item type: {type(item)}")
    return out
