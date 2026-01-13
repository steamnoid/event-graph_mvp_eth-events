from __future__ import annotations

from typing import Sequence

from helpers.evm.traces.identity import normalize_trace_address, parent_trace_address, trace_event_id


def trace_event_order(*, trace_address: Sequence[int | str]) -> str:
    normalized = normalize_trace_address(trace_address)
    if not normalized:
        return "1"
    return "1." + ".".join(str(int(i) + 1) for i in normalized)


def trace_event_name(trace: dict) -> str | None:
    if trace.get("error"):
        return "REVERT"

    trace_type = trace.get("type")
    if str(trace_type).lower() == "call":
        action = trace.get("action") if isinstance(trace.get("action"), dict) else {}
        call_type = action.get("callType") or action.get("call_type")
        if call_type:
            return str(call_type).upper()
        return "CALL"

    if trace_type is None:
        return None
    return str(trace_type).upper()


def trace_call_metadata(trace: dict) -> dict:
    action = trace.get("action") if isinstance(trace.get("action"), dict) else {}
    from_address = action.get("from")
    to_address = action.get("to")
    call_type = action.get("callType") or action.get("call_type")
    value = action.get("value")
    input_data = action.get("input")
    input_sig = None
    if isinstance(input_data, str) and input_data.startswith("0x") and len(input_data) >= 10:
        input_sig = input_data[:10]

    return {
        "from_address": from_address,
        "to_address": to_address,
        "call_type": call_type,
        "value": value,
        "input": input_data,
        "input_sig": input_sig,
    }


def traces_from_trace_block_response(response: dict) -> list[dict]:
    traces = response.get("trace")
    if isinstance(traces, list):
        return traces

    result = response.get("result")
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        traces = result.get("trace")
        if isinstance(traces, list):
            return traces

    return []


def trace_graph_for_trace_block_response(*, response: dict) -> dict:
    traces = traces_from_trace_block_response(response)
    return trace_graph_for_block(traces=traces)


def trace_graph_for_trace_transaction_response(*, response: dict) -> dict:
    traces = traces_from_trace_block_response(response)
    if not traces:
        return {"events": [], "edges": []}

    first = traces[0]
    tx_hash = first.get("transactionHash") or first.get("transaction_hash")
    if not tx_hash:
        return {"events": [], "edges": []}

    tx_hashes = {
        (t.get("transactionHash") or t.get("transaction_hash"))
        for t in traces
        if (t.get("transactionHash") or t.get("transaction_hash"))
    }
    if len(tx_hashes) > 1:
        raise ValueError("mixed transaction hashes in trace_transaction response")

    def _trace_sort_key(t: dict) -> tuple[int, ...]:
        addr = t.get("traceAddress")
        if addr is None:
            addr = t.get("trace_address")
        if addr is None:
            return ()
        return tuple(normalize_trace_address(addr))

    traces_sorted = sorted(traces, key=_trace_sort_key)
    return trace_graph_for_transaction(tx_hash=tx_hash, traces=traces_sorted)


def trace_edge_for_child(*, tx_hash: str, child_trace_address: Sequence[int | str]) -> dict | None:
    parent_addr = parent_trace_address(child_trace_address)
    if parent_addr is None:
        return None

    return {
        "from": trace_event_id(tx_hash=tx_hash, trace_address=parent_addr),
        "to": trace_event_id(tx_hash=tx_hash, trace_address=child_trace_address),
    }


def trace_edges_for_transaction(*, tx_hash: str, traces: Sequence[dict]) -> list[dict]:
    edges: list[dict] = []
    for trace in traces:
        trace_address = trace.get("traceAddress")
        if trace_address is None:
            trace_address = trace.get("trace_address")
        if trace_address is None:
            continue

        edge = trace_edge_for_child(tx_hash=tx_hash, child_trace_address=trace_address)
        if edge is not None:
            edges.append(edge)
    return edges


def trace_events_for_transaction(*, tx_hash: str, traces: Sequence[dict]) -> list[dict]:
    events: list[dict] = []
    for trace in traces:
        trace_address = trace.get("traceAddress")
        if trace_address is None:
            trace_address = trace.get("trace_address")
        if trace_address is None:
            continue

        normalized_addr = normalize_trace_address(trace_address)
        trace_type = trace.get("type")
        event_name = trace_event_name(trace)
        event_order = trace_event_order(trace_address=normalized_addr)

        metadata = trace_call_metadata(trace)

        events.append(
            {
                "event_id": trace_event_id(tx_hash=tx_hash, trace_address=normalized_addr),
                "tx_hash": tx_hash,
                "trace_address": normalized_addr,
                "type": trace_type,
                "event_name": event_name,
                "event_order": event_order,
                **metadata,
            }
        )
    return events


def trace_graph_for_transaction(*, tx_hash: str, traces: Sequence[dict]) -> dict:
    return {
        "events": trace_events_for_transaction(tx_hash=tx_hash, traces=traces),
        "edges": trace_edges_for_transaction(tx_hash=tx_hash, traces=traces),
    }


def trace_graph_for_block(*, traces: Sequence[dict]) -> dict:
    by_tx: dict[str, list[dict]] = {}

    for trace in traces:
        value = trace.get("value") if isinstance(trace, dict) else None
        trace_item = value if isinstance(value, dict) else trace

        tx_hash = trace_item.get("transactionHash") or trace_item.get("transaction_hash")
        if not tx_hash:
            continue
        by_tx.setdefault(tx_hash, []).append(trace_item)

    all_events: list[dict] = []
    all_edges: list[dict] = []

    for tx_hash in sorted(by_tx.keys()):
        tx_traces = by_tx[tx_hash]

        def _trace_sort_key(t: dict) -> tuple[int, ...]:
            addr = t.get("traceAddress")
            if addr is None:
                addr = t.get("trace_address")
            if addr is None:
                return ()
            return tuple(normalize_trace_address(addr))

        tx_traces_sorted = sorted(tx_traces, key=_trace_sort_key)
        graph = trace_graph_for_transaction(tx_hash=tx_hash, traces=tx_traces_sorted)
        all_events.extend(graph.get("events") or [])
        all_edges.extend(graph.get("edges") or [])

    return {"events": all_events, "edges": all_edges}
