from helpers.evm.traces.rpc import (
    build_trace_block_request,
    build_trace_block_requests_for_range,
    build_trace_transaction_request,
)
from helpers.evm.traces.transformer import (
    trace_graph_for_trace_block_response,
    trace_graph_for_trace_transaction_response,
)
import helpers.evm.traces.http as http
from helpers.evm.traces.hash import trace_graph_hash


def fetch_trace_block_graph(*, url: str, block_number: int, request_id: int = 1, timeout_s: int = 30) -> dict:
    payload = build_trace_block_request(block_number=block_number, request_id=request_id)
    response = http.post_json_rpc(url=url, payload=payload, timeout_s=timeout_s)
    graph = trace_graph_for_trace_block_response(response=response)
    graph["graph_hash"] = trace_graph_hash(graph)
    return graph


def fetch_trace_transaction_graph(*, url: str, tx_hash: str, request_id: int = 1, timeout_s: int = 30) -> dict:
    payload = build_trace_transaction_request(tx_hash=tx_hash, request_id=request_id)
    response = http.post_json_rpc(url=url, payload=payload, timeout_s=timeout_s)
    graph = trace_graph_for_trace_transaction_response(response=response)
    graph["graph_hash"] = trace_graph_hash(graph)
    return graph


def fetch_trace_block_range_graph(
    *,
    url: str,
    from_block: int,
    to_block: int,
    start_request_id: int = 1,
    timeout_s: int = 30,
) -> dict:
    payloads = build_trace_block_requests_for_range(
        from_block=from_block,
        to_block=to_block,
        start_request_id=start_request_id,
    )
    response = http.post_json_rpc(url=url, payload=payloads, timeout_s=timeout_s)
    if not isinstance(response, list):
        return {"events": [], "edges": []}

    responses_sorted = sorted(response, key=lambda r: r.get("id", -1) if isinstance(r, dict) else -1)

    all_events: list[dict] = []
    all_edges: list[dict] = []
    for item in responses_sorted:
        if not isinstance(item, dict):
            continue
        graph = trace_graph_for_trace_block_response(response=item)
        all_events.extend(graph.get("events") or [])
        all_edges.extend(graph.get("edges") or [])

    merged_graph = {"events": all_events, "edges": all_edges}
    merged_graph["graph_hash"] = trace_graph_hash(merged_graph)
    return merged_graph
