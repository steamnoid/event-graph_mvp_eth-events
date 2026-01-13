import json

from helpers.evm.traces.hash import trace_graph_hash
from helpers.evm.traces.transformer import trace_graph_for_trace_block_response
from helpers.evm.traces.transformer import trace_graph_for_trace_transaction_response


def merge_neo4j_graphs(graphs: list[dict]) -> dict:
    all_events: list[dict] = []
    all_edges: list[dict] = []

    for g in graphs or []:
        if not isinstance(g, dict):
            continue
        all_events.extend([e for e in (g.get("events") or []) if isinstance(e, dict)])
        all_edges.extend([e for e in (g.get("edges") or []) if isinstance(e, dict)])

    run_id = trace_graph_hash({"events": all_events, "edges": all_edges})
    return {"run_id": run_id, "events": all_events, "edges": all_edges}


def trace_graph_to_neo4j_graph(trace_graph: dict) -> dict:
    return {
        "run_id": trace_graph_hash(trace_graph),
        "events": trace_graph.get("events") or [],
        "edges": trace_graph.get("edges") or [],
    }


def trace_transaction_response_to_neo4j_graph(response: dict) -> dict:
    trace_graph = trace_graph_for_trace_transaction_response(response=response)
    return trace_graph_to_neo4j_graph(trace_graph)


def trace_transaction_fixture_file_to_neo4j_graph(fixture_file: str) -> dict:
    with open(fixture_file, "r", encoding="utf-8") as f:
        response = json.load(f)
    return trace_transaction_response_to_neo4j_graph(response=response)


def trace_block_response_to_neo4j_graph(response: dict) -> dict:
    trace_graph = trace_graph_for_trace_block_response(response=response)
    return trace_graph_to_neo4j_graph(trace_graph)


def trace_block_fixture_file_to_neo4j_graph(fixture_file: str) -> dict:
    with open(fixture_file, "r", encoding="utf-8") as f:
        response = json.load(f)
    return trace_block_response_to_neo4j_graph(response=response)
