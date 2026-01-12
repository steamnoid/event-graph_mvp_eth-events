from helpers.evm.traces.hash import trace_graph_hash
from helpers.evm.traces.transformer import trace_graph_for_trace_block_response
from helpers.evm.traces.transformer import trace_graph_for_trace_transaction_response


def trace_graph_to_neo4j_graph(trace_graph: dict) -> dict:
    return {
        "run_id": trace_graph_hash(trace_graph),
        "events": trace_graph.get("events") or [],
        "edges": trace_graph.get("edges") or [],
    }


def trace_transaction_response_to_neo4j_graph(response: dict) -> dict:
    trace_graph = trace_graph_for_trace_transaction_response(response=response)
    return trace_graph_to_neo4j_graph(trace_graph)


def trace_block_response_to_neo4j_graph(response: dict) -> dict:
    trace_graph = trace_graph_for_trace_block_response(response=response)
    return trace_graph_to_neo4j_graph(trace_graph)
