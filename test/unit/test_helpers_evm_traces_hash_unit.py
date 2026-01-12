import pytest


@pytest.mark.unit
def test_trace_graph_hash_is_order_invariant_for_events_and_edges():
    from helpers.evm.traces.hash import trace_graph_hash

    graph1 = {
        "events": [
            {"event_id": "0xaaa:[]", "tx_hash": "0xaaa", "trace_address": [], "type": "call"},
            {"event_id": "0xaaa:[0]", "tx_hash": "0xaaa", "trace_address": [0], "type": "call"},
        ],
        "edges": [
            {"from": "0xaaa:[]", "to": "0xaaa:[0]"},
        ],
    }

    graph2 = {
        "events": list(reversed(graph1["events"])),
        "edges": list(reversed(graph1["edges"])),
    }

    assert trace_graph_hash(graph1) == trace_graph_hash(graph2)
