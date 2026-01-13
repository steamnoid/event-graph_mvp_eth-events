import pytest


@pytest.mark.unit
def test_merge_neo4j_graphs_unions_events_edges_and_sets_deterministic_run_id():
    from helpers.evm.traces.hash import trace_graph_hash
    from helpers.evm.traces.neo4j import merge_neo4j_graphs

    g1 = {
        "run_id": "run-1",
        "events": [{"event_id": "0xtx:1", "tx_hash": "0xtx", "log_index": 1, "event_name": "CALL"}],
        "edges": [],
    }
    g2 = {
        "run_id": "run-2",
        "events": [{"event_id": "0xtx:0", "tx_hash": "0xtx", "log_index": 0, "event_name": "CALL"}],
        "edges": [{"from": "0xtx:0", "to": "0xtx:1"}],
    }

    merged = merge_neo4j_graphs([g1, g2])

    expected_run_id = trace_graph_hash({"events": g1["events"] + g2["events"], "edges": g1["edges"] + g2["edges"]})

    assert merged["run_id"] == expected_run_id
    assert {e.get("event_id") for e in merged.get("events") or []} == {"0xtx:0", "0xtx:1"}
    assert merged.get("edges") == [{"from": "0xtx:0", "to": "0xtx:1"}]
