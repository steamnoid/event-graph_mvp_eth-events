import json
from pathlib import Path

import pytest


@pytest.mark.behavior
def test_trace_transaction_fixture_can_be_parsed_into_graph():
    from helpers.evm.traces.transformer import trace_graph_for_trace_transaction_response

    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "evm_traces"
        / "trace_transaction_sample.json"
    )
    raw = json.loads(fixture_path.read_text())

    traces = raw.get("result")
    assert isinstance(traces, list)
    assert len(traces) > 0

    tx_hash = traces[0].get("transactionHash")
    assert isinstance(tx_hash, str) and tx_hash

    graph = trace_graph_for_trace_transaction_response(response=raw)

    assert isinstance(graph.get("events"), list)
    assert len(graph["events"]) == len(traces)
    assert all(e.get("tx_hash") == tx_hash for e in graph["events"])
    assert all(isinstance(e.get("event_id"), str) and e["event_id"].startswith(f"{tx_hash}:") for e in graph["events"])


@pytest.mark.behavior
def test_trace_block_fixture_can_be_parsed_into_graph():
    from helpers.evm.traces.transformer import trace_graph_for_trace_block_response, traces_from_trace_block_response

    fixture_path = Path(__file__).resolve().parents[1] / "fixtures" / "evm_traces" / "trace_block_sample.json"
    raw = json.loads(fixture_path.read_text())

    traces = traces_from_trace_block_response(raw)
    assert isinstance(traces, list)
    assert len(traces) > 0

    graph = trace_graph_for_trace_block_response(response=raw)

    assert isinstance(graph.get("events"), list)
    assert len(graph["events"]) > 0
    assert len(graph["events"]) <= len(traces)
    assert all(isinstance(e.get("event_id"), str) and e.get("tx_hash") for e in graph["events"])

    for edge in graph.get("edges") or []:
        from_id = edge.get("from")
        to_id = edge.get("to")
        assert isinstance(from_id, str) and isinstance(to_id, str)
        assert from_id.split(":", 1)[0] == to_id.split(":", 1)[0]
