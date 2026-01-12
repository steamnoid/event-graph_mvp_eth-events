import pytest


@pytest.mark.unit
def test_trace_graph_to_neo4j_graph_uses_graph_hash_as_run_id():
    from helpers.evm.traces.hash import trace_graph_hash
    from helpers.evm.traces.neo4j import trace_graph_to_neo4j_graph

    trace_graph = {
        "events": [
            {"event_id": "0xabc:[]", "tx_hash": "0xabc", "trace_address": [], "type": "call"},
            {"event_id": "0xabc:[0]", "tx_hash": "0xabc", "trace_address": [0], "type": "call"},
        ],
        "edges": [
            {"from": "0xabc:[]", "to": "0xabc:[0]"},
        ],
    }

    neo4j_graph = trace_graph_to_neo4j_graph(trace_graph)

    assert neo4j_graph == {
        "run_id": trace_graph_hash(trace_graph),
        "events": trace_graph["events"],
        "edges": trace_graph["edges"],
    }


@pytest.mark.unit
def test_trace_transaction_response_to_neo4j_graph_wraps_graph_with_run_id():
    from helpers.evm.traces.hash import trace_graph_hash
    from helpers.evm.traces.neo4j import trace_transaction_response_to_neo4j_graph
    from helpers.evm.traces.transformer import trace_graph_for_trace_transaction_response

    response = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {"transactionHash": "0xabc", "traceAddress": [], "type": "call"},
            {"transactionHash": "0xabc", "traceAddress": ["0x0"], "type": "call"},
        ],
    }

    trace_graph = trace_graph_for_trace_transaction_response(response=response)
    neo4j_graph = trace_transaction_response_to_neo4j_graph(response)

    assert neo4j_graph == {
        "run_id": trace_graph_hash(trace_graph),
        "events": trace_graph["events"],
        "edges": trace_graph["edges"],
    }


@pytest.mark.unit
def test_trace_block_response_to_neo4j_graph_wraps_graph_with_run_id():
    from helpers.evm.traces.hash import trace_graph_hash
    from helpers.evm.traces.neo4j import trace_block_response_to_neo4j_graph
    from helpers.evm.traces.transformer import trace_graph_for_trace_block_response

    response = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "trace": [
                {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
                {"transactionHash": "0xaaa", "traceAddress": ["0x0"], "type": "call"},
                {"transactionHash": "0xbbb", "traceAddress": [], "type": "call"},
                {"transactionHash": "0xbbb", "traceAddress": ["0x0"], "type": "call"},
            ]
        },
    }

    trace_graph = trace_graph_for_trace_block_response(response=response)
    neo4j_graph = trace_block_response_to_neo4j_graph(response)

    assert neo4j_graph == {
        "run_id": trace_graph_hash(trace_graph),
        "events": trace_graph["events"],
        "edges": trace_graph["edges"],
    }


@pytest.mark.unit
def test_trace_transaction_fixture_file_to_neo4j_graph_loads_and_wraps(tmp_path):
    import json

    from helpers.evm.traces.hash import trace_graph_hash
    from helpers.evm.traces.neo4j import trace_transaction_fixture_file_to_neo4j_graph
    from helpers.evm.traces.transformer import trace_graph_for_trace_transaction_response

    fixture_file = tmp_path / "trace_transaction.json"
    response = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {"transactionHash": "0xabc", "traceAddress": [], "type": "call"},
            {"transactionHash": "0xabc", "traceAddress": ["0x0"], "type": "call"},
        ],
    }
    fixture_file.write_text(json.dumps(response), encoding="utf-8")

    trace_graph = trace_graph_for_trace_transaction_response(response=response)
    neo4j_graph = trace_transaction_fixture_file_to_neo4j_graph(str(fixture_file))

    assert neo4j_graph == {
        "run_id": trace_graph_hash(trace_graph),
        "events": trace_graph["events"],
        "edges": trace_graph["edges"],
    }


@pytest.mark.unit
def test_trace_block_fixture_file_to_neo4j_graph_loads_and_wraps(tmp_path):
    import json

    from helpers.evm.traces.hash import trace_graph_hash
    from helpers.evm.traces.neo4j import trace_block_fixture_file_to_neo4j_graph
    from helpers.evm.traces.transformer import trace_graph_for_trace_block_response

    fixture_file = tmp_path / "trace_block.json"
    response = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
            {"transactionHash": "0xbbb", "traceAddress": [], "type": "call"},
        ],
    }
    fixture_file.write_text(json.dumps(response), encoding="utf-8")

    trace_graph = trace_graph_for_trace_block_response(response=response)
    neo4j_graph = trace_block_fixture_file_to_neo4j_graph(str(fixture_file))

    assert neo4j_graph == {
        "run_id": trace_graph_hash(trace_graph),
        "events": trace_graph["events"],
        "edges": trace_graph["edges"],
    }
