import pytest


@pytest.mark.unit
def test_trace_edge_for_child_trace_address_links_parent_to_child_event_ids():
    from helpers.evm.traces.transformer import trace_edge_for_child

    assert trace_edge_for_child(tx_hash="0xabc", child_trace_address=[0, 1, 0]) == {
        "from": "0xabc:[0,1]",
        "to": "0xabc:[0,1,0]",
    }


@pytest.mark.unit
def test_trace_edges_for_transaction_skips_root_and_returns_child_edges():
    from helpers.evm.traces.transformer import trace_edges_for_transaction

    traces = [
        {"traceAddress": []},
        {"traceAddress": ["0x0"]},
    ]

    assert trace_edges_for_transaction(tx_hash="0xabc", traces=traces) == [
        {"from": "0xabc:[]", "to": "0xabc:[0]"},
    ]


@pytest.mark.unit
def test_trace_events_for_transaction_builds_event_id_and_normalizes_trace_address():
    from helpers.evm.traces.transformer import trace_events_for_transaction

    traces = [
        {"traceAddress": [], "type": "call"},
        {"traceAddress": ["0x0"], "type": "call", "action": {"callType": "call"}},
    ]

    assert trace_events_for_transaction(tx_hash="0xabc", traces=traces) == [
        {"event_id": "0xabc:[]", "tx_hash": "0xabc", "trace_address": [], "type": "call"},
        {"event_id": "0xabc:[0]", "tx_hash": "0xabc", "trace_address": [0], "type": "call"},
    ]


@pytest.mark.unit
def test_trace_graph_for_transaction_returns_events_and_edges():
    from helpers.evm.traces.transformer import trace_graph_for_transaction

    traces = [
        {"traceAddress": [], "type": "call"},
        {"traceAddress": ["0x0"], "type": "call"},
    ]

    assert trace_graph_for_transaction(tx_hash="0xabc", traces=traces) == {
        "events": [
            {"event_id": "0xabc:[]", "tx_hash": "0xabc", "trace_address": [], "type": "call"},
            {"event_id": "0xabc:[0]", "tx_hash": "0xabc", "trace_address": [0], "type": "call"},
        ],
        "edges": [
            {"from": "0xabc:[]", "to": "0xabc:[0]"},
        ],
    }


@pytest.mark.unit
def test_trace_graph_for_block_groups_by_transaction_hash_and_combines():
    from helpers.evm.traces.transformer import trace_graph_for_block

    traces = [
        {"transactionHash": "0xbbb", "traceAddress": [], "type": "call"},
        {"transactionHash": "0xbbb", "traceAddress": ["0x0"], "type": "call"},
        {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
        {"transactionHash": "0xaaa", "traceAddress": ["0x0"], "type": "call"},
    ]

    assert trace_graph_for_block(traces=traces) == {
        "events": [
            {"event_id": "0xaaa:[]", "tx_hash": "0xaaa", "trace_address": [], "type": "call"},
            {"event_id": "0xaaa:[0]", "tx_hash": "0xaaa", "trace_address": [0], "type": "call"},
            {"event_id": "0xbbb:[]", "tx_hash": "0xbbb", "trace_address": [], "type": "call"},
            {"event_id": "0xbbb:[0]", "tx_hash": "0xbbb", "trace_address": [0], "type": "call"},
        ],
        "edges": [
            {"from": "0xaaa:[]", "to": "0xaaa:[0]"},
            {"from": "0xbbb:[]", "to": "0xbbb:[0]"},
        ],
    }


@pytest.mark.unit
def test_traces_from_trace_block_response_returns_trace_list():
    from helpers.evm.traces.transformer import traces_from_trace_block_response

    response = {
        "trace": [
            {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
        ]
    }

    assert traces_from_trace_block_response(response) == [
        {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
    ]


@pytest.mark.unit
def test_traces_from_trace_block_response_supports_json_rpc_result_wrapper():
    from helpers.evm.traces.transformer import traces_from_trace_block_response

    response = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "trace": [
                {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
            ]
        },
    }

    assert traces_from_trace_block_response(response) == [
        {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
    ]


@pytest.mark.unit
def test_traces_from_trace_block_response_supports_json_rpc_result_list():
    from helpers.evm.traces.transformer import traces_from_trace_block_response

    response = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
        ],
    }

    assert traces_from_trace_block_response(response) == [
        {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
    ]


@pytest.mark.unit
def test_trace_graph_for_trace_block_response_builds_block_graph():
    from helpers.evm.traces.transformer import trace_graph_for_trace_block_response

    response = {
        "result": {
            "trace": [
                {"transactionHash": "0xbbb", "traceAddress": [], "type": "call"},
                {"transactionHash": "0xbbb", "traceAddress": ["0x0"], "type": "call"},
                {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
                {"transactionHash": "0xaaa", "traceAddress": ["0x0"], "type": "call"},
            ]
        }
    }

    assert trace_graph_for_trace_block_response(response=response) == {
        "events": [
            {"event_id": "0xaaa:[]", "tx_hash": "0xaaa", "trace_address": [], "type": "call"},
            {"event_id": "0xaaa:[0]", "tx_hash": "0xaaa", "trace_address": [0], "type": "call"},
            {"event_id": "0xbbb:[]", "tx_hash": "0xbbb", "trace_address": [], "type": "call"},
            {"event_id": "0xbbb:[0]", "tx_hash": "0xbbb", "trace_address": [0], "type": "call"},
        ],
        "edges": [
            {"from": "0xaaa:[]", "to": "0xaaa:[0]"},
            {"from": "0xbbb:[]", "to": "0xbbb:[0]"},
        ],
    }


@pytest.mark.unit
def test_trace_graph_for_trace_transaction_response_builds_transaction_graph():
    from helpers.evm.traces.transformer import trace_graph_for_trace_transaction_response

    response = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {"transactionHash": "0xabc", "traceAddress": [], "type": "call"},
            {"transactionHash": "0xabc", "traceAddress": ["0x0"], "type": "call"},
        ],
    }

    assert trace_graph_for_trace_transaction_response(response=response) == {
        "events": [
            {"event_id": "0xabc:[]", "tx_hash": "0xabc", "trace_address": [], "type": "call"},
            {"event_id": "0xabc:[0]", "tx_hash": "0xabc", "trace_address": [0], "type": "call"},
        ],
        "edges": [
            {"from": "0xabc:[]", "to": "0xabc:[0]"},
        ],
    }


@pytest.mark.unit
def test_trace_graph_for_trace_transaction_response_raises_on_mixed_tx_hashes():
    from helpers.evm.traces.transformer import trace_graph_for_trace_transaction_response

    response = {
        "result": [
            {"transactionHash": "0xabc", "traceAddress": [], "type": "call"},
            {"transactionHash": "0xdef", "traceAddress": ["0x0"], "type": "call"},
        ]
    }

    with pytest.raises(ValueError, match="mixed transaction hashes"):
        trace_graph_for_trace_transaction_response(response=response)
