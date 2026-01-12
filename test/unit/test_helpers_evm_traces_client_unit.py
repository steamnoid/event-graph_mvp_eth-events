import pytest


@pytest.mark.unit
def test_fetch_trace_block_graph_calls_json_rpc_and_returns_graph(monkeypatch):
    from helpers.evm.traces.client import fetch_trace_block_graph
    from helpers.evm.traces.hash import trace_graph_hash

    calls: list[dict] = []

    def _fake_post_json_rpc(*, url: str, payload: dict, timeout_s: int):
        calls.append({"url": url, "payload": payload, "timeout_s": timeout_s})
        return {
            "result": {
                "trace": [
                    {"transactionHash": "0xbbb", "traceAddress": [], "type": "call"},
                    {"transactionHash": "0xbbb", "traceAddress": ["0x0"], "type": "call"},
                    {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
                    {"transactionHash": "0xaaa", "traceAddress": ["0x0"], "type": "call"},
                ]
            }
        }

    import helpers.evm.traces.http as http

    monkeypatch.setattr(http, "post_json_rpc", _fake_post_json_rpc)

    expected_graph = {
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
    expected_graph["graph_hash"] = trace_graph_hash(expected_graph)

    assert fetch_trace_block_graph(url="http://example", block_number=16, request_id=9, timeout_s=3) == expected_graph

    assert calls == [
        {
            "url": "http://example",
            "payload": {"jsonrpc": "2.0", "id": 9, "method": "trace_block", "params": ["0x10"]},
            "timeout_s": 3,
        }
    ]


@pytest.mark.unit
def test_fetch_trace_transaction_graph_calls_json_rpc_and_returns_graph(monkeypatch):
    from helpers.evm.traces.client import fetch_trace_transaction_graph
    from helpers.evm.traces.hash import trace_graph_hash

    calls: list[dict] = []

    def _fake_post_json_rpc(*, url: str, payload: dict, timeout_s: int):
        calls.append({"url": url, "payload": payload, "timeout_s": timeout_s})
        return {
            "result": [
                {"transactionHash": "0xabc", "traceAddress": [], "type": "call"},
                {"transactionHash": "0xabc", "traceAddress": ["0x0"], "type": "call"},
            ]
        }

    import helpers.evm.traces.http as http

    monkeypatch.setattr(http, "post_json_rpc", _fake_post_json_rpc)

    expected_graph = {
        "events": [
            {"event_id": "0xabc:[]", "tx_hash": "0xabc", "trace_address": [], "type": "call"},
            {"event_id": "0xabc:[0]", "tx_hash": "0xabc", "trace_address": [0], "type": "call"},
        ],
        "edges": [
            {"from": "0xabc:[]", "to": "0xabc:[0]"},
        ],
    }
    expected_graph["graph_hash"] = trace_graph_hash(expected_graph)

    assert fetch_trace_transaction_graph(url="http://example", tx_hash="0xabc", request_id=7, timeout_s=3) == expected_graph

    assert calls == [
        {
            "url": "http://example",
            "payload": {"jsonrpc": "2.0", "id": 7, "method": "trace_transaction", "params": ["0xabc"]},
            "timeout_s": 3,
        }
    ]


@pytest.mark.unit
def test_fetch_trace_block_range_graph_uses_json_rpc_batch_and_sorts_responses_by_id(monkeypatch):
    from helpers.evm.traces.client import fetch_trace_block_range_graph
    from helpers.evm.traces.hash import trace_graph_hash

    calls: list[dict] = []

    def _fake_post_json_rpc(*, url: str, payload: dict, timeout_s: int):
        calls.append({"url": url, "payload": payload, "timeout_s": timeout_s})

        # Return in reverse id order to prove we sort deterministically.
        return [
            {
                "jsonrpc": "2.0",
                "id": 11,
                "result": {
                    "trace": [
                        {"transactionHash": "0xbbb", "traceAddress": [], "type": "call"},
                        {"transactionHash": "0xbbb", "traceAddress": ["0x0"], "type": "call"},
                    ]
                },
            },
            {
                "jsonrpc": "2.0",
                "id": 10,
                "result": {
                    "trace": [
                        {"transactionHash": "0xaaa", "traceAddress": [], "type": "call"},
                        {"transactionHash": "0xaaa", "traceAddress": ["0x0"], "type": "call"},
                    ]
                },
            },
        ]

    import helpers.evm.traces.http as http

    monkeypatch.setattr(http, "post_json_rpc", _fake_post_json_rpc)

    expected_graph = {
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
    expected_graph["graph_hash"] = trace_graph_hash(expected_graph)

    assert fetch_trace_block_range_graph(
        url="http://example",
        from_block=16,
        to_block=17,
        start_request_id=10,
        timeout_s=3,
    ) == expected_graph

    assert calls == [
        {
            "url": "http://example",
            "payload": [
                {"jsonrpc": "2.0", "id": 10, "method": "trace_block", "params": ["0x10"]},
                {"jsonrpc": "2.0", "id": 11, "method": "trace_block", "params": ["0x11"]},
            ],
            "timeout_s": 3,
        }
    ]
