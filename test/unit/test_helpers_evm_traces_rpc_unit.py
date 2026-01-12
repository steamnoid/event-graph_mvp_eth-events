import pytest


@pytest.mark.unit
def test_build_trace_block_request_uses_hex_quantity_param():
    from helpers.evm.traces.rpc import build_trace_block_request

    assert build_trace_block_request(block_number=16, request_id=1) == {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "trace_block",
        "params": ["0x10"],
    }


@pytest.mark.unit
def test_build_trace_transaction_request_uses_tx_hash_param():
    from helpers.evm.traces.rpc import build_trace_transaction_request

    assert build_trace_transaction_request(tx_hash="0xabc", request_id=7) == {
        "jsonrpc": "2.0",
        "id": 7,
        "method": "trace_transaction",
        "params": ["0xabc"],
    }


@pytest.mark.unit
def test_build_trace_block_requests_for_range_builds_inclusive_range_with_incrementing_ids():
    from helpers.evm.traces.rpc import build_trace_block_requests_for_range

    assert build_trace_block_requests_for_range(from_block=16, to_block=18, start_request_id=10) == [
        {"jsonrpc": "2.0", "id": 10, "method": "trace_block", "params": ["0x10"]},
        {"jsonrpc": "2.0", "id": 11, "method": "trace_block", "params": ["0x11"]},
        {"jsonrpc": "2.0", "id": 12, "method": "trace_block", "params": ["0x12"]},
    ]


@pytest.mark.unit
def test_build_trace_transaction_requests_uses_given_order_and_incrementing_ids():
    from helpers.evm.traces.rpc import build_trace_transaction_requests

    assert build_trace_transaction_requests(tx_hashes=["0xbbb", "0xaaa"], start_request_id=5) == [
        {"jsonrpc": "2.0", "id": 5, "method": "trace_transaction", "params": ["0xbbb"]},
        {"jsonrpc": "2.0", "id": 6, "method": "trace_transaction", "params": ["0xaaa"]},
    ]


@pytest.mark.unit
def test_build_trace_block_requests_for_range_raises_on_inverted_range():
    from helpers.evm.traces.rpc import build_trace_block_requests_for_range

    with pytest.raises(ValueError, match="from_block must be <= to_block"):
        build_trace_block_requests_for_range(from_block=20, to_block=19, start_request_id=1)
