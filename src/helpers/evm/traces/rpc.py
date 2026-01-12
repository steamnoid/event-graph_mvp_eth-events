def build_trace_block_request(*, block_number: int, request_id: int = 1) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "trace_block",
        "params": [hex(block_number)],
    }


def build_trace_transaction_request(*, tx_hash: str, request_id: int = 1) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "trace_transaction",
        "params": [tx_hash],
    }


def build_trace_block_requests_for_range(*, from_block: int, to_block: int, start_request_id: int = 1) -> list[dict]:
    if from_block > to_block:
        raise ValueError("from_block must be <= to_block")

    requests: list[dict] = []
    request_id = start_request_id
    for block_number in range(from_block, to_block + 1):
        requests.append(build_trace_block_request(block_number=block_number, request_id=request_id))
        request_id += 1
    return requests


def build_trace_transaction_requests(*, tx_hashes: list[str], start_request_id: int = 1) -> list[dict]:
    requests: list[dict] = []
    request_id = start_request_id
    for tx_hash in tx_hashes:
        requests.append(build_trace_transaction_request(tx_hash=tx_hash, request_id=request_id))
        request_id += 1
    return requests
