"""Ethereum log fetching and normalization helpers.

This module contains small, deterministic helpers used by Airflow DAG tasks.
Network I/O is limited to explicit fetch functions; all other helpers are pure.

Key outputs are JSON-serializable dictionaries suitable for Airflow TaskFlow XCom.
"""

from typing import Dict, List, Optional, Tuple

from web3 import HTTPProvider, Web3


def _event_signature_map() -> Dict[str, str]:
    """Return a mapping of topic0 (event signature hash) to a stable event name.

    Notes:
    - The contract used in this MVP is a Uniswap V2 pair, so V2 event signatures
      are included.
    - A small set of Uniswap V3 pool signatures is included for reuse.
    - The mapping is intentionally minimal and stable.
    """
    # NOTE: The address used in tests (0xB4e16...) is a Uniswap V2 pair.
    # Keep a minimal map for both Uniswap V2 and V3, plus ERC-20 Transfer.
    signatures = [
        # ERC-20
        "Transfer(address,address,uint256)",

        # Uniswap V2 Pair
        "Swap(address,uint256,uint256,uint256,uint256,address)",
        "Mint(address,uint256,uint256)",
        "Burn(address,uint256,uint256,address)",
        "Sync(uint112,uint112)",

        # Uniswap V3 Pool
        "Swap(address,address,int256,int256,uint160,uint128,int24)",
        "Mint(address,address,int24,int24,uint128,uint256,uint256)",
        "Burn(address,int24,int24,uint128,uint256,uint256)",
        "Collect(address,address,int24,int24,uint128,uint128)",
    ]
    return {Web3.keccak(text=sig).hex(): sig.split("(")[0] for sig in signatures}


_SIGNATURE_TO_NAME = _event_signature_map()


def _lookup_event_name(log: Dict) -> Optional[str]:
    """Return a human-readable event name for a raw log.

    If the signature is unknown, returns a readable sentinel of the form
    `Unknown(0x...)`.
    """
    topics = log.get("topics") or []
    if not topics:
        return None
    signature = topics[0].hex()
    # Fallback to a readable sentinel rather than None.
    return _SIGNATURE_TO_NAME.get(signature, f"Unknown({signature})")


def _event_id(tx_hash: Optional[str], log_index: Optional[int]) -> Optional[str]:
    """Create stable event identity: `tx_hash:log_index`.

    Returns None if inputs are missing.
    """
    if not tx_hash or log_index is None:
        return None
    return f"{tx_hash}:{log_index}"


def _normalize_log(log: Dict) -> Dict:
    """Normalize a raw web3 log into a JSON-serializable event dict.

    The returned dict is intended to be:
    - stable (fields needed for ordering + causality)
    - JSON-serializable (safe for Airflow XCom)
    - minimally sufficient (raw log is intentionally excluded)
    """
    topics = log.get("topics") or []
    topic0_hash = topics[0].hex() if topics else None
    tx_hash = log.get("transactionHash").hex() if log.get("transactionHash") else None
    log_index = log.get("logIndex")

    data = log.get("data")
    data_hex = data.hex() if hasattr(data, "hex") else data

    address = log.get("address")
    address_str = address if isinstance(address, str) else str(address) if address is not None else None
    return {
        # Identity
        "event_id": _event_id(tx_hash, log_index),
        "tx_hash": tx_hash,
        "log_index": log_index,

        # Type
        "event": topic0_hash,
        "event_name": _lookup_event_name(log),

        # Ordering/context
        "block_number": log.get("blockNumber"),
        "transaction_index": log.get("transactionIndex"),
        "address": address_str,

        # Raw payload (still decodable)
        "topics": [t.hex() for t in topics],
        "data": data_hex,

        # Keep original log for now (not JSON-safe, but useful during exploration)
        # "raw": log,
    }


def validate_block_range(from_block: int, to_block: int) -> Tuple[int, int]:
    """Validate and normalize an inclusive Ethereum block range.

    Raises:
    - TypeError: if either input is not an int
    - ValueError: if values are negative or from_block > to_block
    """
    if not isinstance(from_block, int) or not isinstance(to_block, int):
        raise TypeError("from_block and to_block must be integers")
    if from_block < 0 or to_block < 0:
        raise ValueError("from_block/to_block must be non-negative")
    if from_block > to_block:
        raise ValueError("from_block must be <= to_block")
    return int(from_block), int(to_block)


def _web3(provider_url: str) -> Web3:
    """Create a Web3 instance configured for HTTP with a reasonable timeout."""
    provider = HTTPProvider(provider_url, request_kwargs={"timeout": 30})
    return Web3(provider)


def _require_connected(w3: Web3, provider_url: str) -> None:
    """Fail fast if the Web3 provider cannot be reached."""
    if not w3.is_connected():
        raise ConnectionError("Unable to reach {}".format(provider_url))


def _sort_events_in_block_order(events: List[Dict]) -> List[Dict]:
    """Sort events in canonical chain order (block, tx index, log index)."""
    events.sort(key=lambda e: (e.get("block_number", -1), e.get("transaction_index", -1), e.get("log_index", -1)))
    return events


def fetch_events_in_block_range(
    contract_address: str,
    provider_url: str,
    from_block: int,
    to_block: int,
) -> List[Dict]:
    """Fetch all logs for a contract within an explicit inclusive block range.

    The returned list is normalized and sorted in canonical chain order.
    """
    from_block, to_block = validate_block_range(from_block, to_block)

    w3 = _web3(provider_url)
    _require_connected(w3, provider_url)

    target_address = w3.to_checksum_address(contract_address)
    logs = w3.eth.get_logs(
        {
            "address": target_address,
            "fromBlock": int(from_block),
            "toBlock": int(to_block),
        }
    )

    events = [_normalize_log(log) for log in logs]
    return _sort_events_in_block_order(events)
