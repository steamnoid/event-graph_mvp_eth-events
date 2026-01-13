from __future__ import annotations

from typing import Any, List


def _get_web3(provider_url: str) -> Any:
    # Lazy import to keep DAG/module import-safe.
    from web3 import HTTPProvider, Web3

    return Web3(HTTPProvider(provider_url))


def _tx_hash_to_hex(tx_hash: Any) -> str | None:
    if tx_hash is None:
        return None

    # web3 often returns HexBytes which has .hex()
    hex_method = getattr(tx_hash, "hex", None)
    if callable(hex_method):
        value = hex_method()
        return value if isinstance(value, str) else None

    if isinstance(tx_hash, str):
        return tx_hash

    return None


def fetch_tx_hashes_from_block(*, provider_url: str, block_number: int) -> List[str]:
    w3 = _get_web3(provider_url)
    block = w3.eth.get_block(block_number, full_transactions=False)
    txs = (block or {}).get("transactions") or []

    out: List[str] = []
    for tx in txs:
        as_hex = _tx_hash_to_hex(tx)
        if as_hex:
            out.append(as_hex)

    # Deterministic ordering.
    return sorted(out)
