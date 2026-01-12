"""Unit test: decoding of known event logs into structured fields.

Goal: ensure normalized events contain decoded arguments for known signatures.
This keeps downstream graph logic readable (Swap vs raw topics/data).
"""

from eth_abi import encode
from hexbytes import HexBytes
from web3 import Web3

from dags.dag_helper_functions import events


def _topic_for_indexed_address(address: str) -> HexBytes:
    # topics store indexed values as 32-byte ABI words (left-padded)
    raw = address.lower().removeprefix("0x")
    return HexBytes("0x" + ("00" * 12) + raw)


def test_normalize_log_decodes_uniswap_v2_swap_event_amounts():
    """Given a Uniswap V2 Swap log, normalization exposes decoded amount fields."""

    swap_sig = "Swap(address,uint256,uint256,uint256,uint256,address)"
    topic0 = Web3.keccak(text=swap_sig).hex()

    sender = "0x" + ("11" * 20)
    to = "0x" + ("22" * 20)

    data = encode(
        ["uint256", "uint256", "uint256", "uint256"],
        [1, 2, 3, 4],
    )

    raw_log = {
        "address": "0x" + ("aa" * 20),
        "topics": [HexBytes(topic0), _topic_for_indexed_address(sender), _topic_for_indexed_address(to)],
        "data": data,
        "blockNumber": 123,
        "transactionIndex": 7,
        "logIndex": 9,
        "transactionHash": HexBytes("0x" + ("bb" * 32)),
    }

    normalized = events._normalize_log(raw_log)

    assert normalized["decoded"]["amount0In"] == 1
