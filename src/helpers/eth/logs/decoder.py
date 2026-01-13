from __future__ import annotations

from web3 import Web3

from eth_abi import decode as abi_decode


_ERC20_TRANSFER_TOPIC0 = "0x" + Web3.keccak(text="Transfer(address,address,uint256)").hex()
_ERC20_APPROVAL_TOPIC0 = "0x" + Web3.keccak(text="Approval(address,address,uint256)").hex()

_UNISWAP_V2_PAIR_CREATED_TOPIC0 = (
    "0x" + Web3.keccak(text="PairCreated(address,address,address,uint256)").hex()
)

_UNISWAP_V2_SWAP_TOPIC0 = "0x" + Web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
_UNISWAP_V2_MINT_TOPIC0 = "0x" + Web3.keccak(text="Mint(address,uint256,uint256)").hex()
_UNISWAP_V2_BURN_TOPIC0 = "0x" + Web3.keccak(text="Burn(address,uint256,uint256,address)").hex()
_UNISWAP_V2_SYNC_TOPIC0 = "0x" + Web3.keccak(text="Sync(uint112,uint112)").hex()


def _as_0x_prefixed_hex(value) -> str:
    if isinstance(value, str):
        return value if value.startswith("0x") else "0x" + value
    if hasattr(value, "hex"):
        hex_str = value.hex()
        return hex_str if hex_str.startswith("0x") else "0x" + hex_str
    return str(value)


def _indexed_topic_to_checksum_address(topic) -> str:
    topic_hex = _as_0x_prefixed_hex(topic)
    return Web3.to_checksum_address("0x" + topic_hex[-40:])


def _as_bytes(value) -> bytes:
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    value_hex = _as_0x_prefixed_hex(value)
    return bytes.fromhex(value_hex[2:])


def _decode_two_uint256(data_value) -> tuple[int, int] | None:
    data_bytes = _as_bytes(data_value)
    if not data_bytes:
        return None
    a, b = abi_decode(["uint256", "uint256"], data_bytes)
    return int(a), int(b)


def decode_transfer_args(raw_log: dict) -> dict | None:
    topics = raw_log.get("topics") or []
    if len(topics) < 3:
        return None

    topic0_hex = _as_0x_prefixed_hex(topics[0])
    if topic0_hex != _ERC20_TRANSFER_TOPIC0:
        return None

    from_addr = _indexed_topic_to_checksum_address(topics[1])
    to_addr = _indexed_topic_to_checksum_address(topics[2])

    data_hex = _as_0x_prefixed_hex(raw_log.get("data"))
    value = int(data_hex, 16) if data_hex else 0

    return {"from": from_addr, "to": to_addr, "value": value}


def decode_approval_args(raw_log: dict) -> dict | None:
    topics = raw_log.get("topics") or []
    if len(topics) < 3:
        return None

    topic0_hex = _as_0x_prefixed_hex(topics[0])
    if topic0_hex != _ERC20_APPROVAL_TOPIC0:
        return None

    owner = _indexed_topic_to_checksum_address(topics[1])
    spender = _indexed_topic_to_checksum_address(topics[2])

    data_hex = _as_0x_prefixed_hex(raw_log.get("data"))
    value = int(data_hex, 16) if data_hex else 0

    return {"owner": owner, "spender": spender, "value": value}


def decode_swap_args(raw_log: dict) -> dict | None:
    topics = raw_log.get("topics") or []
    if len(topics) < 3:
        return None

    topic0_hex = _as_0x_prefixed_hex(topics[0])
    if topic0_hex != _UNISWAP_V2_SWAP_TOPIC0:
        return None

    data_bytes = _as_bytes(raw_log.get("data"))
    if not data_bytes:
        return None
    amount0_in, amount1_in, amount0_out, amount1_out = abi_decode(
        ["uint256", "uint256", "uint256", "uint256"],
        data_bytes,
    )

    return {
        "sender": _indexed_topic_to_checksum_address(topics[1]),
        "to": _indexed_topic_to_checksum_address(topics[2]),
        "amount0In": int(amount0_in),
        "amount1In": int(amount1_in),
        "amount0Out": int(amount0_out),
        "amount1Out": int(amount1_out),
    }


def decode_mint_args(raw_log: dict) -> dict | None:
    topics = raw_log.get("topics") or []
    if len(topics) < 2:
        return None

    topic0_hex = _as_0x_prefixed_hex(topics[0])
    if topic0_hex != _UNISWAP_V2_MINT_TOPIC0:
        return None

    decoded = _decode_two_uint256(raw_log.get("data"))
    if decoded is None:
        return None
    amount0, amount1 = decoded
    return {
        "sender": _indexed_topic_to_checksum_address(topics[1]),
        "amount0": amount0,
        "amount1": amount1,
    }


def decode_burn_args(raw_log: dict) -> dict | None:
    topics = raw_log.get("topics") or []
    if len(topics) < 3:
        return None

    topic0_hex = _as_0x_prefixed_hex(topics[0])
    if topic0_hex != _UNISWAP_V2_BURN_TOPIC0:
        return None

    decoded = _decode_two_uint256(raw_log.get("data"))
    if decoded is None:
        return None
    amount0, amount1 = decoded
    return {
        "sender": _indexed_topic_to_checksum_address(topics[1]),
        "to": _indexed_topic_to_checksum_address(topics[2]),
        "amount0": amount0,
        "amount1": amount1,
    }


def decode_sync_args(raw_log: dict) -> dict | None:
    topics = raw_log.get("topics") or []
    if len(topics) < 1:
        return None

    topic0_hex = _as_0x_prefixed_hex(topics[0])
    if topic0_hex != _UNISWAP_V2_SYNC_TOPIC0:
        return None

    data_bytes = _as_bytes(raw_log.get("data"))
    if not data_bytes:
        return None

    reserve0, reserve1 = abi_decode(["uint112", "uint112"], data_bytes)
    return {"reserve0": int(reserve0), "reserve1": int(reserve1)}


def decode_pair_created_args(raw_log: dict) -> dict | None:
    topics = raw_log.get("topics") or []
    if len(topics) < 3:
        return None

    topic0_hex = _as_0x_prefixed_hex(topics[0])
    if topic0_hex != _UNISWAP_V2_PAIR_CREATED_TOPIC0:
        return None

    data_bytes = _as_bytes(raw_log.get("data"))
    if not data_bytes:
        return None

    pair_address, all_pairs_length = abi_decode(["address", "uint256"], data_bytes)

    return {
        "token0": _indexed_topic_to_checksum_address(topics[1]),
        "token1": _indexed_topic_to_checksum_address(topics[2]),
        "pair": Web3.to_checksum_address(pair_address),
        "allPairsLength": int(all_pairs_length),
    }


_KNOWN_EVENT_SPECS = [
    (_ERC20_TRANSFER_TOPIC0, "Transfer", decode_transfer_args),
    (_ERC20_APPROVAL_TOPIC0, "Approval", decode_approval_args),
    (_UNISWAP_V2_PAIR_CREATED_TOPIC0, "PairCreated", decode_pair_created_args),
    (_UNISWAP_V2_SWAP_TOPIC0, "Swap", decode_swap_args),
    (_UNISWAP_V2_MINT_TOPIC0, "Mint", decode_mint_args),
    (_UNISWAP_V2_BURN_TOPIC0, "Burn", decode_burn_args),
    (_UNISWAP_V2_SYNC_TOPIC0, "Sync", decode_sync_args),
]

_TOPIC0_TO_EVENT_NAME = {topic0: event_name for topic0, event_name, _ in _KNOWN_EVENT_SPECS}
_TOPIC0_TO_DECODER = {topic0: decoder for topic0, _, decoder in _KNOWN_EVENT_SPECS}


def decode_event_name(raw_log: dict) -> str | None:
    topics = raw_log.get("topics") or []
    if not topics:
        return None

    topic0_hex = _as_0x_prefixed_hex(topics[0])
    event_name = _TOPIC0_TO_EVENT_NAME.get(topic0_hex)
    return event_name if event_name else f"Unknown({topic0_hex})"


def _decode_known_args(topic0_hex: str | None, raw_log: dict) -> dict | None:
    decoder = _TOPIC0_TO_DECODER.get(topic0_hex)
    return decoder(raw_log) if decoder else None


def decode_log(raw_log: dict) -> dict:
    """Unified decoder: returns event_name plus decoded args (if known)."""
    event_name = decode_event_name(raw_log)
    topics = raw_log.get("topics") or []
    topic0_hex = _as_0x_prefixed_hex(topics[0]) if topics else None

    return {"event_name": event_name, "decoded": _decode_known_args(topic0_hex, raw_log)}
