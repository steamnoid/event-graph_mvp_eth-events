from __future__ import annotations

import pytest


@pytest.mark.unit
def test_raw_log_round_trips_through_kafka_codec_bytes() -> None:
    from kafka_eth_log_codec import decode_raw_log, encode_raw_log

    raw_log = {
        "address": "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc",
        "blockNumber": 24216800,
        "transactionHash": "0x" + "11" * 32,
        "logIndex": 12,
        "transactionIndex": 7,
        "data": "0x" + "00" * 32,
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "0x" + "00" * 12 + "aa" * 20,
            "0x" + "00" * 12 + "bb" * 20,
        ],
    }

    payload = encode_raw_log(raw_log)
    decoded = decode_raw_log(payload)

    assert decoded == raw_log
