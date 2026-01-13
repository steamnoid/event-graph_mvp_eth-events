import pytest


@pytest.mark.unit
def test_select_tx_hashes_scans_blocks_until_min_erc20_met(monkeypatch):
    from helpers.evm.traces import block_adapter
    from helpers.evm.traces import receipt_adapter

    calls: list[int] = []

    def fake_fetch_tx_hashes_from_block(*, provider_url: str, block_number: int) -> list[str]:
        assert provider_url == "http://example.invalid"
        calls.append(block_number)
        if block_number == 100:
            return ["0xA", "0xB"]
        if block_number == 99:
            return ["0xC"]
        if block_number == 98:
            return ["0xD"]
        raise AssertionError(f"unexpected block_number: {block_number}")

    def fake_select_tx_hashes_with_erc20_transfer_receipts(*, provider_url: str, tx_hashes: list[str], max_tx: int) -> list[str]:
        assert provider_url == "http://example.invalid"
        assert max_tx == 3
        if tx_hashes == ["0xA", "0xB"]:
            return ["0xA"]
        if tx_hashes == ["0xC"]:
            return ["0xC"]
        if tx_hashes == ["0xD"]:
            return ["0xD"]
        raise AssertionError(f"unexpected tx_hashes: {tx_hashes}")

    monkeypatch.setattr(block_adapter, "fetch_tx_hashes_from_block", fake_fetch_tx_hashes_from_block)
    monkeypatch.setattr(
        receipt_adapter,
        "select_tx_hashes_with_erc20_transfer_receipts",
        fake_select_tx_hashes_with_erc20_transfer_receipts,
    )

    from helpers.evm.traces.tx_select import select_tx_hashes_for_recent_blocks_with_erc20_transfer_receipts_until_min

    out = select_tx_hashes_for_recent_blocks_with_erc20_transfer_receipts_until_min(
        provider_url="http://example.invalid",
        to_block=100,
        max_blocks_to_scan=10,
        min_total_tx=3,
        max_tx_per_block=3,
    )

    assert out == ["0xA", "0xC", "0xD"]
    assert calls == [100, 99, 98]
