import pytest


@pytest.mark.unit
def test_select_tx_hashes_for_block_range_filters_by_erc20_transfer_receipts(monkeypatch):
    from helpers.evm.traces import block_adapter
    from helpers.evm.traces import receipt_adapter

    def fake_fetch_tx_hashes_from_block(*, provider_url: str, block_number: int) -> list[str]:
        assert provider_url == "http://example.invalid"
        if block_number == 10:
            return ["0xA", "0xB", "0xC"]
        if block_number == 11:
            return ["0xD"]
        raise AssertionError(f"unexpected block_number: {block_number}")

    def fake_select_tx_hashes_with_erc20_transfer_receipts(*, provider_url: str, tx_hashes: list[str], max_tx: int | None) -> list[str]:
        assert provider_url == "http://example.invalid"
        assert max_tx == 2
        if tx_hashes == ["0xA", "0xB", "0xC"]:
            return ["0xB"]
        if tx_hashes == ["0xD"]:
            return ["0xD"]
        raise AssertionError(f"unexpected tx_hashes: {tx_hashes}")

    monkeypatch.setattr(block_adapter, "fetch_tx_hashes_from_block", fake_fetch_tx_hashes_from_block)
    monkeypatch.setattr(
        receipt_adapter,
        "select_tx_hashes_with_erc20_transfer_receipts",
        fake_select_tx_hashes_with_erc20_transfer_receipts,
    )

    from helpers.evm.traces.tx_select import select_tx_hashes_for_block_range_with_erc20_transfer_receipts

    assert select_tx_hashes_for_block_range_with_erc20_transfer_receipts(
        provider_url="http://example.invalid",
        from_block=10,
        to_block=11,
        max_tx_per_block=2,
    ) == ["0xB", "0xD"]
