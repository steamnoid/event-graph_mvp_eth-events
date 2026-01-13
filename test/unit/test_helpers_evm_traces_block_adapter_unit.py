import pytest


@pytest.mark.unit
def test_fetch_tx_hashes_from_block_sorts_and_hexes(monkeypatch):
    from helpers.evm.traces.block_adapter import fetch_tx_hashes_from_block

    class _Hexy:
        def __init__(self, h: str):
            self._h = h

        def hex(self) -> str:
            return self._h

    class FakeEth:
        def get_block(self, number, full_transactions=False):
            assert number == 123
            assert full_transactions is False
            # intentionally unsorted
            return {"transactions": [_Hexy("0xbbb"), _Hexy("0xaaa")]}

    class FakeW3:
        eth = FakeEth()

    def fake_get_web3(provider_url: str):
        assert provider_url == "http://example"
        return FakeW3()

    import helpers.evm.traces.block_adapter as adapter

    monkeypatch.setattr(adapter, "_get_web3", fake_get_web3)

    txs = fetch_tx_hashes_from_block(provider_url="http://example", block_number=123)
    assert txs == ["0xaaa", "0xbbb"]
