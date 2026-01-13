import pytest


@pytest.mark.unit
def test_select_tx_hashes_with_erc20_transfer_receipts_filters_and_limits(monkeypatch):
    from helpers.evm.traces.receipt_adapter import select_tx_hashes_with_erc20_transfer_receipts

    class _Eth:
        def get_transaction_receipt(self, tx_hash):
            # Only tx2 has a Transfer topic
            if tx_hash == "0xtx2":
                return {
                    "logs": [
                        {
                            "topics": [
                                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                            ]
                        }
                    ]
                }
            return {"logs": []}

    class _W3:
        eth = _Eth()

    monkeypatch.setattr("helpers.evm.traces.receipt_adapter._get_web3", lambda _url: _W3())

    txs = ["0xtx1", "0xtx2", "0xtx3"]
    assert select_tx_hashes_with_erc20_transfer_receipts(provider_url="http://rpc", tx_hashes=txs, max_tx=1) == ["0xtx2"]
