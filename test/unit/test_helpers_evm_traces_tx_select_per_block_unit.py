import pytest


@pytest.mark.unit
def test_select_tx_hashes_for_block_limits_to_first_n_preserving_order():
    from helpers.evm.traces.tx_select import select_tx_hashes_for_block

    txs = ["0x3", "0x1", "0x2"]
    assert select_tx_hashes_for_block(txs, max_tx_per_block=3) == ["0x3", "0x1", "0x2"]
    assert select_tx_hashes_for_block(txs, max_tx_per_block=2) == ["0x3", "0x1"]
