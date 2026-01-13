import pytest


@pytest.mark.unit
def test_receipt_has_erc20_transfer_true_when_log_topic0_matches_transfer_sig():
    from helpers.evm.traces.erc20_filter import ERC20_TRANSFER_TOPIC0, receipt_has_erc20_transfer

    receipt = {
        "logs": [
            {"topics": [ERC20_TRANSFER_TOPIC0, "0x" + "00" * 32, "0x" + "11" * 32]},
        ]
    }

    assert receipt_has_erc20_transfer(receipt) is True


@pytest.mark.unit
def test_receipt_has_erc20_transfer_false_when_no_logs_or_no_topics_match():
    from helpers.evm.traces.erc20_filter import receipt_has_erc20_transfer

    assert receipt_has_erc20_transfer({"logs": []}) is False
    assert receipt_has_erc20_transfer({}) is False

    receipt = {
        "logs": [
            {"topics": ["0x" + "aa" * 32]},
            {"topics": []},
        ]
    }

    assert receipt_has_erc20_transfer(receipt) is False
