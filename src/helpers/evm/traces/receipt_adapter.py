from helpers.evm.traces.block_adapter import _get_web3
from helpers.evm.traces.erc20_filter import receipt_has_erc20_transfer


def select_tx_hashes_with_erc20_transfer_receipts(*, provider_url: str, tx_hashes: list[str], max_tx: int) -> list[str]:
    w3 = _get_web3(provider_url)

    selected: list[str] = []
    for tx_hash in tx_hashes or []:
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        if receipt_has_erc20_transfer(receipt):
            selected.append(tx_hash)
            if max_tx > 0 and len(selected) >= max_tx:
                break

    return selected
