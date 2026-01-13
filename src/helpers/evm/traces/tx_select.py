def select_tx_hashes_for_run(tx_hashes: list[str], *, max_tx: int | None) -> list[str]:
    if max_tx is None or max_tx <= 0:
        return list(tx_hashes or [])
    return list((tx_hashes or [])[:max_tx])


def select_tx_hashes_for_block(tx_hashes: list[str], *, max_tx_per_block: int | None) -> list[str]:
    if max_tx_per_block is None or max_tx_per_block <= 0:
        return list(tx_hashes or [])
    return list((tx_hashes or [])[:max_tx_per_block])


def select_tx_hashes_for_block_range_with_erc20_transfer_receipts(
    *,
    provider_url: str,
    from_block: int,
    to_block: int,
    max_tx_per_block: int | None,
) -> list[str]:
    from helpers.evm.traces.block_adapter import fetch_tx_hashes_from_block
    from helpers.evm.traces.receipt_adapter import select_tx_hashes_with_erc20_transfer_receipts

    tx_hashes: list[str] = []
    for block_number in range(int(from_block), int(to_block) + 1):
        per_block = fetch_tx_hashes_from_block(provider_url=provider_url, block_number=int(block_number))
        tx_hashes.extend(
            select_tx_hashes_with_erc20_transfer_receipts(
                provider_url=provider_url,
                tx_hashes=per_block,
                max_tx=max_tx_per_block,
            )
        )
    return tx_hashes


def select_tx_hashes_for_recent_blocks_with_erc20_transfer_receipts_until_min(
    *,
    provider_url: str,
    to_block: int,
    max_blocks_to_scan: int,
    min_total_tx: int,
    max_tx_per_block: int,
) -> list[str]:
    from helpers.evm.traces.block_adapter import fetch_tx_hashes_from_block
    from helpers.evm.traces.receipt_adapter import select_tx_hashes_with_erc20_transfer_receipts

    selected: list[str] = []
    for i in range(int(max_blocks_to_scan)):
        block_number = int(to_block) - i
        if block_number < 0:
            break

        per_block = fetch_tx_hashes_from_block(provider_url=provider_url, block_number=block_number)
        selected.extend(
            select_tx_hashes_with_erc20_transfer_receipts(
                provider_url=provider_url,
                tx_hashes=per_block,
                max_tx=int(max_tx_per_block),
            )
        )

        if int(min_total_tx) > 0 and len(selected) >= int(min_total_tx):
            break

    return selected
