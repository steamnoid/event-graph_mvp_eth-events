import pytest


@pytest.mark.unit
@pytest.mark.parametrize("latest_blocks", [1, 5, 100])
def test_compute_latest_block_range_honors_latest_blocks(latest_blocks: int):
    from helpers.evm.traces.block_range import compute_latest_block_range

    from_block, to_block = compute_latest_block_range(to_block=10_000, latest_blocks=latest_blocks)

    assert (to_block - from_block + 1) == latest_blocks
