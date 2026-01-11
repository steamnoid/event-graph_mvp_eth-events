"""Unit test: compute an inclusive block range from latest_blocks.

This is pure logic (no Web3/network), so it stays deterministic.
"""

import pytest

from dags.dag_helper_functions.events import compute_latest_block_range


@pytest.mark.parametrize("latest_blocks", [1, 5, 100])
def test_compute_latest_block_range_honors_latest_blocks(latest_blocks: int):
    """Given a chain tip and latest_blocks, the inclusive range covers that many blocks."""
    from_block, to_block = compute_latest_block_range(to_block=10_000, latest_blocks=latest_blocks)
    assert (to_block - from_block + 1) == latest_blocks
