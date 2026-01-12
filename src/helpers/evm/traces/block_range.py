def compute_latest_block_range(*, to_block: int, latest_blocks: int) -> tuple[int, int]:
    if not isinstance(to_block, int) or not isinstance(latest_blocks, int):
        raise TypeError("to_block and latest_blocks must be integers")
    if to_block < 0:
        raise ValueError("to_block must be non-negative")
    if latest_blocks <= 0:
        raise ValueError("latest_blocks must be a positive integer")

    from_block = max(0, to_block - latest_blocks + 1)
    return int(from_block), int(to_block)
