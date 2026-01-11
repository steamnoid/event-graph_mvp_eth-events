"""Ingest parameter parsing/validation for the Airflow DAG.

Keep DAG tasks thin: put all conf parsing/validation here.
"""

from typing import Dict


def parse_logs_number_ingest_params(conf: Dict, to_block: int) -> Dict:
    """Parse `logs_number` mode params from a dag_run.conf-like dict.

    Returns a dict suitable for XCom (JSON-serializable).
    """
    if not isinstance(to_block, int):
        raise TypeError("to_block must be an integer")
    if to_block < 0:
        raise ValueError("to_block must be non-negative")

    if conf.get("logs_number") is None:
        raise ValueError("logs_number is required")

    logs_number_int = int(conf.get("logs_number"))
    if logs_number_int <= 0:
        raise ValueError("logs_number must be a positive integer")

    chunk_size_blocks = int(conf.get("chunk_size_blocks", 10_000))
    if chunk_size_blocks <= 0:
        raise ValueError("chunk_size_blocks must be a positive integer")

    max_scan_blocks = conf.get("max_scan_blocks")
    if max_scan_blocks is not None:
        max_scan_blocks = int(max_scan_blocks)
        if max_scan_blocks <= 0:
            raise ValueError("max_scan_blocks must be a positive integer")

    return {
        "mode": "logs_number",
        "to_block": int(to_block),
        "logs_number": logs_number_int,
        "chunk_size_blocks": chunk_size_blocks,
        "max_scan_blocks": max_scan_blocks,
    }
