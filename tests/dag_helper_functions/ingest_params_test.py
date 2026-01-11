"""Unit tests: deterministic parsing/validation of ingest params.

These helpers are used by the Airflow DAG to avoid complex logic inside tasks.
"""

import pytest

from dags.dag_helper_functions.ingest_params import parse_logs_number_ingest_params


def test_parse_logs_number_ingest_params_accepts_max_scan_blocks():
    """Given max_scan_blocks, it is propagated into the returned params."""
    conf = {"logs_number": 1, "chunk_size_blocks": 10, "max_scan_blocks": 123}
    params = parse_logs_number_ingest_params(conf=conf, to_block=1000)

    assert params["max_scan_blocks"] == 123
