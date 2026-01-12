import pytest
import json
from dags.helpers.eth.adapter import *

@pytest.mark.e2e
def test_chain_id_is_1():
    assert fetch_chain_id() == 1, "Chain ID is not 1 for Ethereum Mainnet"

@pytest.mark.e2e
def test_latest_block_number_is_large():
    assert fetch_latest_block_number() > 17000000, "Latest block number is unexpectedly low"

@pytest.mark.e2e
def test_logs_are_not_empty():
    logs = fetch_logs()
    assert logs, "Logs should not be empty"

@pytest.mark.e2e
def test_logs_count_at_least_100():
    logs = fetch_logs()
    assert len(logs) >= 100, "Expected at least 100 logs"


@pytest.mark.e2e
def test_write_logs_to_file_writes_json(tmp_path):
    logs = fetch_logs()
    out_file = tmp_path / "eth_logs.json"

    written = write_logs_to_file(logs, str(out_file))
    payload = json.loads(out_file.read_text(encoding="utf-8"))

    assert written == len(payload)