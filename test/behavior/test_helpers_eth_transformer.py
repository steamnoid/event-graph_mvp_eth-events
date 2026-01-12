import json
from pathlib import Path
import pytest

_FIXTURE_LOGS_FILE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "eth_logs"
    / "fetch_logs_uniswap_v2_weth_usdc.json"
)


@pytest.mark.behavior
def test_transform_logs_decodes_all_fixture_logs():
    from dags.helpers.eth.logs.transformer import load_logs_from_file, transform_logs

    raw_logs = load_logs_from_file(str(_FIXTURE_LOGS_FILE))
    transformed = transform_logs(raw_logs)

    assert len(transformed) == len(raw_logs)
    assert all(
        item.get("event_name")
        and not str(item.get("event_name")).startswith("Unknown(")
        and item.get("decoded") is not None
        for item in transformed
    )


@pytest.mark.behavior
def test_logs_written_and_loaded_from_file_are_kosher(tmp_path):
    from dags.helpers.eth.adapter import write_logs_to_file
    from dags.helpers.eth.logs.transformer import load_logs_from_file, transform_logs

    raw_logs = load_logs_from_file(str(_FIXTURE_LOGS_FILE))
    out_file = tmp_path / "eth_logs.json"

    write_logs_to_file(raw_logs, str(out_file))
    loaded_logs = load_logs_from_file(str(out_file))

    assert len(loaded_logs) == len(raw_logs)

    transformed = transform_logs(loaded_logs)
    assert all(
        item.get("event_name")
        and not str(item.get("event_name")).startswith("Unknown(")
        and item.get("decoded") is not None
        for item in transformed
    )

@pytest.mark.behavior
def test_transform_logs_can_be_written_to_file(tmp_path):
    from dags.helpers.eth.logs.transformer import load_logs_from_file, transform_logs, write_events_to_file

    raw_logs = load_logs_from_file(str(_FIXTURE_LOGS_FILE))
    transformed = transform_logs(raw_logs)

    out_file = tmp_path / "events.json"
    written = write_events_to_file(transformed, str(out_file))

    assert written == len(transformed)
    assert len(json.loads(out_file.read_text(encoding="utf-8"))) == len(transformed)
