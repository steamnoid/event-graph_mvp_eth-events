import json
import pytest

@pytest.mark.e2e
def test_transform_logs_decodes_all_fetched_logs():
    from dags.helpers.eth.adapter import fetch_logs
    from dags.helpers.eth.logs.transformer import transform_logs

    raw_logs = fetch_logs()
    transformed = transform_logs(raw_logs)

    assert len(transformed) == len(raw_logs)
    assert all(
        item.get("event_name")
        and not str(item.get("event_name")).startswith("Unknown(")
        and item.get("decoded") is not None
        for item in transformed
    )


@pytest.mark.e2e
def test_logs_written_and_loaded_from_file_are_kosher(tmp_path):
    from dags.helpers.eth.adapter import fetch_logs, write_logs_to_file
    from dags.helpers.eth.logs.transformer import load_logs_from_file, transform_logs

    raw_logs = fetch_logs()
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
