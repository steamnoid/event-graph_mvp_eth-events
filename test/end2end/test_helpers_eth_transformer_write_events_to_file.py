import json
import pytest


@pytest.mark.e2e
def test_transform_logs_can_be_written_to_file(tmp_path):
    from dags.helpers.eth.adapter import fetch_logs
    from dags.helpers.eth.logs.transformer import transform_logs, write_events_to_file

    raw_logs = fetch_logs()
    transformed = transform_logs(raw_logs)

    out_file = tmp_path / "events.json"
    written = write_events_to_file(transformed, str(out_file))

    assert written == len(transformed)
    assert len(json.loads(out_file.read_text(encoding="utf-8"))) == len(transformed)
