import json
import pytest

@pytest.mark.unit
def test_write_events_to_file_writes_json_and_returns_count(tmp_path):
    from dags.helpers.eth.logs.transformer import write_events_to_file

    events = [{"event_id": "0xabc:1", "event_name": "Transfer"}]
    out_file = tmp_path / "events.json"

    written = write_events_to_file(events, str(out_file))

    assert written == 1
    assert json.loads(out_file.read_text(encoding="utf-8")) == events
