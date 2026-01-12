import pytest

@pytest.mark.e2e
def test_load_events_from_file_loads_transformed_events_from_disk(tmp_path):
    from dags.helpers.eth.adapter import fetch_logs, write_logs_to_file
    from dags.helpers.eth.logs.transformer import load_logs_from_file, transform_logs, write_events_to_file
    from dags.helpers.neo4j.transformer import load_events_from_file

    raw_logs = fetch_logs()
    logs_file = tmp_path / "logs.json"
    write_logs_to_file(raw_logs, str(logs_file))

    loaded_logs = load_logs_from_file(str(logs_file))
    transformed = transform_logs(loaded_logs)

    events_file = tmp_path / "events.json"
    written = write_events_to_file(transformed, str(events_file))

    loaded_events = load_events_from_file(str(events_file))

    assert written == len(loaded_events)