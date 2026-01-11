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
