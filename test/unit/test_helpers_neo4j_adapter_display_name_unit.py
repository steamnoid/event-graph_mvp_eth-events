import pytest


@pytest.mark.unit
def test_event_display_name_prefers_ordered_name_when_available():
    from helpers.neo4j.adapter import event_display_name

    assert event_display_name({"event_name": "CALL", "event_order": "1.1"}) == "1.1 CALL"


@pytest.mark.unit
def test_event_display_name_falls_back_to_event_name():
    from helpers.neo4j.adapter import event_display_name

    assert event_display_name({"event_name": "Transfer"}) == "Transfer"
