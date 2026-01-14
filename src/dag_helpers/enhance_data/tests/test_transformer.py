from __future__ import annotations

from typing import Any

import pytest


@pytest.mark.unit
def test_enhance_events_add_event_name_adds_missing() -> None:
	from dag_helpers.enhance_data.transformer import enhance_events_add_event_name

	events: list[dict[str, Any]] = [
		{"event_type": "PaymentConfirmed", "payload": {}},
		{"event_type": "AccessGranted", "event_name": "CustomName", "payload": {}},
		{"payload": {}},
	]

	out = enhance_events_add_event_name(events)

	# Does not mutate inputs.
	assert "event_name" not in events[0]

	assert out[0]["event_name"] == "Payment Confirmed"
	assert out[1]["event_name"] == "CustomName"
	assert "event_name" not in out[2]


@pytest.mark.unit
def test_enhance_events_add_event_name_handles_empty_event_name() -> None:
	from dag_helpers.enhance_data.transformer import enhance_events_add_event_name

	events = [{"event_type": "EnrollmentCompleted", "event_name": ""}]
	out = enhance_events_add_event_name(events)
	assert out[0]["event_name"] == "Enrollment Completed"


@pytest.mark.unit
def test_event_type_to_event_name_handles_acronyms() -> None:
	from dag_helpers.enhance_data.transformer import enhance_events_add_event_name

	out = enhance_events_add_event_name([{"event_type": "HTTPServerStarted"}])
	assert out[0]["event_name"] == "HTTP Server Started"
