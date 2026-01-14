import pytest


@pytest.mark.unit
def test_build_edges_creates_edges_from_declared_parents():
	from helpers.enrollment.graph import build_edges

	events = [
		{
			"event_id": "e1",
			"event_type": "CourseEnrollmentRequested",
			"event_kind": "fact",
			"parent_event_ids": [],
			"layer": "L0",
			"entity_id": "enrollment-1",
			"payload": {},
			"emitted_at": "2026-01-01T00:00:00+00:00",
		},
		{
			"event_id": "e2",
			"event_type": "PaymentProcessingRequested",
			"event_kind": "fact",
			"parent_event_ids": ["e1"],
			"layer": "L1",
			"entity_id": "enrollment-1",
			"payload": {},
			"emitted_at": "2026-01-01T00:00:01+00:00",
		},
		{
			"event_id": "e3",
			"event_type": "AccessGranted",
			"event_kind": "decision",
			"parent_event_ids": ["e1", "e2"],
			"layer": "L6",
			"entity_id": "enrollment-1",
			"payload": {},
			"emitted_at": "2026-01-01T00:00:02+00:00",
		},
	]

	edges = build_edges(events)
	assert {tuple(sorted(e.items())) for e in edges} == {
		tuple(sorted({"from": "e1", "to": "e2"}.items())),
		tuple(sorted({"from": "e1", "to": "e3"}.items())),
		tuple(sorted({"from": "e2", "to": "e3"}.items())),
	}


@pytest.mark.unit
def test_build_edges_raises_when_parent_missing():
	from helpers.enrollment.graph import build_edges

	with pytest.raises(ValueError, match="parent_event_id not found"):
		build_edges(
			[
				{
					"event_id": "e2",
					"event_type": "PaymentProcessingRequested",
					"event_kind": "fact",
					"parent_event_ids": ["missing"],
					"layer": "L1",
					"entity_id": "enrollment-1",
					"payload": {},
					"emitted_at": "2026-01-01T00:00:01+00:00",
				}
			]
		)
