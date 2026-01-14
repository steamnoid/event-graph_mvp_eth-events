from pathlib import Path
import pytest


_FIXTURE_EVENTS_FILE = (
	Path(__file__).resolve().parents[1]
	/ "fixtures"
	/ "events"
	/ "enrollment_events.json"
)


@pytest.mark.behavior
def test_enrollment_fixture_to_graph_has_valid_edges_and_schema(tmp_path):
	import json

	from helpers.enrollment.adapter import load_events_from_file, write_events_to_file
	from helpers.enrollment.graph import build_edges, write_graph_to_file
	from helpers.enrollment.transformer import normalize_events
	from event_fixture_validator import validate_events

	raw_events = load_events_from_file(str(_FIXTURE_EVENTS_FILE))
	assert len(raw_events) > 0

	# The committed fixture is expected to be consistent.
	result = validate_events(raw_events, mode="consistent")
	assert result.is_valid, result.errors

	normalized = normalize_events(raw_events)
	events_file = tmp_path / "events.json"
	write_events_to_file(normalized, str(events_file))

	loaded = load_events_from_file(str(events_file))
	edges = build_edges(loaded)

	run_id = "behavior:enrollment-fixture"
	graph_file = tmp_path / "graph.json"
	write_graph_to_file(events=loaded, edges=edges, run_id=run_id, filename=str(graph_file))

	graph = json.loads(graph_file.read_text(encoding="utf-8"))
	assert set(graph.keys()) == {"run_id", "events", "edges"}
	assert graph["run_id"] == run_id
	assert len(graph["events"]) == len(loaded)
	assert len(graph["edges"]) > 0

	by_id = {e.get("event_id"): e for e in graph["events"] if e.get("event_id")}
	assert len(by_id) == len(loaded)

	for edge in graph["edges"]:
		assert set(edge.keys()) == {"from", "to"}
		assert edge["from"] in by_id
		assert edge["to"] in by_id

		parent = by_id[edge["from"]]
		child = by_id[edge["to"]]
		assert parent["entity_id"] == child["entity_id"]
		assert edge["from"] in child["parent_event_ids"]
