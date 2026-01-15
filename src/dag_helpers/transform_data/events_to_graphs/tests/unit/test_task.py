from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.mark.unit
def test_task_events_to_graphs_emits_edges_baseline_and_graph_file(tmp_path: Path) -> None:
	from dag_helpers.transform_data.events_to_graphs.task import events_to_graphs

	input_events = tmp_path / "events.ndjson"
	events = [
		{"event_id": "a", "event_type": "A", "event_name": "A", "parent_event_ids": []},
		{"event_id": "b", "event_type": "B", "event_name": "B", "parent_event_ids": ["a"]},
		{"event_id": "c", "event_type": "C", "event_name": "C", "parent_event_ids": ["a", "b"]},
	]
	input_events.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")

	baseline, graph_path = events_to_graphs(
		artifact_dir=tmp_path / "artifacts_graphs",
		source_events=input_events,
		out_graph=tmp_path / "graph.json",
		run_id="r1",
	)

	assert baseline.exists()
	assert graph_path.exists()
	assert graph_path.read_text(encoding="utf-8").lstrip().startswith("{")
