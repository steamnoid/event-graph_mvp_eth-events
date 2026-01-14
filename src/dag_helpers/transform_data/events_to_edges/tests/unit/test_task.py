from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.mark.unit
def test_task_events_to_edges_emits_baselines_and_edges(tmp_path: Path) -> None:
	from dag_helpers.transform_data.events_to_edges.task import events_to_edges

	# Minimal NDJSON events input (no dependency on fetch_data).
	input_events = tmp_path / "events.ndjson"
	events = [
		{"event_id": "a", "entity_id": "ent1", "layer": "L1", "event_type": "A", "parent_event_ids": []},
		{"event_id": "b", "entity_id": "ent1", "layer": "L2", "event_type": "B", "parent_event_ids": ["a"]},
		{"event_id": "c", "entity_id": "ent1", "layer": "L3", "event_type": "C", "parent_event_ids": ["a", "b"]},
	]
	input_events.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")

	pre, post, edges_path = events_to_edges(
		artifact_dir=tmp_path / "artifacts_edges",
		source_events=input_events,
		out_edges=tmp_path / "edges.json",
	)

	assert pre.exists() and post.exists()
	assert edges_path.exists()
	# This stage validates edge canonical baselines (pre: in-memory, post: re-read from disk).
	assert pre.read_text(encoding="utf-8") == post.read_text(encoding="utf-8")
	# Edges are written as a JSON array.
	assert edges_path.read_text(encoding="utf-8").lstrip().startswith("[")
