from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


@pytest.mark.unit
def test_build_edges_happy_path() -> None:
	from dag_helpers.transform_data.events_to_edges.transformer import build_edges

	events: list[dict[str, Any]] = [
		{"event_id": "A", "parent_event_ids": []},
		{"event_id": "B", "parent_event_ids": ["A"]},
		{"event_id": "C", "parent_event_ids": ["A", "B"]},
	]

	edges = build_edges(events)
	assert edges == [
		{"from": "A", "to": "B"},
		{"from": "A", "to": "C"},
		{"from": "B", "to": "C"},
	]


@pytest.mark.unit
def test_build_edges_rejects_self_parent() -> None:
	from dag_helpers.transform_data.events_to_edges.transformer import build_edges

	with pytest.raises(ValueError, match="self-parent"):
		build_edges([{"event_id": "A", "parent_event_ids": ["A"]}])


@pytest.mark.unit
def test_build_edges_rejects_missing_parent() -> None:
	from dag_helpers.transform_data.events_to_edges.transformer import build_edges

	with pytest.raises(ValueError, match="parent_event_id not found"):
		build_edges([{"event_id": "B", "parent_event_ids": ["A"]}])


@pytest.mark.unit
def test_build_edges_requires_parent_ids_list() -> None:
	from dag_helpers.transform_data.events_to_edges.transformer import build_edges

	with pytest.raises(ValueError, match="parent_event_ids must be a list"):
		build_edges([{"event_id": "B", "parent_event_ids": "A"}])


@pytest.mark.unit
def test_transform_edges_to_canonical_baseline_format_sorts_and_normalizes() -> None:
	from dag_helpers.transform_data.events_to_edges.transformer import (
		transform_edges_to_canonical_baseline_format,
	)

	edges = [
		{"from": 2, "to": 1, "extra": "ignored"},
		{"to": "b", "from": "a"},
	]

	baseline = transform_edges_to_canonical_baseline_format(edges)
	assert baseline == [
		{"from": "2", "to": "1"},
		{"from": "a", "to": "b"},
	]


@pytest.mark.unit
def test_save_edge_baseline_artifacts_write_canonical_json(tmp_path: Path) -> None:
	from dag_helpers.transform_data.events_to_edges.transformer import (
		save_post_transformation_canonical_baseline_artifact,
		save_pre_transformation_canonical_baseline_artifact,
	)

	edges = [{"from": "B", "to": "C"}, {"from": "A", "to": "B"}]

	pre_path = save_pre_transformation_canonical_baseline_artifact(
		fixture_data=edges,
		path=tmp_path / "pre_edges.baseline.json",
	)
	post_path = save_post_transformation_canonical_baseline_artifact(
		edges=edges,
		path=tmp_path / "post_edges.baseline.json",
	)

	assert pre_path.exists() and post_path.exists()
	pre = json.loads(pre_path.read_text(encoding="utf-8"))
	post = json.loads(post_path.read_text(encoding="utf-8"))
	assert pre == post == [{"from": "A", "to": "B"}, {"from": "B", "to": "C"}]


@pytest.mark.unit
def test_read_events_from_file_reads_ndjson(tmp_path: Path) -> None:
	from dag_helpers.transform_data.events_to_edges.transformer import read_events_from_file

	path = tmp_path / "events.ndjson"
	path.write_text('{"event_id": "A"}\n{"event_id": "B"}\n', encoding="utf-8")

	events = read_events_from_file(path)
	assert events == [{"event_id": "A"}, {"event_id": "B"}]
