from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
def test_transform_edges_to_canonical_baseline_format_sorts_and_coerces() -> None:
	from dag_helpers.store_data.neo4j.canonical_baseline_helper import transform_edges_to_canonical_baseline_format

	edges = [
		{"from": 2, "to": "b"},
		{"from": "1", "to": "a"},
		{"from": "1", "to": "b"},
	]
	baseline = transform_edges_to_canonical_baseline_format(edges)
	assert baseline == [
		{"from": "1", "to": "a"},
		{"from": "1", "to": "b"},
		{"from": "2", "to": "b"},
	]


@pytest.mark.unit
def test_read_edges_from_file_roundtrips_json_array(tmp_path: Path) -> None:
	from dag_helpers.store_data.neo4j.adapter import read_edges_from_file, write_edges_to_file

	path = tmp_path / "edges.json"
	write_edges_to_file(edges=[{"from": "a", "to": "b"}], path=path)
	assert read_edges_from_file(path) == [{"from": "a", "to": "b"}]
