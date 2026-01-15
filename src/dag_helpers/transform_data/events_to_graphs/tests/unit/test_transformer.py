from __future__ import annotations

from typing import Any

import pytest


@pytest.mark.unit
def test_build_graph_batch_includes_nodes_and_relationships() -> None:
	from dag_helpers.transform_data.events_to_graphs.transformer import build_graph_batch

	events: list[dict[str, Any]] = [
		{"event_id": "A", "event_type": "A", "event_name": "A", "parent_event_ids": []},
		{"event_id": "B", "event_type": "B", "event_name": "B", "parent_event_ids": ["A"]},
		{"event_id": "C", "event_type": "C", "event_name": "C", "parent_event_ids": ["A", "B"]},
	]

	graph = build_graph_batch(events=events, run_id="r1")
	assert graph["graph_type"] == "neo4j_property_graph_batch"
	assert graph["run_id"] == "r1"
	assert len(graph["nodes"]) == 3
	assert len(graph["relationships"]) == 3

	# Neo4j Browser caption convention.
	by_id = {n["event_id"]: n for n in graph["nodes"]}
	assert by_id["A"]["properties"]["name"] == "A"


@pytest.mark.unit
def test_edges_from_graph_roundtrips_to_edge_list() -> None:
	from dag_helpers.transform_data.events_to_graphs.transformer import build_graph_batch, edges_from_graph

	events: list[dict[str, Any]] = [
		{"event_id": "A", "parent_event_ids": []},
		{"event_id": "B", "parent_event_ids": ["A"]},
		{"event_id": "C", "parent_event_ids": ["A", "B"]},
	]
	graph = build_graph_batch(events=events, run_id="r1")
	edges = edges_from_graph(graph)
	assert edges == [
		{"from": "A", "to": "B"},
		{"from": "A", "to": "C"},
		{"from": "B", "to": "C"},
	]


@pytest.mark.unit
def test_event_names_by_id_from_graph_extracts_names() -> None:
	from dag_helpers.transform_data.events_to_graphs.transformer import build_graph_batch, event_names_by_id_from_graph

	events: list[dict[str, Any]] = [
		{"event_id": "A", "event_name": "Alpha", "parent_event_ids": []},
		{"event_id": "B", "event_name": "Beta", "parent_event_ids": ["A"]},
	]
	graph = build_graph_batch(events=events, run_id="r1")
	names = event_names_by_id_from_graph(graph)
	assert names == {"A": "Alpha", "B": "Beta"}
