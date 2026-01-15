from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.mark.integration
def test_store_edges_in_neo4j_then_validate_readback(tmp_path: Path) -> None:
	"""Requires a running Neo4j (e.g. via docker-compose).

	This test is intentionally environment-gated so it can live in the suite
	without breaking local/unit runs.
	"""
	if not os.getenv("NEO4J_URI"):
		pytest.skip("NEO4J_URI not set; start docker and set env to run")

	from dag_helpers.store_data.neo4j.task import store_edges_in_neo4j
	from dag_helpers.store_data.neo4j.canonical_baseline_helper import save_canonical_baseline_artifact
	from dag_helpers.validate_baseline.task import validate_canonical_baseline
	from dag_helpers.transform_data.events_to_graphs.transformer import write_graph_to_file

	graph_path = tmp_path / "graph.json"
	write_graph_to_file(
		graph={
			"graph_type": "neo4j_property_graph_batch",
			"run_id": "integration:neo4j",
			"nodes": [
				{"event_id": "a", "labels": ["Event"], "properties": {"event_name": "A", "name": "A"}},
				{"event_id": "b", "labels": ["Event"], "properties": {"event_name": "B", "name": "B"}},
				{"event_id": "c", "labels": ["Event"], "properties": {"event_name": "C", "name": "C"}},
			],
			"relationships": [
				{"from": "a", "to": "b", "type": "CAUSES", "properties": {"run_id": "integration:neo4j"}},
				{"from": "b", "to": "c", "type": "CAUSES", "properties": {"run_id": "integration:neo4j"}},
			],
		},
		path=graph_path,
	)

	# Reference baseline (equivalent to what fetch_data would provide for edges).
	ref_edges = save_canonical_baseline_artifact(
		edges=[{"from": "a", "to": "b"}, {"from": "b", "to": "c"}],
		path=tmp_path / "ref_edges_baseline.json",
	)

	candidate_baseline, _readback = store_edges_in_neo4j(
		artifact_dir=tmp_path / "artifacts_neo4j",
		source_graph=graph_path,
		run_id="integration:neo4j",
	)

	validated = validate_canonical_baseline(
		reference_baseline_path=ref_edges,
		candidate_baseline_path=candidate_baseline,
		artifact_dir=tmp_path / "artifacts_validate",
		out_name="C_neo4j.json",
	)
	assert validated.exists()
