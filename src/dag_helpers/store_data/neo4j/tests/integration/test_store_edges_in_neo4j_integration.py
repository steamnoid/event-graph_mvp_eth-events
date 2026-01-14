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
	from dag_helpers.store_data.neo4j.adapter import read_edges_from_file
	from dag_helpers.validate_baseline.task import validate_canonical_baseline

	edges_path = tmp_path / "edges.json"
	edges_path.write_text('[{"from": "a", "to": "b"}, {"from": "b", "to": "c"}]\n', encoding="utf-8")

	# Reference baseline (equivalent to what fetch_data would provide for edges).
	ref_edges = save_canonical_baseline_artifact(
		edges=read_edges_from_file(edges_path),
		path=tmp_path / "ref_edges_baseline.json",
	)

	candidate_baseline, _readback = store_edges_in_neo4j(
		artifact_dir=tmp_path / "artifacts_neo4j",
		source_edges=edges_path,
		run_id="integration:neo4j",
	)

	validated = validate_canonical_baseline(
		reference_baseline_path=ref_edges,
		candidate_baseline_path=candidate_baseline,
		artifact_dir=tmp_path / "artifacts_validate",
		out_name="C_neo4j.json",
	)
	assert validated.exists()
