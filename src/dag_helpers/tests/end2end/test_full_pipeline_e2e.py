from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest


def _neo4j_is_reachable() -> bool:
	"""Best-effort reachability check so `-m e2e` fails less mysteriously."""
	try:
		from neo4j import GraphDatabase  # type: ignore
		from neo4j.exceptions import Neo4jError  # type: ignore
	except Exception:
		return False

	try:
		# Keep consistent with our defaults in `neo4j_config_from_env()`.
		driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "test"))
		try:
			with driver.session() as session:
				session.run("RETURN 1 AS ok").single()
		finally:
			driver.close()
		return True
	except (OSError, Neo4jError, Exception):
		return False


@pytest.mark.end2end
def test_full_pipeline_e2e_builds_6_entities_and_validates_every_stage(tmp_path: Path) -> None:
	"""End-to-end pipeline test.

	Requirements:
	- Builds >= 6 entities
	- inconsistency_rate=0.0 and missing_event_rate=0.0
	- Runs the entire pipeline including Neo4j write+readback
	"""
	if not _neo4j_is_reachable():
		pytest.skip("Neo4j not reachable on neo4j://localhost:7687 (start docker compose neo4j)")

	from dag_helpers.fetch_data.task import fetch_data
	from dag_helpers.store_data.neo4j.task import store_edges_in_neo4j
	from dag_helpers.transform_data.enhance_data.task import enhance_data
	from dag_helpers.transform_data.events_to_graphs.task import events_to_graphs
	from dag_helpers.validate_baseline.task import validate_canonical_baseline

	run_tag = uuid4().hex

	# Stage 1: fetch/generate data + baselines (reference for all downstream validation)
	ref_events_baseline, ref_edges_baseline = fetch_data(
		artifact_dir=tmp_path / "artifacts_fetch",
		out_events=tmp_path / "events.ndjson",
		out_rules=tmp_path / "rules.txt",
		format="ndjson",
		seed=20260114,
		entity_count=10,
		inconsistency_rate=0.0,
		missing_event_rate=0.0,
		run_id=f"e2e:fetch:{run_tag}",
	)
	assert ref_events_baseline.exists() and ref_edges_baseline.exists()

	# Stage 1 validation (reference baseline is self-consistent)
	validated_events_ref = validate_canonical_baseline(
		reference_baseline_path=ref_events_baseline,
		candidate_baseline_path=ref_events_baseline,
		artifact_dir=tmp_path / "artifacts_validate_ref_events",
		out_name="C_ref_events.json",
	)
	assert validated_events_ref.exists()

	# Stage 2: enhance events (adds event_name) + candidate baseline
	enhance_baseline, enhanced_events_path = enhance_data(
		artifact_dir=tmp_path / "artifacts_enhance",
		source_events=tmp_path / "events.ndjson",
		out_events=tmp_path / "enhanced_events.ndjson",
		format="ndjson",
	)
	assert enhance_baseline.exists() and enhanced_events_path.exists()

	validated_enhanced_events = validate_canonical_baseline(
		reference_baseline_path=ref_events_baseline,
		candidate_baseline_path=enhance_baseline,
		artifact_dir=tmp_path / "artifacts_validate_enhance",
		out_name="C_enhance_events.json",
	)
	assert validated_enhanced_events.exists()

	# Stage 3: transform events -> graph batch + candidate edges baseline
	edges_baseline, graph_path = events_to_graphs(
		artifact_dir=tmp_path / "artifacts_graphs",
		source_events=enhanced_events_path,
		out_graph=tmp_path / "graph.json",
		run_id=f"e2e:neo4j:{run_tag}",
	)
	assert edges_baseline.exists() and graph_path.exists()

	validated_edges = validate_canonical_baseline(
		reference_baseline_path=ref_edges_baseline,
		candidate_baseline_path=edges_baseline,
		artifact_dir=tmp_path / "artifacts_validate_edges",
		out_name="C_edges.json",
	)
	assert validated_edges.exists()

	# Stage 4: store edges in Neo4j, read back, canonicalize readback + candidate baseline
	neo4j_baseline, readback_edges_path = store_edges_in_neo4j(
		artifact_dir=tmp_path / "artifacts_neo4j",
		source_graph=graph_path,
		run_id=f"e2e:neo4j:{run_tag}",
		clear_run_first=True,
	)
	assert neo4j_baseline.exists() and readback_edges_path.exists()

	validated_neo4j_readback = validate_canonical_baseline(
		reference_baseline_path=ref_edges_baseline,
		candidate_baseline_path=neo4j_baseline,
		artifact_dir=tmp_path / "artifacts_validate_neo4j",
		out_name="C_neo4j_edges.json",
	)
	assert validated_neo4j_readback.exists()
