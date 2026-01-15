from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.integration
def test_fetch_validate_enhance_validate_pipeline(tmp_path: Path) -> None:
	from dag_helpers.fetch_data.task import fetch_data
	from dag_helpers.transform_data.enhance_data.task import enhance_data
	from dag_helpers.transform_data.events_to_graphs.task import events_to_graphs
	from dag_helpers.validate_baseline.task import validate_canonical_baseline

	# Stage 1: fetch/generate data + baselines
	events_path = tmp_path / "events.ndjson"
	rules_path = tmp_path / "rules.txt"
	fetch_artifacts_dir = tmp_path / "artifacts_fetch"

	ref_events_baseline, ref_edges_baseline = fetch_data(
		artifact_dir=fetch_artifacts_dir,
		out_events=events_path,
		out_rules=rules_path,
		format="ndjson",
		seed=20260114,
		entity_count=2,
		inconsistency_rate=0.0,
		missing_event_rate=0.0,
		run_id="integration:fetch",
	)
	assert ref_events_baseline.exists() and ref_edges_baseline.exists()

	# Stage 1 validation (reference baseline is self-consistent)
	validated_c1 = validate_canonical_baseline(
		reference_baseline_path=ref_events_baseline,
		candidate_baseline_path=ref_events_baseline,
		artifact_dir=tmp_path / "artifacts_validate_c1",
		out_name="C1.json",
	)
	assert validated_c1.exists()

	# Stage 2: enhance data (event_name) + baselines
	enhance_artifacts_dir = tmp_path / "artifacts_enhance"
	enhanced_events_path = tmp_path / "enhanced_events.ndjson"

	enhance_baseline, _enhanced_out = enhance_data(
		artifact_dir=enhance_artifacts_dir,
		source_events=events_path,
		out_events=enhanced_events_path,
		format="ndjson",
	)
	assert enhance_baseline.exists() and enhanced_events_path.exists()

	# Stage 2 validation (candidate vs fetch reference)
	validated_c2 = validate_canonical_baseline(
		reference_baseline_path=ref_events_baseline,
		candidate_baseline_path=enhance_baseline,
		artifact_dir=tmp_path / "artifacts_validate_c2",
		out_name="C2.json",
	)
	assert validated_c2.exists()

	# Stage 3: transform events -> graph batch + edges baselines
	graphs_artifacts_dir = tmp_path / "artifacts_graphs"
	graph_path = tmp_path / "graph.json"

	edges_baseline, out_graph = events_to_graphs(
		artifact_dir=graphs_artifacts_dir,
		source_events=enhanced_events_path,
		out_graph=graph_path,
		run_id="integration:graphs",
	)
	assert edges_baseline.exists() and out_graph.exists()

	# Stage 3 validation (candidate vs fetch reference)
	validated_c3 = validate_canonical_baseline(
		reference_baseline_path=ref_edges_baseline,
		candidate_baseline_path=edges_baseline,
		artifact_dir=tmp_path / "artifacts_validate_c3",
		out_name="C3.json",
	)
	assert validated_c3.exists()

	# Sanity: validated baselines are stable handoff artifacts
	assert validated_c1.read_text(encoding="utf-8") == ref_events_baseline.read_text(encoding="utf-8")
	assert validated_c2.read_text(encoding="utf-8") == enhance_baseline.read_text(encoding="utf-8")
	assert validated_c3.read_text(encoding="utf-8") == edges_baseline.read_text(encoding="utf-8")
