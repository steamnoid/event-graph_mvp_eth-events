from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.integration
def test_fetch_validate_enhance_validate_pipeline(tmp_path: Path) -> None:
	from dag_helpers.fetch_data.task import fetch_data
	from dag_helpers.transform_data.enhance_data.task import enhance_data
	from dag_helpers.transform_data.events_to_edges.task import events_to_edges
	from dag_helpers.validate_baseline.task import validate_canonical_baseline

	# Stage 1: fetch/generate data + baselines
	events_path = tmp_path / "events.ndjson"
	rules_path = tmp_path / "rules.txt"
	fetch_artifacts_dir = tmp_path / "artifacts_fetch"

	pre_fetch, post_fetch = fetch_data(
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
	assert pre_fetch.exists() and post_fetch.exists()

	# Stage 1 validation (between tasks)
	validated_c1 = validate_canonical_baseline(
		pre_baseline_path=pre_fetch,
		post_baseline_path=post_fetch,
		artifact_dir=tmp_path / "artifacts_validate_c1",
		out_name="C1.json",
	)
	assert validated_c1.exists()

	# Stage 2: enhance data (event_name) + baselines
	enhance_artifacts_dir = tmp_path / "artifacts_enhance"
	enhanced_events_path = tmp_path / "enhanced_events.ndjson"

	pre_enhance, post_enhance, _ = enhance_data(
		artifact_dir=enhance_artifacts_dir,
		source_events=events_path,
		out_events=enhanced_events_path,
		format="ndjson",
	)
	assert pre_enhance.exists() and post_enhance.exists() and enhanced_events_path.exists()

	# Stage 2 validation (between tasks)
	validated_c2 = validate_canonical_baseline(
		pre_baseline_path=pre_enhance,
		post_baseline_path=post_enhance,
		artifact_dir=tmp_path / "artifacts_validate_c2",
		out_name="C2.json",
	)
	assert validated_c2.exists()

	# Stage 3: transform events -> edges + baselines
	edges_artifacts_dir = tmp_path / "artifacts_edges"
	edges_path = tmp_path / "edges.json"

	pre_edges, post_edges, out_edges = events_to_edges(
		artifact_dir=edges_artifacts_dir,
		source_events=enhanced_events_path,
		out_edges=edges_path,
	)
	assert pre_edges.exists() and post_edges.exists() and out_edges.exists()

	# Stage 3 validation (between tasks)
	validated_c3 = validate_canonical_baseline(
		pre_baseline_path=pre_edges,
		post_baseline_path=post_edges,
		artifact_dir=tmp_path / "artifacts_validate_c3",
		out_name="C3.json",
	)
	assert validated_c3.exists()

	# Sanity: validated baselines are stable handoff artifacts
	assert validated_c1.read_text(encoding="utf-8") == pre_fetch.read_text(encoding="utf-8")
	assert validated_c2.read_text(encoding="utf-8") == pre_enhance.read_text(encoding="utf-8")
	assert validated_c3.read_text(encoding="utf-8") == pre_edges.read_text(encoding="utf-8")
