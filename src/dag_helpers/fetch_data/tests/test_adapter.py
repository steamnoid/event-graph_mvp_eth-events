from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
def test_adapter_generate_read_and_write_baseline(tmp_path: Path) -> None:
	from dag_helpers.fetch_data.adapter import (
		generate_fixture_files,
		read_fixture_file,
		transform_fixture_data_to_events,
	)
	from dag_helpers.fetch_data.canonical_baseline_helper import (
		save_canonical_baseline_artifact,
		transform_events_to_canonical_baseline_format,
	)

	out_events = tmp_path / "events.ndjson"
	out_rules = tmp_path / "rules.txt"
	out_baseline = tmp_path / "baseline.json"

	generated_path = generate_fixture_files(
		out_events=out_events,
		out_rules=out_rules,
		format="ndjson",
		seed=20260113,
		entity_count=2,
		inconsistency_rate=0.2,
		missing_event_rate=0.1,
		run_id="fixture:unit",
	)
	assert generated_path.exists()
	assert out_rules.exists()

	fixture_data = read_fixture_file(generated_path)
	assert fixture_data

	events = transform_fixture_data_to_events(fixture_data)
	baseline = transform_events_to_canonical_baseline_format(events)
	assert baseline

	baseline_path = save_canonical_baseline_artifact(events=events, path=out_baseline)
	assert baseline_path.exists()
	assert baseline_path.read_text(encoding="utf-8").lstrip().startswith("[")


@pytest.mark.unit
def test_canonical_baseline_artifacts_identical(tmp_path: Path) -> None:
	from dag_helpers.fetch_data.adapter import (
		generate_fixture_files,
		read_fixture_file,
		transform_fixture_data_to_events,
	)
	from dag_helpers.fetch_data.canonical_baseline_helper import save_canonical_baseline_artifact

	generated_path = generate_fixture_files(
		out_events=tmp_path / "events.json",
		out_rules=tmp_path / "rules.txt",
		format="json",
		seed=20260113,
		entity_count=2,
		inconsistency_rate=0.0,
		missing_event_rate=0.0,
		run_id="fixture:baseline-identical",
	)

	fixture_data = read_fixture_file(generated_path)
	events_1 = transform_fixture_data_to_events(fixture_data)
	events_2 = transform_fixture_data_to_events(fixture_data)

	pre_path = save_canonical_baseline_artifact(events=events_1, path=tmp_path / "baseline_1.json")
	post_path = save_canonical_baseline_artifact(events=events_2, path=tmp_path / "baseline_2.json")

	assert pre_path.read_text(encoding="utf-8") == post_path.read_text(encoding="utf-8")
