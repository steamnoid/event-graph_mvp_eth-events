from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
def test_task_fetch_data_returns_two_baseline_paths(tmp_path: Path) -> None:
	from dag_helpers.fetch_data.task import fetch_data

	pre_path, post_path = fetch_data(
		artifact_dir=tmp_path / "artifacts",
		out_events=tmp_path / "events.ndjson",
		out_rules=tmp_path / "rules.txt",
		format="ndjson",
		seed=20260114,
		entity_count=2,
		inconsistency_rate=0.0,
		missing_event_rate=0.0,
		run_id="fixture:task",
	)

	assert pre_path.exists()
	assert post_path.exists()
	assert pre_path.read_text(encoding="utf-8") == post_path.read_text(encoding="utf-8")
