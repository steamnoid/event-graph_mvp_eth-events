from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
def test_task_enhance_data_returns_baselines_and_output(tmp_path: Path) -> None:
	from dag_helpers.enhance_data.task import enhance_data
	from dag_helpers.fetch_data.task import fetch_data

	# Produce an input events file via fetch task.
	input_events = tmp_path / "events.ndjson"
	pre_fetch, post_fetch = fetch_data(
		artifact_dir=tmp_path / "artifacts_fetch",
		out_events=input_events,
		out_rules=tmp_path / "rules.txt",
		format="ndjson",
		seed=20260114,
		entity_count=2,
		inconsistency_rate=0.0,
		missing_event_rate=0.0,
		run_id="fixture:enhance-task",
	)
	assert pre_fetch.exists() and post_fetch.exists() and input_events.exists()

	pre, post, out_events = enhance_data(
		artifact_dir=tmp_path / "artifacts_enhance",
		source_events=input_events,
		out_events=tmp_path / "enhanced.ndjson",
		format="ndjson",
	)

	assert pre.exists() and post.exists() and out_events.exists()
	# Baselines should stay identical (event_name is ignored by enhance baseline).
	assert pre.read_text(encoding="utf-8") == post.read_text(encoding="utf-8")
