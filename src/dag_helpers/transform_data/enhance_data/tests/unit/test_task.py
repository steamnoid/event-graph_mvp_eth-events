from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.mark.unit
def test_task_enhance_data_returns_baselines_and_output(tmp_path: Path) -> None:
	from dag_helpers.transform_data.enhance_data.task import enhance_data

	# Minimal NDJSON events input (no dependency on fetch_data).
	input_events = tmp_path / "events.ndjson"
	events = [
		{
			"event_id": "e1",
			"entity_id": "ent1",
			"layer": "L1",
			"event_type": "PaymentConfirmed",
			"parent_event_ids": [],
		},
		{
			"event_id": "e2",
			"entity_id": "ent1",
			"layer": "L2",
			"event_type": "ReceiptIssued",
			"parent_event_ids": ["e1"],
		},
	]
	input_events.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")

	pre, post, out_events = enhance_data(
		artifact_dir=tmp_path / "artifacts_enhance",
		source_events=input_events,
		out_events=tmp_path / "enhanced.ndjson",
		format="ndjson",
	)

	assert pre.exists() and post.exists() and out_events.exists()
	# Baselines should stay identical (event_name is ignored by enhance baseline).
	assert pre.read_text(encoding="utf-8") == post.read_text(encoding="utf-8")
