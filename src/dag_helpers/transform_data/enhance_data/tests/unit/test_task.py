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

	baseline, out_events = enhance_data(
		artifact_dir=tmp_path / "artifacts_enhance",
		source_events=input_events,
		out_events=tmp_path / "enhanced.ndjson",
		format="ndjson",
	)

	assert baseline.exists() and out_events.exists()
