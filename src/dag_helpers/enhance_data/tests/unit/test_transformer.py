from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest


@pytest.mark.unit
def test_enhance_events_add_event_name_adds_missing() -> None:
	from dag_helpers.enhance_data.transformer import enhance_events_add_event_name

	events: list[dict[str, Any]] = [
		{"event_type": "PaymentConfirmed", "payload": {}},
		{"event_type": "AccessGranted", "event_name": "CustomName", "payload": {}},
		{"payload": {}},
	]

	out = enhance_events_add_event_name(events)

	# Does not mutate inputs.
	assert "event_name" not in events[0]

	assert out[0]["event_name"] == "Payment Confirmed"
	assert out[1]["event_name"] == "CustomName"
	assert "event_name" not in out[2]


@pytest.mark.unit
def test_enhance_events_add_event_name_handles_empty_event_name() -> None:
	from dag_helpers.enhance_data.transformer import enhance_events_add_event_name

	events = [{"event_type": "EnrollmentCompleted", "event_name": ""}]
	out = enhance_events_add_event_name(events)
	assert out[0]["event_name"] == "Enrollment Completed"


@pytest.mark.unit
def test_event_type_to_event_name_handles_acronyms() -> None:
	from dag_helpers.enhance_data.transformer import enhance_events_add_event_name

	out = enhance_events_add_event_name([{"event_type": "HTTPServerStarted"}])
	assert out[0]["event_name"] == "HTTP Server Started"


@pytest.mark.unit
def test_canonical_baseline_artifacts_identical(tmp_path: Path) -> None:
	from dag_helpers.enhance_data.transformer import (
		enhance_events_add_event_name,
		save_post_transformation_canonical_baseline_artifact,
		save_pre_transformation_canonical_baseline_artifact,
	)

	from data_generator.generator import (
		generate_causality_rules_text,
		materialize_events_from_causality_rules_text,
	)

	rules_text = generate_causality_rules_text(
		seed=123,
		entity_count=3,
		inconsistency_rate=0.0,
		missing_event_rate=0.0,
		run_id="baseline-identical",
	)
	original_events = materialize_events_from_causality_rules_text(rules_text=rules_text, seed=123)
	enhanced_events = enhance_events_add_event_name(original_events)

	pre_path = save_pre_transformation_canonical_baseline_artifact(
		fixture_data=original_events,
		path=tmp_path / "pre_enhance.baseline.json",
	)
	post_path = save_post_transformation_canonical_baseline_artifact(
		events=enhanced_events,
		path=tmp_path / "post_enhance.baseline.json",
	)

	assert pre_path.read_text(encoding="utf-8") == post_path.read_text(encoding="utf-8")
