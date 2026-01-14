import json

import pytest


@pytest.mark.functional
def test_generator_emits_canonical_declarations_when_consistent(tmp_path):
	from data_generator.generator import (
		generate_events_batch,
		generate_events_file,
		generate_events_stream,
	)

	seed = 123
	entity_count = 2

	batch_events = generate_events_batch(
		seed=seed, entity_count=entity_count, inconsistency_rate=0.0
	)
	assert len(batch_events) > 0

	required_keys = {
		"event_id",
		"event_type",
		"event_kind",
		"parent_event_ids",
		"layer",
		"entity_id",
		"payload",
		"emitted_at",
	}
	assert all(required_keys.issubset(e.keys()) for e in batch_events)
	assert {e["event_kind"] for e in batch_events}.issuperset({"fact", "decision"})
	assert len({e["event_id"] for e in batch_events}) == len(batch_events)

	entity_ids = {e["entity_id"] for e in batch_events}
	assert entity_ids == {"enrollment-1", "enrollment-2"}

	by_key = {(e["entity_id"], e["event_type"]): e for e in batch_events}
	for entity_id in entity_ids:
		ag = by_key[(entity_id, "AccessGranted")]
		pc = by_key[(entity_id, "PaymentConfirmed")]
		ep = by_key[(entity_id, "EligibilityPassed")]
		cp = by_key[(entity_id, "ContentPrepared")]

		assert set(ag["parent_event_ids"]) == {pc["event_id"], ep["event_id"], cp["event_id"]}

		ec = by_key[(entity_id, "EnrollmentCompleted")]
		assert ec["parent_event_ids"] == [ag["event_id"]]

		ea = by_key[(entity_id, "EnrollmentArchived")]
		assert ea["parent_event_ids"] == [ec["event_id"]]

	# Fact events have 0 or 1 parent.
	assert all(
		(e["event_kind"] != "fact") or (len(e["parent_event_ids"]) <= 1)
		for e in batch_events
	)

	stream = generate_events_stream(seed=seed, entity_count=entity_count, inconsistency_rate=0.0)
	first = next(stream)
	assert isinstance(first, dict)

	stream_events = [first, *list(stream)]
	assert {e["event_id"] for e in stream_events} == {e["event_id"] for e in batch_events}

	# File output: NDJSON
	ndjson_path = tmp_path / "events.ndjson"
	generate_events_file(
		path=ndjson_path,
		format="ndjson",
		seed=seed,
		entity_count=entity_count,
		inconsistency_rate=0.0,
	)
	ndjson_rows = [json.loads(line) for line in ndjson_path.read_text().splitlines()]
	assert len(ndjson_rows) == len(batch_events)

	# File output: JSON array
	json_path = tmp_path / "events.json"
	generate_events_file(
		path=json_path,
		format="json",
		seed=seed,
		entity_count=entity_count,
		inconsistency_rate=0.0,
	)
	json_rows = json.loads(json_path.read_text())
	assert len(json_rows) == len(batch_events)


@pytest.mark.functional
def test_generator_supports_inconsistent_declarations():
	from data_generator.generator import generate_events_batch

	seed = 1
	consistent = generate_events_batch(seed=seed, entity_count=1, inconsistency_rate=0.0)
	inconsistent = generate_events_batch(seed=seed, entity_count=1, inconsistency_rate=1.0)

	by_type_consistent = {e["event_type"]: e for e in consistent}
	by_type_inconsistent = {e["event_type"]: e for e in inconsistent}

	correct_parents = set(by_type_consistent["AccessGranted"]["parent_event_ids"])
	inconsistent_ag = by_type_inconsistent.get("AccessGranted")

	# In inconsistent mode, the generator may omit the decision or declare incorrect parents.
	assert inconsistent_ag is None or set(inconsistent_ag["parent_event_ids"]) != correct_parents


@pytest.mark.functional
def test_validate_consistent_generated_events_passes(tmp_path):
	from data_generator.generator import generate_events_file
	from data_generator.fixture_validator import validate_events_file

	path = tmp_path / "events.json"
	generate_events_file(
		path=path,
		format="json",
		seed=123,
		entity_count=3,
		inconsistency_rate=0.0,
	)

	result = validate_events_file(path, mode="consistent")
	assert result.is_valid
