from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Literal, Optional, Sequence


Event = dict[str, Any]
FixtureFormat = Literal["json", "ndjson"]


def generate_fixture_files(
	*,
	out_events: str | Path,
	format: FixtureFormat = "ndjson",
	seed: int | None = None,
	entity_count: int = 1,
	inconsistency_rate: float = 0.0,
	missing_event_rate: float = 0.0,
	run_id: str | None = None,
	out_rules: str | Path | None = None,
) -> Path:
	"""Generate an events fixture file using `data_generator`.

	Optionally writes causality rules text to `out_rules`.
	"""
	from data_generator.generator import (
		generate_causality_rules_text,
		materialize_events_from_causality_rules_text,
		write_events_file,
	)

	rules_text = generate_causality_rules_text(
		seed=seed,
		entity_count=entity_count,
		inconsistency_rate=inconsistency_rate,
		missing_event_rate=missing_event_rate,
		run_id=run_id,
	)

	if out_rules is not None:
		out_rules_path = Path(out_rules)
		out_rules_path.parent.mkdir(parents=True, exist_ok=True)
		out_rules_path.write_text(rules_text, encoding="utf-8")

	events = materialize_events_from_causality_rules_text(rules_text=rules_text, seed=seed)
	return write_events_file(events=events, path=out_events, format=format)


def read_fixture_file(path: str | Path) -> list[Event]:
	"""Read events from a fixture file (JSON array or NDJSON)."""
	path = Path(path)
	text = path.read_text(encoding="utf-8").strip()
	if not text:
		return []

	if text.startswith("["):
		data = json.loads(text)
		if not isinstance(data, list):
			raise ValueError("JSON fixture must contain a top-level array")
		return data

	events: list[Event] = []
	for line in text.splitlines():
		line = line.strip()
		if not line:
			continue
		events.append(json.loads(line))
	return events


def transform_fixture_data_to_events(fixture_data: Sequence[Event]) -> list[Event]:
	"""Transform fixture payload into event dicts.

	This is currently an identity transform, but kept as a seam for future evolution
	of fixture formats.
	"""
	return [dict(e) for e in fixture_data]


def write_events_to_file(
	*,
	events: Iterable[Event],
	path: str | Path,
	format: FixtureFormat = "ndjson",
) -> Path:
	"""Write events to disk as JSON array or NDJSON."""
	from data_generator.generator import write_events_file

	return write_events_file(events=events, path=path, format=format)


def transform_events_to_canonical_baseline_format(events: Sequence[Event]) -> list[Event]:
	"""Make events diff-friendly and stable.

	Baseline rules:
	- stable ordering by (entity_id, layer, event_type, event_id)
	- stable ordering of parent_event_ids
	"""
	baseline: list[Event] = []
	for event in events:
		copy = dict(event)
		parents = copy.get("parent_event_ids")
		if isinstance(parents, list):
			copy["parent_event_ids"] = sorted(parents)
		baseline.append(copy)

	def _key(e: Event) -> tuple:
		return (
			e.get("entity_id") or "",
			e.get("layer") or "",
			e.get("event_type") or "",
			e.get("event_id") or "",
		)

	return sorted(baseline, key=_key)


def save_pre_transformation_canonical_baseline_artifact(
	*,
	fixture_data: Sequence[Event],
	path: str | Path,
) -> Path:
	"""Persist a canonical baseline before any transformation step.

	Today this is effectively the same as post-transformation (our transform is identity),
	but we keep both APIs to make the pipeline stages explicit.
	"""
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	baseline = transform_events_to_canonical_baseline_format(fixture_data)
	path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path


def save_post_transformation_canonical_baseline_artifact(
	*,
	events: Sequence[Event],
	path: str | Path,
) -> Path:
	"""Persist a canonical baseline after transformation to events."""
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	baseline = transform_events_to_canonical_baseline_format(events)
	path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path
