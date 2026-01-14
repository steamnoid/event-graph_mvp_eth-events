from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable, Literal


Event = dict[str, Any]
FixtureFormat = Literal["json", "ndjson"]


def read_events_from_file(path: str | Path) -> list[Event]:
	"""Read events from disk (JSON array or NDJSON)."""
	path = Path(path)
	text = path.read_text(encoding="utf-8").strip()
	if not text:
		return []

	if text.startswith("["):
		data = json.loads(text)
		if not isinstance(data, list):
			raise ValueError("events file must contain a top-level JSON array")
		return [dict(e) for e in data]

	events: list[Event] = []
	for line in text.splitlines():
		line = line.strip()
		if not line:
			continue
		events.append(json.loads(line))
	return [dict(e) for e in events]


def write_events_to_file(
	*,
	events: Iterable[Event],
	path: str | Path,
	format: FixtureFormat = "ndjson",
) -> Path:
	"""Write events to disk as JSON array or NDJSON.

	This is intentionally implemented locally so downstream stages do not depend
	on the fetch stage's adapter helpers.
	"""
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	payload = [dict(e) for e in events]

	if format == "json":
		path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
		return path

	if format != "ndjson":
		raise ValueError(f"unsupported format: {format}")

	lines = [json.dumps(e, sort_keys=True) for e in payload]
	path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
	return path


def enhance_events_add_event_name(events: Iterable[Event]) -> list[Event]:
	"""Ensure each event has `event_name` for downstream consumers.

	Rules:
	- If `event_name` is missing or falsy, set it from `event_type`.
	- Never mutate the input event objects.
	"""
	out: list[Event] = []
	for event in events:
		copy = dict(event)
		if not copy.get("event_name") and copy.get("event_type"):
			copy["event_name"] = _event_type_to_event_name(str(copy.get("event_type")))
		out.append(copy)
	return out


def transform_events_to_canonical_baseline_format(events: Iterable[Event]) -> list[Event]:
	"""Make events diff-friendly and stable (canonical baseline).

	This uses the same baseline semantics and naming as `dag_helpers.fetch_data`,
	with one important nuance:
	- `event_name` is ignored because it is a presentation/enrichment field and
	  should not affect canonical diffs.

	Baseline rules:
	- stable ordering by (entity_id, layer, event_type, event_id)
	- stable ordering of parent_event_ids
	"""
	baseline: list[Event] = []
	for event in events:
		copy = dict(event)
		copy.pop("event_name", None)

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
	fixture_data: Iterable[Event],
	path: str | Path,
) -> Path:
	"""Persist a canonical baseline before this transformation step."""
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	baseline = transform_events_to_canonical_baseline_format(fixture_data)
	path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path


def save_post_transformation_canonical_baseline_artifact(
	*,
	events: Iterable[Event],
	path: str | Path,
) -> Path:
	"""Persist a canonical baseline after this transformation step."""
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	baseline = transform_events_to_canonical_baseline_format(events)
	path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path


_CAMEL_BOUNDARY_1 = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_CAMEL_BOUNDARY_2 = re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])")


def _event_type_to_event_name(event_type: str) -> str:
	"""Convert `event_type` like `PaymentConfirmed` into `Payment Confirmed`.

	Handles both camelCase and PascalCase, and preserves acronyms reasonably
	(e.g. HTTPServerStarted -> HTTP Server Started).
	"""
	text = event_type.strip()
	if not text:
		return text

	# Handle snake/kebab case too (cheap win).
	text = text.replace("_", " ").replace("-", " ")

	# Insert spaces on case transitions.
	text = _CAMEL_BOUNDARY_2.sub(" ", text)
	text = _CAMEL_BOUNDARY_1.sub(" ", text)

	# Collapse extra whitespace.
	text = " ".join(text.split())

	# Ensure leading word is capitalized for camelCase inputs.
	if text and text[0].islower():
		text = text[0].upper() + text[1:]

	return text
