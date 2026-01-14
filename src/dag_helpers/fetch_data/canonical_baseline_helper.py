from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Sequence


Event = dict[str, Any]


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


def save_canonical_baseline_artifact(*, events: Iterable[Event], path: str | Path) -> Path:
	"""Persist a canonical events baseline artifact."""
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	baseline = transform_events_to_canonical_baseline_format(list(events))
	path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path
