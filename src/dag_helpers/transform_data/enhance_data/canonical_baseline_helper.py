from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


Event = dict[str, Any]


def transform_events_to_canonical_baseline_format(events: Iterable[Event]) -> list[Event]:
	"""Canonical baseline for enhance stage.

	Same semantics as fetch_data events baseline, except:
	- `event_name` is ignored (enrichment/presentation field)
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


def save_canonical_baseline_artifact(*, events: Iterable[Event], path: str | Path) -> Path:
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	baseline = transform_events_to_canonical_baseline_format(events)
	path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path
