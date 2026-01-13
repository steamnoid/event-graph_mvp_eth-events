from typing import Any, List


Event = dict[str, Any]


_REQUIRED_KEYS = {
	"event_id",
	"event_type",
	"event_kind",
	"parent_event_ids",
	"layer",
	"entity_id",
	"payload",
	"emitted_at",
}


def normalize_events(events: List[Event]) -> List[Event]:
	"""Normalize Enrollment events into a stable, schema-compatible representation.

	Enrollment fixtures are already close to the canonical schema; this step mainly:
	- validates required keys and basic types
	- adds `event_name` for downstream Neo4j adapter compatibility
	"""
	normalized: List[Event] = []
	for idx, event in enumerate(events):
		if not isinstance(event, dict):
			raise ValueError(f"event[{idx}] must be an object")

		missing = _REQUIRED_KEYS.difference(event.keys())
		if missing:
			raise ValueError(f"event[{idx}] missing keys: {sorted(missing)}")

		parent_ids = event.get("parent_event_ids")
		if not isinstance(parent_ids, list):
			raise ValueError(f"event[{idx}] parent_event_ids must be a list")

		out = dict(event)
		# Match ETH helper convention used by Neo4j adapter.
		out.setdefault("event_name", out.get("event_type"))
		normalized.append(out)

	return normalized
