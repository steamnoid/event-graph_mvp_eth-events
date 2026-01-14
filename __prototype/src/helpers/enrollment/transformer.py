from typing import Any, List


Event = dict[str, Any]


def task_transform_events_to_normalized_file(events_file: str, **context) -> str:
	"""Airflow task callable: Stage C2 transform."""
	from helpers.enrollment import pipeline

	run_id = str(context.get("run_id") or "manual")
	return pipeline.transform_events_to_normalized_file(run_id=run_id, events_file=events_file)


def normalize_events(events: List[Event]) -> List[Event]:
	"""Normalize Enrollment events into a stable, schema-compatible representation.

	Enrollment fixtures are already close to the canonical schema; this step mainly:
	- validates required keys and basic types
	- adds `event_name` for downstream Neo4j adapter compatibility
	"""
	normalized: List[Event] = []
	for idx, event in enumerate(events):
		_validate_event_is_object(event=event, idx=idx)
		_validate_required_keys(event=event, idx=idx)
		_validate_parent_ids_is_list(event=event, idx=idx)
		normalized.append(_normalize_one_event(event=event))

	return normalized


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


def _validate_event_is_object(*, event: Any, idx: int) -> None:
	if not isinstance(event, dict):
		raise ValueError(f"event[{idx}] must be an object")


def _validate_required_keys(*, event: Event, idx: int) -> None:
	missing = _REQUIRED_KEYS.difference(event.keys())
	if missing:
		raise ValueError(f"event[{idx}] missing keys: {sorted(missing)}")


def _validate_parent_ids_is_list(*, event: Event, idx: int) -> None:
	parent_ids = event.get("parent_event_ids")
	if not isinstance(parent_ids, list):
		raise ValueError(f"event[{idx}] parent_event_ids must be a list")


def _normalize_one_event(*, event: Event) -> Event:
	out = dict(event)
	# Match the Neo4j adapter display field used by the graph writer.
	out.setdefault("event_name", out.get("event_type"))
	return out
