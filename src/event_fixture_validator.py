from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple


Event = Dict[str, Any]
ValidationMode = Literal["consistent", "inconsistent"]


_CANONICAL: Dict[str, Tuple[str, str, Sequence[str]]] = {
	"CourseEnrollmentRequested": ("fact", "L0", ()),
	"PaymentProcessingRequested": ("fact", "L1", ("CourseEnrollmentRequested",)),
	"PaymentConfirmed": ("fact", "L2", ("PaymentProcessingRequested",)),
	"UserProfileLoaded": ("fact", "L3", ("PaymentConfirmed",)),
	"CourseAccessRequested": ("fact", "L3", ("PaymentConfirmed",)),
	"EligibilityChecked": ("fact", "L4", ("UserProfileLoaded",)),
	"ContentAvailabilityChecked": ("fact", "L4", ("CourseAccessRequested",)),
	"EligibilityPassed": ("fact", "L5", ("EligibilityChecked",)),
	"ContentAvailable": ("fact", "L5", ("ContentAvailabilityChecked",)),
	"ContentPrepared": ("fact", "L6", ("ContentAvailable",)),
	"AccessGranted": (
		"decision",
		"L6",
		("PaymentConfirmed", "EligibilityPassed", "ContentPrepared"),
	),
	"EnrollmentCompleted": ("decision", "L7", ("AccessGranted",)),
	"EnrollmentArchived": ("fact", "L8", ("EnrollmentCompleted",)),
}


@dataclass(frozen=True)
class ValidationResult:
	is_valid: bool
	errors: Tuple[str, ...] = ()


def _load_events(path: Path) -> List[Event]:
	text = path.read_text(encoding="utf-8").strip()
	if not text:
		return []

	if text.startswith("["):
		data = json.loads(text)
		if not isinstance(data, list):
			raise ValueError("JSON file must contain a top-level array")
		return data

	# NDJSON
	events: List[Event] = []
	for line in text.splitlines():
		line = line.strip()
		if not line:
			continue
		events.append(json.loads(line))
	return events


def validate_events(events: Sequence[Event], *, mode: ValidationMode) -> ValidationResult:
	errors: List[str] = []

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

	if not events:
		errors.append("no events found")
		return ValidationResult(is_valid=False, errors=tuple(errors))

	for idx, event in enumerate(events):
		missing = required_keys.difference(event.keys())
		if missing:
			errors.append(f"event[{idx}] missing keys: {sorted(missing)}")
			continue

		if event["event_kind"] not in {"fact", "decision"}:
			errors.append(f"event[{idx}] invalid event_kind: {event['event_kind']}")

		if not isinstance(event["parent_event_ids"], list):
			errors.append(f"event[{idx}] parent_event_ids must be a list")
			continue

		if event["event_kind"] == "fact" and len(event["parent_event_ids"]) > 1:
			errors.append(
				f"event[{idx}] fact has >1 parent: {event['event_type']}"
			)

	event_ids = [e.get("event_id") for e in events if isinstance(e, dict)]
	if len(set(event_ids)) != len(event_ids):
		errors.append("event_id values must be unique")

	all_ids = set(event_ids)
	for idx, event in enumerate(events):
		if not isinstance(event, dict) or "parent_event_ids" not in event:
			continue
		for parent_id in event["parent_event_ids"]:
			if parent_id not in all_ids:
				errors.append(
					f"event[{idx}] parent_event_id not found in file: {parent_id}"
				)

	if mode == "consistent":
		by_entity: Dict[str, Dict[str, Event]] = {}
		for event in events:
			entity_id = event.get("entity_id")
			event_type = event.get("event_type")
			if not isinstance(entity_id, str) or not isinstance(event_type, str):
				continue
			by_entity.setdefault(entity_id, {})[event_type] = event

		for entity_id, type_map in by_entity.items():
			# Require all canonical event types for each entity.
			missing_types = set(_CANONICAL.keys()).difference(type_map.keys())
			if missing_types:
				errors.append(
					f"entity {entity_id} missing event types: {sorted(missing_types)}"
				)
				continue

			for event_type, (kind, layer, parent_types) in _CANONICAL.items():
				event = type_map[event_type]
				if event["event_kind"] != kind:
					errors.append(
						f"entity {entity_id} {event_type} wrong kind: {event['event_kind']}"
					)
				if event["layer"] != layer:
					errors.append(
						f"entity {entity_id} {event_type} wrong layer: {event['layer']}"
					)

				# Validate declared parent relationships match canonical declarations.
				parent_ids_expected = [type_map[p]["event_id"] for p in parent_types]
				if kind == "decision" and event_type == "AccessGranted":
					if set(event["parent_event_ids"]) != set(parent_ids_expected):
						errors.append(
							f"entity {entity_id} AccessGranted parents mismatch"
						)
				else:
					if event["parent_event_ids"] != parent_ids_expected:
						errors.append(
							f"entity {entity_id} {event_type} parents mismatch"
						)

				# Optional sanity: expected id format.
				expected_id = f"{entity_id}:{event_type}"
				if event["event_id"] != expected_id:
					errors.append(
						f"entity {entity_id} {event_type} unexpected event_id: {event['event_id']}"
					)

	return ValidationResult(is_valid=(len(errors) == 0), errors=tuple(errors))


def validate_events_file(path: str | Path, *, mode: ValidationMode) -> ValidationResult:
	path = Path(path)
	events = _load_events(path)
	return validate_events(events, mode=mode)
