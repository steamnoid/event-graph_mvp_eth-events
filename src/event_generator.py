from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import random
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Literal, Sequence


Event = Dict[str, Any]
FileFormat = Literal["json", "ndjson"]


@dataclass(frozen=True)
class _EventSpec:
	event_type: str
	event_kind: Literal["fact", "decision"]
	layer: str
	parent_event_types: Sequence[str]


_CANONICAL_SPECS: Sequence[_EventSpec] = (
	_EventSpec(
		event_type="CourseEnrollmentRequested",
		event_kind="fact",
		layer="L0",
		parent_event_types=(),
	),
	_EventSpec(
		event_type="PaymentProcessingRequested",
		event_kind="fact",
		layer="L1",
		parent_event_types=("CourseEnrollmentRequested",),
	),
	_EventSpec(
		event_type="PaymentConfirmed",
		event_kind="fact",
		layer="L2",
		parent_event_types=("PaymentProcessingRequested",),
	),
	_EventSpec(
		event_type="UserProfileLoaded",
		event_kind="fact",
		layer="L3",
		parent_event_types=("PaymentConfirmed",),
	),
	_EventSpec(
		event_type="CourseAccessRequested",
		event_kind="fact",
		layer="L3",
		parent_event_types=("PaymentConfirmed",),
	),
	_EventSpec(
		event_type="EligibilityChecked",
		event_kind="fact",
		layer="L4",
		parent_event_types=("UserProfileLoaded",),
	),
	_EventSpec(
		event_type="ContentAvailabilityChecked",
		event_kind="fact",
		layer="L4",
		parent_event_types=("CourseAccessRequested",),
	),
	_EventSpec(
		event_type="EligibilityPassed",
		event_kind="fact",
		layer="L5",
		parent_event_types=("EligibilityChecked",),
	),
	_EventSpec(
		event_type="ContentAvailable",
		event_kind="fact",
		layer="L5",
		parent_event_types=("ContentAvailabilityChecked",),
	),
	_EventSpec(
		event_type="ContentPrepared",
		event_kind="fact",
		layer="L6",
		parent_event_types=("ContentAvailable",),
	),
	_EventSpec(
		event_type="AccessGranted",
		event_kind="decision",
		layer="L6",
		parent_event_types=(
			"PaymentConfirmed",
			"EligibilityPassed",
			"ContentPrepared",
		),
	),
	_EventSpec(
		event_type="EnrollmentCompleted",
		event_kind="decision",
		layer="L7",
		parent_event_types=("AccessGranted",),
	),
	_EventSpec(
		event_type="EnrollmentArchived",
		event_kind="fact",
		layer="L8",
		parent_event_types=("EnrollmentCompleted",),
	),
)


def _base_time(*, seed: int | None) -> datetime:
	if seed is None:
		return datetime.now(timezone.utc)

	rng = random.Random(seed)
	base = datetime(2026, 1, 1, tzinfo=timezone.utc)
	return base + timedelta(seconds=rng.randrange(0, 24 * 60 * 60))


def _event_id(*, entity_id: str, event_type: str) -> str:
	return f"{entity_id}:{event_type}"


def _maybe_make_inconsistent_parent_ids(
	*,
	rng: random.Random,
	correct_parent_ids: List[str],
	all_event_ids: Sequence[str],
	inconsistency_rate: float,
) -> List[str]:
	if inconsistency_rate <= 0.0:
		return correct_parent_ids

	if rng.random() >= inconsistency_rate:
		return correct_parent_ids

	mode = rng.choice(["missing", "wrong", "mixed"])
	if mode == "missing":
		if not correct_parent_ids:
			return []
		return correct_parent_ids[: rng.randrange(0, len(correct_parent_ids))]

	wrong = []
	if all_event_ids:
		wrong_count = rng.randrange(0, 4)
		wrong = [rng.choice(all_event_ids) for _ in range(wrong_count)]

	if mode == "wrong":
		return wrong

	# mixed
	kept = correct_parent_ids[: rng.randrange(0, len(correct_parent_ids) + 1)]
	return kept + wrong


def _build_entity_events(
	*,
	entity_id: str,
	seed_rng: random.Random,
	base_time: datetime,
	inconsistency_rate: float,
	all_event_ids: Sequence[str],
) -> List[Event]:
	ids_by_type = {
		spec.event_type: _event_id(entity_id=entity_id, event_type=spec.event_type)
		for spec in _CANONICAL_SPECS
	}

	events: List[Event] = []
	for index, spec in enumerate(_CANONICAL_SPECS):
		correct_parent_ids = [ids_by_type[parent] for parent in spec.parent_event_types]
		parent_event_ids = _maybe_make_inconsistent_parent_ids(
			rng=seed_rng,
			correct_parent_ids=correct_parent_ids,
			all_event_ids=all_event_ids,
			inconsistency_rate=inconsistency_rate,
		)

		# With inconsistency, we may omit a decision entirely.
		if (
			spec.event_kind == "decision"
			and inconsistency_rate > 0.0
			and seed_rng.random() < (inconsistency_rate / 4.0)
		):
			continue

		events.append(
			{
				"event_id": ids_by_type[spec.event_type],
				"event_type": spec.event_type,
				"event_kind": spec.event_kind,
				"parent_event_ids": parent_event_ids,
				"layer": spec.layer,
				"entity_id": entity_id,
				"payload": {},
				"emitted_at": (base_time + timedelta(seconds=index)).isoformat(),
			}
		)

	return events


def generate_events_batch(
	*,
	seed: int | None = None,
	entity_count: int = 1,
	inconsistency_rate: float = 0.0,
) -> List[Event]:
	"""Generate a batch of synthetic events.

	- Deterministic when `seed` is provided.
	- Emits events in an arbitrary order (no ordering guarantees).
	- Supports multi-entity generation via `entity_count`.
	- Supports inconsistent causality declarations via `inconsistency_rate`.
	"""
	if entity_count < 1:
		return []

	if inconsistency_rate < 0.0:
		inconsistency_rate = 0.0
	if inconsistency_rate > 1.0:
		inconsistency_rate = 1.0

	rng = random.Random(seed)
	base_time = _base_time(seed=seed)

	# Precompute all possible event ids so we can declare "incorrect" parents that still exist.
	all_event_ids: List[str] = []
	for entity_index in range(entity_count):
		entity_id = f"enrollment-{entity_index + 1}"
		for spec in _CANONICAL_SPECS:
			all_event_ids.append(_event_id(entity_id=entity_id, event_type=spec.event_type))

	events: List[Event] = []
	for entity_index in range(entity_count):
		entity_id = f"enrollment-{entity_index + 1}"
		entity_base_time = base_time + timedelta(minutes=entity_index)
		events.extend(
			_build_entity_events(
				entity_id=entity_id,
				seed_rng=rng,
				base_time=entity_base_time,
				inconsistency_rate=inconsistency_rate,
				all_event_ids=all_event_ids,
			)
		)

	# Arbitrary order: shuffle the stream. Deterministic under seed.
	rng.shuffle(events)
	return events


def generate_events_stream(
	*,
	seed: int | None = None,
	entity_count: int = 1,
	inconsistency_rate: float = 0.0,
) -> Iterator[Event]:
	"""Generate synthetic events as a stream (iterator)."""
	for event in generate_events_batch(
		seed=seed, entity_count=entity_count, inconsistency_rate=inconsistency_rate
	):
		yield event


def write_events_file(
	*,
	events: Iterable[Event],
	path: str | Path,
	format: FileFormat = "ndjson",
) -> Path:
	"""Write events to disk as JSON array or NDJSON."""
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)

	if format == "json":
		with path.open("w", encoding="utf-8") as f:
			json.dump(list(events), f, indent=2, sort_keys=True)
			f.write("\n")
		return path

	if format == "ndjson":
		with path.open("w", encoding="utf-8") as f:
			for event in events:
				f.write(json.dumps(event, sort_keys=True))
				f.write("\n")
		return path

	raise ValueError(f"Unsupported format: {format}")


def generate_events_file(
	*,
	path: str | Path,
	format: FileFormat = "ndjson",
	seed: int | None = None,
	entity_count: int = 1,
	inconsistency_rate: float = 0.0,
) -> Path:
	"""Convenience API: generate events and write them to disk."""
	events = generate_events_batch(
		seed=seed, entity_count=entity_count, inconsistency_rate=inconsistency_rate
	)
	return write_events_file(events=events, path=path, format=format)
