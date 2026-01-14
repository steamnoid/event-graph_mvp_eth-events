from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import random
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Literal, Sequence

from enrollment_canonical import CANONICAL_ENROLLMENT_SPECS, spec_by_type
from helpers.enrollment.causality_rules import (
	CausalityRules,
	render_causality_rules,
	parse_causality_rules,
)


Event = Dict[str, Any]
FileFormat = Literal["json", "ndjson"]


@dataclass(frozen=True)
class _CausalitySpec:
	run_id: str
	types_by_entity: dict[str, list[str]]
	edges_by_entity: dict[str, list[tuple[str, str]]]


def generate_causality_rules_text(
	*,
	seed: int | None = None,
	entity_count: int = 1,
	inconsistency_rate: float,
	missing_event_rate: float = 0.0,
	run_id: str | None = None,
) -> str:
	"""Generate a banal, diff-friendly causality rules text format first.

	This is the generator's first step. JSON/NDJSON events are materialized
	from this intermediate representation.
	"""
	if entity_count < 1:
		entity_count = 0
	if inconsistency_rate < 0.0:
		inconsistency_rate = 0.0
	if inconsistency_rate > 1.0:
		inconsistency_rate = 1.0
	if missing_event_rate < 0.0:
		missing_event_rate = 0.0
	if missing_event_rate > 1.0:
		missing_event_rate = 1.0

	rng = random.Random(seed)
	use_run_id = run_id or f"generator:seed={seed}:entities={entity_count}:inconsistency={inconsistency_rate}"

	# Start from canonical types per entity.
	all_types = [spec.event_type for spec in CANONICAL_ENROLLMENT_SPECS]
	types_by_entity: dict[str, list[str]] = {}
	edges_by_entity: dict[str, list[tuple[str, str]]] = {}

	for entity_index in range(entity_count):
		entity_id = f"enrollment-{entity_index + 1}"
		present_types = _apply_missing_cascade(
			rng=rng,
			all_types=all_types,
			missing_event_rate=missing_event_rate,
		)

		# With inconsistency, we may omit a decision event entirely.
		if inconsistency_rate > 0.0:
			for decision_type in ["AccessGranted", "EnrollmentCompleted"]:
				if rng.random() < (inconsistency_rate / 4.0):
					if decision_type in present_types:
						present_types.remove(decision_type)

		present_type_set = set(present_types)
		types_by_entity[entity_id] = sorted(present_types)

		# Canonical edges for present nodes.
		edges: list[tuple[str, str]] = []
		for child in CANONICAL_ENROLLMENT_SPECS:
			if child.event_type not in present_type_set:
				continue
			for parent_type in child.parent_event_types:
				if parent_type not in present_type_set:
					continue
				edges.append((parent_type, child.event_type))

		# Mutate decision parent declarations only.
		mutated_edges: list[tuple[str, str]] = []
		by_child: dict[str, list[str]] = {}
		for parent_type, child_type in edges:
			by_child.setdefault(child_type, []).append(parent_type)

		for child_type, correct_parents in by_child.items():
			child_spec = spec_by_type().get(child_type)
			if not child_spec:
				continue
			if child_spec.event_kind != "decision":
				# Facts remain constrained to 0/1 parent.
				for p in correct_parents:
					mutated_edges.append((p, child_type))
				continue

			mutated_parent_types = _maybe_make_inconsistent_parent_types(
				rng=rng,
				correct_parent_types=list(correct_parents),
				all_types=[t for t in present_types if t != child_type],
				inconsistency_rate=inconsistency_rate,
				max_wrong=3,
			)
			for p in mutated_parent_types:
				if p in present_type_set and p != child_type:
					mutated_edges.append((p, child_type))

		edges_by_entity[entity_id] = sorted(set(mutated_edges))

	rules = CausalityRules(run_id=use_run_id, types_by_entity=types_by_entity, edges_by_entity=edges_by_entity)
	return render_causality_rules(rules)


def generate_events_batch(
	*,
	seed: int | None = None,
	entity_count: int = 1,
	inconsistency_rate: float = 0.0,
	missing_event_rate: float = 0.0,
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

	# Step 1: generate banal causality rules text.
	rules_text = generate_causality_rules_text(
		seed=seed,
		entity_count=entity_count,
		inconsistency_rate=inconsistency_rate,
		missing_event_rate=missing_event_rate,
	)

	# Step 2: parse and materialize JSON events.
	spec = _parse_spec(rules_text)
	return _materialize_events_from_causality_spec(spec=spec, seed=seed)


def materialize_events_from_causality_rules_text(
	*,
	rules_text: str,
	seed: int | None = None,
) -> List[Event]:
	"""Materialize JSON events from an explicit causality-rules text.

	This is the generator's second step, exposed as a public API so that callers
	can apply deterministic edits to declared causality before materialization.
	"""
	spec = _parse_spec(rules_text)
	return _materialize_events_from_causality_spec(spec=spec, seed=seed)


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


def write_events_file(*, events: Iterable[Event], path: str | Path, format: FileFormat = "ndjson") -> Path:
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


def _base_time(*, seed: int | None) -> datetime:
	if seed is None:
		return datetime.now(timezone.utc)

	rng = random.Random(seed)
	base = datetime(2026, 1, 1, tzinfo=timezone.utc)
	return base + timedelta(seconds=rng.randrange(0, 24 * 60 * 60))


def _event_id(*, entity_id: str, event_type: str) -> str:
	return f"{entity_id}:{event_type}"


def _parse_spec(text: str) -> _CausalitySpec:
	r = parse_causality_rules(text)
	return _CausalitySpec(run_id=r.run_id, types_by_entity=r.types_by_entity, edges_by_entity=r.edges_by_entity)


def _maybe_make_inconsistent_parent_types(
	*,
	rng: random.Random,
	correct_parent_types: List[str],
	all_types: Sequence[str],
	inconsistency_rate: float,
	max_wrong: int = 4,
) -> List[str]:
	if inconsistency_rate <= 0.0:
		return correct_parent_types

	if rng.random() >= inconsistency_rate:
		return correct_parent_types

	mode = rng.choice(["missing", "wrong", "mixed"])
	if mode == "missing":
		if not correct_parent_types:
			return []
		return correct_parent_types[: rng.randrange(0, len(correct_parent_types))]

	wrong = []
	if all_types:
		wrong_count = rng.randrange(0, max_wrong + 1)
		wrong = [rng.choice(all_types) for _ in range(wrong_count)]

	if mode == "wrong":
		return wrong

	kept = correct_parent_types[: rng.randrange(0, len(correct_parent_types) + 1)]
	return kept + wrong


def _apply_missing_cascade(
	*,
	rng: random.Random,
	all_types: list[str],
	missing_event_rate: float,
	root_type: str = "CourseEnrollmentRequested",
) -> list[str]:
	"""Return the surviving event types after applying missing+downstream cascade.

	Model:
	- an event may be missing
	- if an event is missing, any event that *depends on it* is also missing

	This is a simplification to model production gaps without introducing
	business-rule inference outside the declared canonical dependency structure.
	"""
	if missing_event_rate <= 0.0:
		return list(all_types)

	if missing_event_rate < 0.0:
		missing_event_rate = 0.0
	if missing_event_rate > 1.0:
		missing_event_rate = 1.0

	present: set[str] = set(all_types)

	# Seed missing set (never remove the root to keep the entity meaningful).
	missing: set[str] = set()
	for t in all_types:
		if t == root_type:
			continue
		if rng.random() < missing_event_rate:
			missing.add(t)

	# Cascade: if any required parent type is missing, child must be missing.
	if missing:
		missing_changed = True
		while missing_changed:
			missing_changed = False
			for spec in CANONICAL_ENROLLMENT_SPECS:
				child = spec.event_type
				if child in missing:
					continue
				if any(parent in missing for parent in spec.parent_event_types):
					missing.add(child)
					missing_changed = True

	present.difference_update(missing)
	return sorted(present)


def _materialize_events_from_causality_spec(*, spec: _CausalitySpec, seed: int | None) -> list[Event]:
	rng = random.Random(seed)
	base_time = _base_time(seed=seed)
	spec_lookup = spec_by_type()

	events: list[Event] = []
	for entity_index, entity_id in enumerate(sorted(spec.types_by_entity.keys())):
		present_types = list(spec.types_by_entity.get(entity_id) or [])
		present_set = set(present_types)
		entity_base_time = base_time + timedelta(minutes=entity_index)

		# Build parent_event_ids from edge pairs.
		parents_by_child: dict[str, list[str]] = {}
		for parent_type, child_type in spec.edges_by_entity.get(entity_id, []):
			if parent_type not in present_set or child_type not in present_set:
				continue
			parents_by_child.setdefault(child_type, []).append(parent_type)

		# Create stable IDs for present nodes.
		ids_by_type = {t: _event_id(entity_id=entity_id, event_type=t) for t in present_types}

		for index, event_type in enumerate(present_types):
			s = spec_lookup.get(event_type)
			if not s:
				continue
			parent_types = parents_by_child.get(event_type, [])
			parent_event_ids = [ids_by_type[p] for p in parent_types if p in ids_by_type]
			events.append(
				{
					"event_id": ids_by_type[event_type],
					"event_type": event_type,
					"event_kind": s.event_kind,
					"parent_event_ids": parent_event_ids,
					"layer": s.layer,
					"entity_id": entity_id,
					"payload": {},
					"emitted_at": (entity_base_time + timedelta(seconds=index)).isoformat(),
				}
			)

	# Arbitrary order: shuffle deterministically.
	rng.shuffle(events)
	return events
