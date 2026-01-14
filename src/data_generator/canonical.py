from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Sequence


@dataclass(frozen=True)
class EnrollmentEventSpec:
	event_type: str
	event_kind: Literal["fact", "decision"]
	layer: str
	parent_event_types: Sequence[str]


CANONICAL_ENROLLMENT_SPECS: Sequence[EnrollmentEventSpec] = (
	EnrollmentEventSpec(
		event_type="CourseEnrollmentRequested",
		event_kind="fact",
		layer="L0",
		parent_event_types=(),
	),
	EnrollmentEventSpec(
		event_type="PaymentProcessingRequested",
		event_kind="fact",
		layer="L1",
		parent_event_types=("CourseEnrollmentRequested",),
	),
	EnrollmentEventSpec(
		event_type="PaymentConfirmed",
		event_kind="fact",
		layer="L2",
		parent_event_types=("PaymentProcessingRequested",),
	),
	EnrollmentEventSpec(
		event_type="UserProfileLoaded",
		event_kind="fact",
		layer="L3",
		parent_event_types=("PaymentConfirmed",),
	),
	EnrollmentEventSpec(
		event_type="CourseAccessRequested",
		event_kind="fact",
		layer="L3",
		parent_event_types=("PaymentConfirmed",),
	),
	EnrollmentEventSpec(
		event_type="EligibilityChecked",
		event_kind="fact",
		layer="L4",
		parent_event_types=("UserProfileLoaded",),
	),
	EnrollmentEventSpec(
		event_type="ContentAvailabilityChecked",
		event_kind="fact",
		layer="L4",
		parent_event_types=("CourseAccessRequested",),
	),
	EnrollmentEventSpec(
		event_type="EligibilityPassed",
		event_kind="fact",
		layer="L5",
		parent_event_types=("EligibilityChecked",),
	),
	EnrollmentEventSpec(
		event_type="ContentAvailable",
		event_kind="fact",
		layer="L5",
		parent_event_types=("ContentAvailabilityChecked",),
	),
	EnrollmentEventSpec(
		event_type="ContentPrepared",
		event_kind="fact",
		layer="L6",
		parent_event_types=("ContentAvailable",),
	),
	EnrollmentEventSpec(
		event_type="AccessGranted",
		event_kind="decision",
		layer="L6",
		parent_event_types=(
			"PaymentConfirmed",
			"EligibilityPassed",
			"ContentPrepared",
		),
	),
	EnrollmentEventSpec(
		event_type="EnrollmentCompleted",
		event_kind="decision",
		layer="L7",
		parent_event_types=("AccessGranted",),
	),
	EnrollmentEventSpec(
		event_type="EnrollmentArchived",
		event_kind="fact",
		layer="L8",
		parent_event_types=("EnrollmentCompleted",),
	),
)


def canonical_types() -> set[str]:
	return {spec.event_type for spec in CANONICAL_ENROLLMENT_SPECS}


def canonical_edges() -> set[tuple[str, str]]:
	edges: set[tuple[str, str]] = set()
	for child in CANONICAL_ENROLLMENT_SPECS:
		for parent_type in child.parent_event_types:
			edges.add((parent_type, child.event_type))
	return edges


def spec_by_type() -> dict[str, EnrollmentEventSpec]:
	return {spec.event_type: spec for spec in CANONICAL_ENROLLMENT_SPECS}
