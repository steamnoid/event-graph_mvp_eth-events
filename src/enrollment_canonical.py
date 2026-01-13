"""Backwards-compatible re-export of canonical Enrollment spec.

The authoritative definitions live under helpers so they are importable inside the
Airflow containers (which mount helpers read-only).
"""

from helpers.enrollment.canonical import (  # noqa: F401
	CANONICAL_ENROLLMENT_SPECS,
	EnrollmentEventSpec,
	canonical_edges,
	canonical_types,
	spec_by_type,
)


__all__ = [
	"EnrollmentEventSpec",
	"CANONICAL_ENROLLMENT_SPECS",
	"canonical_types",
	"canonical_edges",
	"spec_by_type",
]
