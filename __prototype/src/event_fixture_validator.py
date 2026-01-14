"""Backward-compatible shim for the Enrollment fixture validator."""

from enrollment.fixture_validator import (  # noqa: F401
	ValidationMode,
	ValidationResult,
	validate_events,
	validate_events_file,
)

__all__ = [
	"ValidationMode",
	"ValidationResult",
	"validate_events",
	"validate_events_file",
]
