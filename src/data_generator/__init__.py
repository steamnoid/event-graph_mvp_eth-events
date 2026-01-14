"""Synthetic data generator (Enrollment events).

Copied from `__prototype/` and made self-contained.
"""

from .generator import (  # noqa: F401
	generate_causality_rules_text,
	generate_events_batch,
	generate_events_file,
	generate_events_stream,
	materialize_events_from_causality_rules_text,
	write_events_file,
)
from .fixture_validator import (  # noqa: F401
	ValidationMode,
	ValidationResult,
	validate_events,
	validate_events_file,
)

__all__ = [
	"generate_causality_rules_text",
	"generate_events_batch",
	"generate_events_file",
	"generate_events_stream",
	"materialize_events_from_causality_rules_text",
	"write_events_file",
	"ValidationMode",
	"ValidationResult",
	"validate_events",
	"validate_events_file",
]
