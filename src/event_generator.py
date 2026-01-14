"""Backward-compatible shim for the Enrollment event generator."""

from enrollment.generator import (  # noqa: F401
	generate_causality_rules_text,
	generate_events_batch,
	generate_events_file,
	generate_events_stream,
	materialize_events_from_causality_rules_text,
	write_events_file,
)

__all__ = [
	"generate_causality_rules_text",
	"generate_events_batch",
	"generate_events_file",
	"generate_events_stream",
	"materialize_events_from_causality_rules_text",
	"write_events_file",
]
