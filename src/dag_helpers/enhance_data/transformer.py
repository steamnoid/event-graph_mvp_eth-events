from __future__ import annotations

import re
from typing import Any, Iterable


Event = dict[str, Any]


def enhance_events_add_event_name(events: Iterable[Event]) -> list[Event]:
	"""Ensure each event has `event_name` for downstream consumers.

	Rules:
	- If `event_name` is missing or falsy, set it to `event_type` (if present).
	- Never mutate the input event objects.
	"""
	out: list[Event] = []
	for event in events:
		copy = dict(event)
		if not copy.get("event_name") and copy.get("event_type"):
			copy["event_name"] = _event_type_to_event_name(str(copy.get("event_type")))
		out.append(copy)
	return out


_CAMEL_BOUNDARY_1 = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_CAMEL_BOUNDARY_2 = re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])")


def _event_type_to_event_name(event_type: str) -> str:
	"""Convert `event_type` like `PaymentConfirmed` into `Payment Confirmed`.

	Handles both camelCase and PascalCase, and preserves acronyms reasonably
	(e.g. HTTPServerStarted -> HTTP Server Started).
	"""
	text = event_type.strip()
	if not text:
		return text

	# Handle snake/kebab case too (cheap win).
	text = text.replace("_", " ").replace("-", " ")

	# Insert spaces on case transitions.
	text = _CAMEL_BOUNDARY_2.sub(" ", text)
	text = _CAMEL_BOUNDARY_1.sub(" ", text)

	# Collapse extra whitespace.
	text = " ".join(text.split())

	# Ensure leading word is capitalized for camelCase inputs.
	if text and text[0].islower():
		text = text[0].upper() + text[1:]

	return text
