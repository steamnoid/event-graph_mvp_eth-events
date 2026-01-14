"""DAG helper: enrich/augment event payloads.

Currently:
- add `event_name` field for downstream compatibility.
"""

from .transformer import enhance_events_add_event_name  # noqa: F401

__all__ = [
	"enhance_events_add_event_name",
]
