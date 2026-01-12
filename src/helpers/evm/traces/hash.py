import hashlib
import json


def trace_graph_hash(graph: dict) -> str:
    events = graph.get("events") or []
    edges = graph.get("edges") or []

    events_sorted = sorted(
        (e for e in events if isinstance(e, dict)),
        key=lambda e: e.get("event_id") or "",
    )
    edges_sorted = sorted(
        (e for e in edges if isinstance(e, dict)),
        key=lambda e: (e.get("from") or "", e.get("to") or ""),
    )

    canonical = {"events": events_sorted, "edges": edges_sorted}
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
