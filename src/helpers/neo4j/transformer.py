import hashlib
import json


def _canonical_event(event: dict) -> dict:
	return {
		"event_id": event.get("event_id"),
		"tx_hash": event.get("tx_hash"),
		"log_index": event.get("log_index"),
		"event_name": event.get("event_name"),
	}


def _canonical_edge(edge: dict) -> dict:
	return {"from": edge.get("from"), "to": edge.get("to")}


def compute_graph_hash(*, events: list[dict], edges: list[dict]) -> str:
	"""Deterministic fingerprint of the graph as written to Neo4j.

	We hash only fields that are persisted by the Neo4j adapter today:
	- Events: event_id, tx_hash, log_index, event_name
	- Edges: from, to
	"""
	canon_events = sorted((_canonical_event(e) for e in events), key=lambda e: str(e.get("event_id")))
	canon_edges = sorted(
		(_canonical_edge(ed) for ed in edges),
		key=lambda ed: (str(ed.get("from")), str(ed.get("to"))),
	)

	payload = {"events": canon_events, "edges": canon_edges}
	data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
	return hashlib.sha256(data).hexdigest()


def load_events_from_file(filename: str) -> list[dict]:
	with open(filename, "r", encoding="utf-8") as f:
		return json.load(f)


def write_graph_to_file(*, events: list[dict], edges: list[dict], run_id: str, filename: str) -> None:
	graph = {
		"run_id": run_id,
		"events": events,
		"edges": edges,
		"graph_hash": compute_graph_hash(events=events, edges=edges),
	}
	with open(filename, "w", encoding="utf-8") as f:
		json.dump(graph, f)


def transform_events(events: list[dict]) -> list[dict]:
	by_tx: dict[str, list[dict]] = {}
	for event in events:
		tx_hash = event.get("tx_hash")
		if not tx_hash:
			continue
		by_tx.setdefault(tx_hash, []).append(event)

	edges: list[dict] = []
	for tx_hash, tx_events in by_tx.items():
		_ = tx_hash
		sorted_events = sorted(tx_events, key=lambda e: e.get("log_index", -1))
		last_sync_id: str | None = None
		transfer_ids: list[str] = []
		transfer_events: list[dict] = []
		for event in sorted_events:
			event_id = event.get("event_id")
			if not event_id:
				continue

			name = event.get("event_name")
			if name == "Transfer":
				transfer_ids.append(event_id)
				transfer_events.append(event)
			elif name == "Sync":
				pool = event.get("address")
				if pool:
					for transfer_event in transfer_events:
						decoded = transfer_event.get("decoded") or {}
						if decoded.get("from") == pool or decoded.get("to") == pool:
							edges.append({"from": transfer_event.get("event_id"), "to": event_id})
				last_sync_id = event_id
			elif name in {"Swap", "Mint"}:
				for transfer_id in transfer_ids:
					edges.append({"from": transfer_id, "to": event_id})
				if last_sync_id is not None:
					edges.append({"from": last_sync_id, "to": event_id})
			elif name == "Burn" and last_sync_id is not None:
				edges.append({"from": last_sync_id, "to": event_id})

	return edges
