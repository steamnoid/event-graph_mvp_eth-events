from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
import sys

import pytest


@pytest.mark.e2e
def test_kafka_produce_then_consume_bounded_offsets_builds_edges() -> None:
    # Import main-project helpers (read-only) for the graph build.
    workspace_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(workspace_root / "src"))

    from helpers.eth.logs.transformer import transform_logs
    from helpers.neo4j.transformer import transform_events

    # Import prototype Kafka runner utilities.
    sys.path.insert(0, str(workspace_root / "__kafka_prototype" / "python"))
    from kafka_eth_log_topic import (  # type: ignore
        ensure_compose_up,
        get_end_offset,
        produce_raw_logs,
        consume_raw_logs,
    )

    fixture_file = workspace_root / "test" / "fixtures" / "eth_logs" / "fetch_logs_uniswap_v2_weth_usdc.json"
    raw_logs_all = json.loads(fixture_file.read_text(encoding="utf-8"))

    # Pick a deterministic tx slice that actually yields causal edges.
    tx_counts = Counter(r.get("transactionHash") for r in raw_logs_all if r.get("transactionHash"))
    tx_candidates = sorted(tx_counts.items(), key=lambda kv: (-kv[1], str(kv[0])))

    selected_logs: list[dict] = []
    selected_edges: list[dict] = []
    for tx_hash, _count in tx_candidates[:50]:
        tx_logs = [r for r in raw_logs_all if r.get("transactionHash") == tx_hash]
        events = transform_logs(tx_logs)
        edges = transform_events(events)
        if edges:
            selected_logs = tx_logs
            selected_edges = edges
            break

    assert selected_logs and selected_edges

    topic = "eth.raw_logs.boundedsmoke"
    project = "eventgraph-kafka-prototype"

    ensure_compose_up(project=project)

    before_end = get_end_offset(project=project, topic=topic, partition=0)

    # Produce the selected logs as JSON lines.
    produce_raw_logs(project=project, topic=topic, raw_logs=selected_logs)

    after_end = get_end_offset(project=project, topic=topic, partition=0)
    start_offset = after_end - len(selected_logs)

    # Consume exactly the produced bounded range.
    consumed = consume_raw_logs(
        project=project,
        topic=topic,
        partition=0,
        offset=start_offset,
        max_messages=len(selected_logs),
    )

    assert len(consumed) == len(selected_logs)

    events = transform_logs(consumed)
    edges = transform_events(events)

    assert edges and all(
        int(next(e["log_index"] for e in events if e["event_id"] == edge["from"]))
        < int(next(e["log_index"] for e in events if e["event_id"] == edge["to"]))
        for edge in edges
    )
