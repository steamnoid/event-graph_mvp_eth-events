from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def _bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def consume_raw_logs_to_file(
    *,
    topic: str,
    out_file: str,
    partition: int = 0,
    offset: int = 0,
    max_messages: int = 1,
) -> int:
    """Consume a bounded range of raw-log JSON messages and write them as a JSON list.

    Determinism contract:
    - Caller supplies (partition, offset, max_messages)
    - We read exactly max_messages messages starting at offset
    - If fewer messages are available, we raise

    Notes:
    - Imports kafka-python lazily so host tests don't require it.
    """

    if max_messages <= 0:
        raise ValueError("max_messages must be > 0")

    try:
        from kafka import KafkaConsumer, TopicPartition  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"kafka-python is required at runtime: {e}")

    consumer = KafkaConsumer(
        bootstrap_servers=_bootstrap_servers(),
        enable_auto_commit=False,
        auto_offset_reset="none",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=2000,
    )

    tp = TopicPartition(topic, partition)
    consumer.assign([tp])
    consumer.seek(tp, offset)

    messages: list[dict[str, Any]] = []
    while len(messages) < max_messages:
        batch = consumer.poll(timeout_ms=1000, max_records=max_messages - len(messages))
        records = batch.get(tp) or []
        for record in records:
            messages.append(record.value)
        if not records:
            break

    consumer.close()

    if len(messages) != max_messages:
        raise RuntimeError(
            f"Expected {max_messages} messages from {topic}[{partition}] at offset {offset}, got {len(messages)}"
        )

    out_path = Path(out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(messages), encoding="utf-8")
    return len(messages)


def plan_bootstrap_offsets(*, end_offset: int, num_messages: int) -> int:
    if num_messages <= 0:
        raise ValueError("num_messages must be > 0")
    if end_offset < 0:
        raise ValueError("end_offset must be >= 0")

    start_offset = end_offset - num_messages
    if start_offset < 0:
        raise ValueError("start_offset would be negative")
    return start_offset
