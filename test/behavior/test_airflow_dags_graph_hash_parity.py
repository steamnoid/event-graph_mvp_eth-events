import json
import subprocess
from pathlib import Path

import pytest


@pytest.mark.e2e
def test_airflow_eth_and_kafka_dags_produce_identical_graph_hash_in_neo4j(tmp_path):
    """Given the same raw logs, both DAGs write the same graph to Neo4j (by graph_hash)."""

    repo_root = Path(__file__).resolve().parents[2]
    docker_dir = repo_root / "docker"

    fixture = repo_root / "test" / "fixtures" / "eth_logs" / "fetch_logs_uniswap_v2_weth_usdc.json"
    raw_logs = json.loads(fixture.read_text(encoding="utf-8"))
    assert isinstance(raw_logs, list) and len(raw_logs) > 0

    # Deterministic subset: first transaction in the fixture, keep all logs for that tx.
    tx_hash = raw_logs[0]["transactionHash"]
    payload = [l for l in raw_logs if l.get("transactionHash") == tx_hash]
    assert len(payload) > 0

    # Write payload into the shared Airflow logs volume.
    host_shared_dir = docker_dir / "logs" / "e2e_inputs" / "graph_hash_parity"
    host_shared_dir.mkdir(parents=True, exist_ok=True)
    host_logs_file = host_shared_dir / "raw_logs.json"
    host_logs_file.write_text(json.dumps(payload), encoding="utf-8")
    container_logs_file = "/opt/airflow/logs/e2e_inputs/graph_hash_parity/raw_logs.json"

    # Create a unique topic and produce the same payload into Kafka.
    topic = "eth.raw_logs.graph_hash_parity"
    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            "kafka:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
        cwd=str(docker_dir),
        check=True,
    )

    def _end_offset() -> int:
        p = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "kafka",
                "kafka-get-offsets",
                "--bootstrap-server",
                "kafka:9092",
                "--topic",
                topic,
                "--time",
                "-1",
            ],
            cwd=str(docker_dir),
            capture_output=True,
            text=True,
            check=True,
        )
        # Format: <topic>:<partition>:<offset>
        line = (p.stdout or "").strip().splitlines()[-1]
        return int(line.split(":")[-1])

    start_offset = _end_offset()

    producer = subprocess.Popen(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-console-producer",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            topic,
        ],
        cwd=str(docker_dir),
        stdin=subprocess.PIPE,
        text=True,
    )
    assert producer.stdin is not None
    for log in payload:
        producer.stdin.write(json.dumps(log) + "\n")
    producer.stdin.close()
    rc = producer.wait(timeout=30)
    assert rc == 0

    max_messages = len(payload)

    # Run both DAGs in-process (no polling): different logical dates -> different run_id nodes.
    conf_eth = json.dumps({"source_logs_file": container_logs_file})
    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "airflow-webserver",
            "airflow",
            "dags",
            "test",
            "eth_to_neo4j_graph",
            "2026-01-02",
            "--conf",
            conf_eth,
        ],
        cwd=str(docker_dir),
        check=True,
    )

    conf_kafka = json.dumps(
        {
            "topic": topic,
            "partition": 0,
            "offset": start_offset,
            "max_messages": max_messages,
        }
    )
    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "airflow-webserver",
            "airflow",
            "dags",
            "test",
            "kafka_eth_to_neo4j_graph",
            "2026-01-03",
            "--conf",
            conf_kafka,
        ],
        cwd=str(docker_dir),
        check=True,
    )

    run_eth = "manual__2026-01-02T00:00:00+00:00"
    run_kafka = "manual__2026-01-03T00:00:00+00:00"

    def _cypher_scalar(query: str) -> str:
        p = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "neo4j",
                "cypher-shell",
                "-u",
                "neo4j",
                "-p",
                "test",
                query,
            ],
            cwd=str(docker_dir),
            capture_output=True,
            text=True,
            check=True,
        )
        value = (p.stdout or "").strip().splitlines()[-1]
        if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
            value = value[1:-1]
        return value

    hash_eth = _cypher_scalar(f"MATCH (r:Run {{run_id: '{run_eth}'}}) RETURN r.graph_hash")
    hash_kafka = _cypher_scalar(f"MATCH (r:Run {{run_id: '{run_kafka}'}}) RETURN r.graph_hash")

    assert hash_eth == hash_kafka

    # Stronger guarantee: also recompute the hash from Neo4j contents.
    from helpers.neo4j.adapter import compute_graph_hash_from_db

    db_hash_eth = compute_graph_hash_from_db(run_eth)
    db_hash_kafka = compute_graph_hash_from_db(run_kafka)

    assert db_hash_eth == hash_eth
    assert db_hash_kafka == hash_kafka
    assert db_hash_eth == db_hash_kafka
