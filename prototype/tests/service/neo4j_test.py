"""Service smoke test: Neo4j HTTP and Bolt ports are reachable."""

import socket
import subprocess
import time

import pytest


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    """Run a subprocess command and capture stdout/stderr for assertions."""
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def _wait_for_tcp_port(host: str, port: int, attempts: int = 30) -> None:
    """Poll a TCP port until it accepts a connection, or fail."""
    for _ in range(attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(0.5)
    pytest.fail(f"Service is not running on port {port}")


def test_neo4j_service_running():
    """Neo4j should accept TCP connections on localhost:7474 and localhost:7687."""
    compose = ["docker", "compose", "-f", "docker/docker-compose.yml"]
    up = _run(compose + ["up", "-d", "neo4j"])
    assert up.returncode == 0, up.stderr

    _wait_for_tcp_port("localhost", 7474)
    _wait_for_tcp_port("localhost", 7687)