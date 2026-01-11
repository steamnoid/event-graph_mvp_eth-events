import pytest


@pytest.mark.e2e
@pytest.mark.parametrize("port, service", [
    (5432, "Postgres"),
    (7474, "Neo4j UI"),
    (7687, "Neo4j"),
    (8080, "Airflow UI"),

])
def test_neo4j_service_running(port, service):
    _wait_for_tcp_port(service, "localhost", port)
    

def _wait_for_tcp_port(service: str, host: str, port: int, attempts: int = 30) -> None:
    """Poll a TCP port until it accepts a connection, or fail."""
    import socket
    import time

    for _ in range(attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(0.5)
    pytest.fail(f"{service} service is not running on port {port}")