import socket
import pytest

def test_postgres_service_running():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        if sock.connect_ex(('localhost', 5432)) != 0:
            pytest.fail("Postgres service is not running on port 5432")