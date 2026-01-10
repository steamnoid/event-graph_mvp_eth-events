import socket
import pytest

def test_airflow_service_running():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        if sock.connect_ex(('localhost', 8080)) != 0:
            pytest.fail("Airflow service is not running on port 8080")