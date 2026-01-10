import socket
import pytest

def test_neo4j_service_running():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        if sock.connect_ex(('localhost', 7474)) != 0:
            pytest.fail("Neo4j service is not running on port 7474")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        if sock.connect_ex(('localhost', 7687)) != 0:
            pytest.fail("Neo4j service is not running on port 7687")