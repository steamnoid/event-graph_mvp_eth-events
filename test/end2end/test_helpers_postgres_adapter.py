import pytest
from dags.helpers.postgres.adapter import get_postgres_connection

@pytest.mark.e2e
def test_postgres_connection():
    """Test that we can connect to the PostgreSQL database."""
    try:
        connection = get_postgres_connection()
        assert connection is not None, "Failed to establish a connection to PostgreSQL."
        connection.close()
    except Exception as e:
        pytest.fail(f"PostgreSQL connection test failed: {e}")