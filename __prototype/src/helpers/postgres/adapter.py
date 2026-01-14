from os import getenv

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def _get_postgres_setting(env_var_name: str, default: str) -> str:
    value = getenv(env_var_name)
    return value if value else default


def _get_postgres_connection_params() -> dict:
    host = _get_postgres_setting("POSTGRES_HOST", "localhost")
    port = int(_get_postgres_setting("POSTGRES_PORT", "5432"))
    user = _get_postgres_setting("POSTGRES_USER", "airflow")
    password = _get_postgres_setting("POSTGRES_PASSWORD", "airflow")
    dbname = _get_postgres_setting("POSTGRES_DB", "airflow")

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "dbname": dbname,
    }


def get_postgres_connection():
    """Establish a connection to the PostgreSQL database."""

    params = _get_postgres_connection_params()

    try:
        return psycopg2.connect(**params)
    except Exception as exc:
        raise ConnectionError(f"Failed to connect to PostgreSQL: {exc}")