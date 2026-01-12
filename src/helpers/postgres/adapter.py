import psycopg2
from os import getenv
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def _get_postgres_env_var(var_name):
    value = getenv(var_name)
    if not value:
        raise EnvironmentError(f"Environment variable {var_name} is not set.")
    return value

def get_postgres_connection():
    """Establish a connection to the PostgreSQL database."""
    host = _get_postgres_env_var("POSTGRES_HOST")
    port = _get_postgres_env_var("POSTGRES_PORT")
    user = _get_postgres_env_var("POSTGRES_USER")
    password = _get_postgres_env_var("POSTGRES_PASSWORD")
    dbname = _get_postgres_env_var("POSTGRES_DB")

    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname
        )
        return connection
    except Exception as e:
        raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")