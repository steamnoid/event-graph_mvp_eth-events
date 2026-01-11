"""Static config test: docker-compose passes through ALCHEMY_URL."""

from pathlib import Path


def test_docker_compose_exposes_alchemy_url_to_airflow_services():
    """Compose file should reference ALCHEMY_URL via variable interpolation."""
    compose_path = Path(__file__).resolve().parents[2] / "docker" / "docker-compose.yml"
    text = compose_path.read_text(encoding="utf-8")

    # The Airflow tasks rely on ALCHEMY_URL. We want Docker Compose to inject it
    # into the Airflow containers from the developer's local `.env` (not committed).
    assert "ALCHEMY_URL" in text
    assert "${ALCHEMY_URL}" in text
