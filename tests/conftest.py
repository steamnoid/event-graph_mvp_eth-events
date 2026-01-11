"""Pytest configuration for local/integration runs.

This project relies on a developer-provided `ALCHEMY_URL`.

To keep tests deterministic and avoid hardcoding secrets in the repo, we support
loading `ALCHEMY_URL` from `docker/.env` (which is ignored by git) when it is not
already present in the process environment.

No secrets are printed.
"""

from __future__ import annotations

import os
from pathlib import Path


def _load_env_file(path: Path) -> dict[str, str]:
    """Parse a simple .env file (KEY=VALUE lines, # comments)."""
    if not path.exists():
        return {}

    env: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def pytest_configure() -> None:
    """Load `ALCHEMY_URL` from docker/.env into os.environ if missing."""
    if os.getenv("ALCHEMY_URL"):
        return

    repo_root = Path(__file__).resolve().parents[1]
    docker_env = repo_root / "docker" / ".env"
    values = _load_env_file(docker_env)

    alchemy_url = values.get("ALCHEMY_URL")
    if alchemy_url:
        os.environ["ALCHEMY_URL"] = alchemy_url
