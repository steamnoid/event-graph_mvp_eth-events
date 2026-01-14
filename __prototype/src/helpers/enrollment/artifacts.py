from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_ARTIFACT_ROOT = "/opt/airflow/logs/eventgraph"


def run_dir(*, run_id: str, artifact_root: str = DEFAULT_ARTIFACT_ROOT) -> Path:
	base = Path(artifact_root) / run_id
	base.mkdir(parents=True, exist_ok=True)
	return base


def write_text(*, path: Path, text: str) -> str:
	path.parent.mkdir(parents=True, exist_ok=True)
	path.write_text(text, encoding="utf-8")
	return str(path)


def read_text(*, path: Path) -> str:
	return path.read_text(encoding="utf-8")


def write_json(*, path: Path, payload: Any) -> str:
	path.parent.mkdir(parents=True, exist_ok=True)
	path.write_text(json.dumps(payload), encoding="utf-8")
	return str(path)


def copy_text_file(*, source_file: str, dest_file: Path) -> str:
	dest_file.parent.mkdir(parents=True, exist_ok=True)
	dest_file.write_text(Path(source_file).read_text(encoding="utf-8"), encoding="utf-8")
	return str(dest_file)
