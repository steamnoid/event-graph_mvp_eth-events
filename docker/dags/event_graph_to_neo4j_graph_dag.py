from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pendulum
from airflow.decorators import dag, task


# Make repo sources importable inside the Airflow container.
# docker-compose mounts the repository `src/` at `/opt/airflow/dags/src`.
_SRC_PATH = "/opt/airflow/dags/src"
if _SRC_PATH not in sys.path:
	sys.path.insert(0, _SRC_PATH)


DAG_ID = "event_graph_to_neo4j_graph"


def _safe_run_id(run_id: str) -> str:
	# Keep paths portable and reasonably readable.
	return re.sub(r"[^a-zA-Z0-9._-]+", "_", run_id).strip("_") or "run"


@dag(
	dag_id=DAG_ID,
	start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
	schedule=None,
	catchup=False,
	tags=["event-graph", "neo4j"],
)
def event_graph_to_neo4j_graph_dag():
	"""Full pipeline DAG using `src/dag_helpers`.

	Pipeline:
	- fetch_data: generate events + reference baselines (events + edges)
	- enhance_data: add event_name + candidate baseline
	- events_to_edges: produce edges + candidate baseline
	- store_edges_in_neo4j: write edges, read back, canonicalize readback + candidate baseline
	- validate_baseline: always compares candidate vs fetch reference
	"""

	@task
	def fetch() -> dict[str, str]:
		from dag_helpers.fetch_data.task import fetch_data

		run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual")
		safe_run_id = _safe_run_id(run_id)

		artifact_root = Path("/opt/airflow/dags/artifacts") / DAG_ID / safe_run_id
		artifact_root.mkdir(parents=True, exist_ok=True)

		events_path = artifact_root / "events.ndjson"
		rules_path = artifact_root / "rules.txt"

		ref_events_baseline, ref_edges_baseline = fetch_data(
			artifact_dir=artifact_root / "artifacts_fetch",
			out_events=events_path,
			out_rules=rules_path,
			format="ndjson",
			seed=20260114,
			entity_count=6,
			inconsistency_rate=0.0,
			missing_event_rate=0.0,
			run_id=f"airflow:{safe_run_id}",
		)

		return {
			"artifact_root": str(artifact_root),
			"events_path": str(events_path),
			"ref_events_baseline": str(ref_events_baseline),
			"ref_edges_baseline": str(ref_edges_baseline),
			"safe_run_id": safe_run_id,
		}

	@task
	def validate_fetch_events(fetch_out: dict[str, str]) -> str:
		from dag_helpers.validate_baseline.task import validate_canonical_baseline

		artifact_root = Path(fetch_out["artifact_root"])
		ref_events = fetch_out["ref_events_baseline"]

		validated = validate_canonical_baseline(
			reference_baseline_path=ref_events,
			candidate_baseline_path=ref_events,
			artifact_dir=artifact_root / "artifacts_validate_ref_events",
			out_name="C_ref_events.json",
		)
		return str(validated)

	@task
	def enhance(fetch_out: dict[str, str]) -> dict[str, str]:
		from dag_helpers.transform_data.enhance_data.task import enhance_data

		artifact_root = Path(fetch_out["artifact_root"])
		source_events = fetch_out["events_path"]
		out_events = artifact_root / "enhanced_events.ndjson"

		candidate_baseline, out_path = enhance_data(
			artifact_dir=artifact_root / "artifacts_enhance",
			source_events=source_events,
			out_events=out_events,
			format="ndjson",
		)

		return {
			"enhanced_events_path": str(out_path),
			"candidate_events_baseline": str(candidate_baseline),
		}

	@task
	def validate_enhanced_events(fetch_out: dict[str, str], enhance_out: dict[str, str]) -> str:
		from dag_helpers.validate_baseline.task import validate_canonical_baseline

		artifact_root = Path(fetch_out["artifact_root"])
		validated = validate_canonical_baseline(
			reference_baseline_path=fetch_out["ref_events_baseline"],
			candidate_baseline_path=enhance_out["candidate_events_baseline"],
			artifact_dir=artifact_root / "artifacts_validate_enhance",
			out_name="C_enhance_events.json",
		)
		return str(validated)

	@task
	def build_edges(fetch_out: dict[str, str], enhance_out: dict[str, str]) -> dict[str, str]:
		from dag_helpers.transform_data.events_to_edges.task import events_to_edges

		artifact_root = Path(fetch_out["artifact_root"])
		out_edges = artifact_root / "edges.json"

		candidate_baseline, edges_path = events_to_edges(
			artifact_dir=artifact_root / "artifacts_edges",
			source_events=enhance_out["enhanced_events_path"],
			out_edges=out_edges,
		)

		return {
			"edges_path": str(edges_path),
			"candidate_edges_baseline": str(candidate_baseline),
		}

	@task
	def validate_edges(fetch_out: dict[str, str], edges_out: dict[str, str]) -> str:
		from dag_helpers.validate_baseline.task import validate_canonical_baseline

		artifact_root = Path(fetch_out["artifact_root"])
		validated = validate_canonical_baseline(
			reference_baseline_path=fetch_out["ref_edges_baseline"],
			candidate_baseline_path=edges_out["candidate_edges_baseline"],
			artifact_dir=artifact_root / "artifacts_validate_edges",
			out_name="C_edges.json",
		)
		return str(validated)

	@task
	def store_in_neo4j(fetch_out: dict[str, str], edges_out: dict[str, str]) -> dict[str, str]:
		from dag_helpers.store_data.neo4j.task import store_edges_in_neo4j

		artifact_root = Path(fetch_out["artifact_root"])
		run_id = fetch_out["safe_run_id"]

		candidate_baseline, readback_edges_path = store_edges_in_neo4j(
			artifact_dir=artifact_root / "artifacts_neo4j",
			source_edges=edges_out["edges_path"],
			run_id=f"airflow:{run_id}",
			clear_run_first=True,
		)

		return {
			"neo4j_candidate_edges_baseline": str(candidate_baseline),
			"readback_edges_path": str(readback_edges_path),
		}

	@task
	def validate_neo4j_readback(fetch_out: dict[str, str], neo4j_out: dict[str, str]) -> str:
		from dag_helpers.validate_baseline.task import validate_canonical_baseline

		artifact_root = Path(fetch_out["artifact_root"])
		validated = validate_canonical_baseline(
			reference_baseline_path=fetch_out["ref_edges_baseline"],
			candidate_baseline_path=neo4j_out["neo4j_candidate_edges_baseline"],
			artifact_dir=artifact_root / "artifacts_validate_neo4j",
			out_name="C_neo4j_edges.json",
		)
		return str(validated)

	fetch_out = fetch()
	_ = validate_fetch_events(fetch_out)
	enhance_out = enhance(fetch_out)
	_ = validate_enhanced_events(fetch_out, enhance_out)
	edges_out = build_edges(fetch_out, enhance_out)
	_ = validate_edges(fetch_out, edges_out)
	neo4j_out = store_in_neo4j(fetch_out, edges_out)
	_ = validate_neo4j_readback(fetch_out, neo4j_out)


event_graph_to_neo4j_graph_dag()
