"""Enrollment pipeline causality diff runner.

Generates C(stage) in the canonical banal text format at each pipeline stage and
writes them to disk so you can `diff` them.

Stages:
  C0: generator causality rules text
  C1: raw fixture JSON (events)
  C2: normalized events
  C3: edges list
  C4: graph file
  C5: Neo4j readback

Exit codes:
  0: all stages identical (and canonical checks pass when enabled)
  1: mismatch detected (diffs written)

Usage:
  python scripts/enrollment_causality_diff_runner.py --seed 1337 --entity-count 4 --inconsistency-rate 0.0
  python scripts/enrollment_causality_diff_runner.py --seed 7 --entity-count 4 --inconsistency-rate 0.6

Notes:
  - Neo4j must be running for C5.
  - This runner is a complement to pytest e2e tests; it produces artifacts for humans.
"""

from __future__ import annotations

import argparse
import difflib
import sys
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_DIR = _REPO_ROOT / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _diff(a_name: str, a_text: str, b_name: str, b_text: str) -> str:
    lines = list(
        difflib.unified_diff(
            a_text.splitlines(keepends=True),
            b_text.splitlines(keepends=True),
            fromfile=a_name,
            tofile=b_name,
        )
    )
    return "".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--entity-count", type=int, default=4)
    parser.add_argument("--inconsistency-rate", type=float, default=0.0)
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional explicit run_id. Default derived from seed/entity_count/inconsistency.",
    )
    parser.add_argument(
        "--out-dir",
        default="causality_diffs",
        help="Directory to write C0..C5 and diff files.",
    )
    parser.add_argument(
        "--expect-canonical",
        action="store_true",
        help="Also enforce canonical shape + identical across entities (recommended for inconsistency_rate=0.0).",
    )
    args = parser.parse_args()

    seed: int = args.seed
    entity_count: int = args.entity_count
    inconsistency_rate: float = args.inconsistency_rate
    out_dir = Path(args.out_dir)

    run_id = args.run_id or (
        f"cli:causality-diff:seed={seed}:entities={entity_count}:p={inconsistency_rate}"
    )

    if args.expect_canonical and inconsistency_rate != 0.0:
        raise SystemExit("--expect-canonical only makes sense with --inconsistency-rate 0.0")

    from enrollment_canonical import canonical_edges, canonical_types
    from event_generator import generate_causality_rules_text, generate_events_batch
    from helpers.enrollment.adapter import load_events_from_file, write_events_to_file
    from helpers.enrollment.causality_rules import (
        CausalityRules,
        causality_rules_from_edges,
        causality_rules_from_events,
        format_causality_rules_text,
    )
    from helpers.enrollment.graph import build_edges, write_graph_to_file
    from helpers.enrollment.neo4j_causality_rules import fetch_causality_model_from_neo4j
    from helpers.enrollment.transformer import normalize_events
    from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db

    stage_text: dict[str, str] = {}

    # C0: generator rules
    stage_text["C0"] = generate_causality_rules_text(
        seed=seed,
        entity_count=entity_count,
        inconsistency_rate=inconsistency_rate,
        run_id=run_id,
    )

    # C1: raw fixture
    raw_events = generate_events_batch(
        seed=seed,
        entity_count=entity_count,
        inconsistency_rate=inconsistency_rate,
    )
    fixture_path = out_dir / run_id.replace("/", "_").replace(":", "_") / "events.json"
    fixture_path.parent.mkdir(parents=True, exist_ok=True)
    write_events_to_file(raw_events, str(fixture_path))
    fixture_events = load_events_from_file(str(fixture_path))
    stage_text["C1"] = format_causality_rules_text(
        causality_rules_from_events(run_id=run_id, events=fixture_events)
    )

    # C2: normalized
    normalized = normalize_events(fixture_events)
    stage_text["C2"] = format_causality_rules_text(
        causality_rules_from_events(run_id=run_id, events=normalized)
    )

    # C3: edges list
    edges = build_edges(normalized)
    stage_text["C3"] = format_causality_rules_text(
        causality_rules_from_edges(run_id=run_id, events=normalized, edges=edges)
    )

    # C4: graph file
    graph_file = fixture_path.parent / "graph.json"
    write_graph_to_file(events=normalized, edges=edges, run_id=run_id, filename=str(graph_file))
    graph = load_graph_from_file(str(graph_file))
    stage_text["C4"] = format_causality_rules_text(
        causality_rules_from_edges(run_id=run_id, events=graph["events"], edges=graph["edges"])
    )

    # C5: Neo4j readback
    write_graph_to_db(graph)
    neo_model = fetch_causality_model_from_neo4j(run_id=run_id)
    stage_text["C5"] = format_causality_rules_text(
        CausalityRules(
            run_id=neo_model.run_id,
            types_by_entity=neo_model.types_by_entity,
            edges_by_entity=neo_model.edges_by_entity,
        )
    )

    # Write all stage texts.
    for key, text in stage_text.items():
        _write(fixture_path.parent / f"{key}.txt", text)

    # Optional strict canonical assertions.
    if args.expect_canonical:
        ct = canonical_types()
        ce = canonical_edges()
        for entity_id, types in neo_model.types_by_entity.items():
            if set(types) != ct:
                raise SystemExit(f"canonical types mismatch for {entity_id}")
        for entity_id, pairs in neo_model.edges_by_entity.items():
            if set(pairs) != ce:
                raise SystemExit(f"canonical edges mismatch for {entity_id}")

        entities = sorted(neo_model.types_by_entity.keys())
        ref = entities[0]
        for entity_id in entities[1:]:
            if neo_model.types_by_entity[entity_id] != neo_model.types_by_entity[ref]:
                raise SystemExit("types not identical across entities")
            if neo_model.edges_by_entity[entity_id] != neo_model.edges_by_entity[ref]:
                raise SystemExit("edges not identical across entities")

    # Compare across stages and write diffs.
    order = ["C0", "C1", "C2", "C3", "C4", "C5"]
    mismatch = False
    for i in range(len(order) - 1):
        a = order[i]
        b = order[i + 1]
        a_text = stage_text[a]
        b_text = stage_text[b]
        if a_text != b_text:
            mismatch = True
            d = _diff(f"{a}.txt", a_text, f"{b}.txt", b_text)
            _write(fixture_path.parent / f"diff_{a}_{b}.txt", d)

    print(f"run_id={run_id}")
    print(f"out_dir={fixture_path.parent}")
    print("stages=" + ",".join(order))
    print("result=" + ("OK" if not mismatch else "MISMATCH"))
    return 1 if mismatch else 0


if __name__ == "__main__":
    raise SystemExit(main())
