# causality_rules_pipeline.instructions.md — CANONICAL CAUSALITY FORMAT (AUTHORITATIVE)

## Purpose

This repo models behavior as **graphs**. To keep behavior deterministic and debuggable, we require a **single canonical “causality rules” text format** that can be generated from every intermediate representation in the Enrollment pipeline.

The intent is:

> Upstream declares causality.
> Downstream validates declarations.

Downstream systems must never infer workflow.

## Scope

This instruction applies to the **Enrollment synthetic pipeline**:

- input fixtures (JSON / NDJSON)
- normalized event list (helpers)
- graph file (`{run_id, events, edges}`)
- persisted Neo4j subgraph for the run

## Non-negotiable requirement

After each transformation stage, produce the **same canonical causality representation** and assert it is identical across stages.

Stages (minimum):

0) generator causality rules text (banal format)
1) raw fixture (JSON/NDJSON events)
2) normalized events (`helpers/enrollment/transformer.py`)
3) edges list (`helpers/enrollment/graph.py`)
4) graph file (`helpers/enrollment/graph.py` write_graph_to_file)
5) Neo4j readback (query by `run_id` + `CAUSES {run_id}`)

### Equality rule

Let `C(stage)` be the canonical causality representation derived from that stage.

For a given `run_id`, the pipeline must satisfy:

$$
C(0) = C(1) = C(2) = C(3) = C(4) = C(5)
$$

This must hold for:

- multiple `entity_id` values (`entity_count >= 1`)
- deterministic seeds
- different `inconsistency_rate` values (including `0.0`)

**Important:** for `inconsistency_rate > 0.0` the graph may be semantically “wrong”, but the representation must still be preserved identically across stages (no repair, no inference).

## Canonical causality text format

The canonical output is a deterministic, sortable text file with the following structure:

- header: `run_id=<...>`
- repeated blocks per entity:
  - `entity_id=<...>`
  - `types:` list of `event_type`
  - `edges:` list of `parent_type -> child_type`

Sorting requirements:

- entities sorted by `entity_id`
- `types` sorted lexicographically
- `edges` sorted lexicographically by `(parent_type, child_type)`

The format is intentionally comparable with `diff`.

## Canonical-shape validation

When `inconsistency_rate = 0.0`, downstream validation must assert:

- per entity: exact set of canonical Enrollment event types
- per entity: exact set of canonical parent→child edges
- across entities: type list and edge list are identical

When `inconsistency_rate > 0.0`, do not enforce canonical correctness; enforce only **cross-stage equality**.

## Testing policy

- Use `@pytest.mark.e2e` tests that:
  - generate events deterministically
  - write to Neo4j
  - compute `C(stage)` for each stage
  - assert equality across stages
- Avoid UI-driven verification. Visualizations are optional and secondary.

## Implementation notes

- Neo4j readback must filter by `run_id`:
  - nodes: `(r:Run {run_id})-[:INCLUDES]->(:EnrollmentEvent)`
  - edges: `(:EnrollmentEvent)-[:CAUSES {run_id}]->(:EnrollmentEvent)`
- The canonical representation must be derived from **declared parents** only (no inference).

