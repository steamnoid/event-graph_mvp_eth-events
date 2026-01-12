from pathlib import Path
import pytest

_FIXTURE_LOGS_FILE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "eth_logs"
    / "fetch_logs_uniswap_v2_weth_usdc.json"
)


@pytest.mark.behavior
def test_load_events_from_file_loads_transformed_events_from_disk(tmp_path):
    from helpers.eth.adapter import write_logs_to_file
    from helpers.eth.logs.transformer import load_logs_from_file, transform_logs, write_events_to_file
    from helpers.neo4j.transformer import load_events_from_file

    raw_logs = load_logs_from_file(str(_FIXTURE_LOGS_FILE))
    logs_file = tmp_path / "logs.json"
    write_logs_to_file(raw_logs, str(logs_file))

    loaded_logs = load_logs_from_file(str(logs_file))
    transformed = transform_logs(loaded_logs)

    events_file = tmp_path / "events.json"
    written = write_events_to_file(transformed, str(events_file))

    loaded_events = load_events_from_file(str(events_file))

    assert written == len(loaded_events)


@pytest.mark.behavior
def test_write_graph_to_file_writes_schema_v1_with_object_edges(tmp_path):
    import json

    from helpers.eth.logs.transformer import load_logs_from_file, transform_logs, write_events_to_file
    from helpers.neo4j.transformer import load_events_from_file, write_graph_to_file

    raw_logs = load_logs_from_file(str(_FIXTURE_LOGS_FILE))
    transformed = transform_logs(raw_logs)

    events_file = tmp_path / "events.json"
    write_events_to_file(transformed, str(events_file))
    loaded_events = load_events_from_file(str(events_file))

    run_id = "behavior:run-1"
    edges = [{"from": "e1", "to": "e2"}]

    graph_file = tmp_path / "graph.json"
    write_graph_to_file(events=loaded_events, edges=edges, run_id=run_id, filename=str(graph_file))

    with open(graph_file, "r", encoding="utf-8") as f:
        graph = json.load(f)

    assert graph["run_id"] == run_id and graph["edges"] == edges and set(graph.keys()) == {"run_id", "events", "edges"}


@pytest.mark.behavior
def test_full_stack_fixture_to_graph_produces_valid_causal_edges(tmp_path):
    import json

    from helpers.eth.logs.transformer import load_logs_from_file, transform_logs, write_events_to_file
    from helpers.neo4j.transformer import load_events_from_file, transform_events, write_graph_to_file

    raw_logs = load_logs_from_file(str(_FIXTURE_LOGS_FILE))
    events = transform_logs(raw_logs)

    events_file = tmp_path / "events.json"
    write_events_to_file(events, str(events_file))

    loaded_events = load_events_from_file(str(events_file))
    edges = transform_events(loaded_events)

    run_id = "behavior:full-stack"
    graph_file = tmp_path / "graph.json"
    write_graph_to_file(events=loaded_events, edges=edges, run_id=run_id, filename=str(graph_file))

    graph = json.loads(graph_file.read_text(encoding="utf-8"))

    by_id = {e.get("event_id"): e for e in graph.get("events", []) if e.get("event_id")}

    def _edge_ok(edge: dict) -> bool:
        if not isinstance(edge, dict) or set(edge.keys()) != {"from", "to"}:
            return False
        parent = by_id.get(edge.get("from"))
        child = by_id.get(edge.get("to"))
        if not parent or not child:
            return False
        if parent.get("tx_hash") != child.get("tx_hash"):
            return False
        return int(parent.get("log_index")) < int(child.get("log_index"))

    assert (
        set(graph.keys()) == {"run_id", "events", "edges"}
        and graph["run_id"] == run_id
        and len(graph["edges"]) > 0
        and all(_edge_ok(edge) for edge in graph["edges"])
    )

@pytest.mark.behavior
def test_full_stack_fixture_to_neo4j_writes_all_events_and_edges(tmp_path):
    from collections import defaultdict

    from neo4j import GraphDatabase

    from helpers.eth.logs.transformer import load_logs_from_file, transform_logs, write_events_to_file
    from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db
    from helpers.neo4j.transformer import load_events_from_file, transform_events, write_graph_to_file

    raw_logs = load_logs_from_file(str(_FIXTURE_LOGS_FILE))
    events = transform_logs(raw_logs)

    events_file = tmp_path / "events.json"
    write_events_to_file(events, str(events_file))
    loaded_events = load_events_from_file(str(events_file))

    edges = transform_events(loaded_events)

    run_id = "behavior:full-stack-fixture"
    graph_file = tmp_path / "graph.json"
    write_graph_to_file(events=loaded_events, edges=edges, run_id=run_id, filename=str(graph_file))

    graph = load_graph_from_file(str(graph_file))
    write_graph_to_db(graph)

    # Compute average depth (nodes on longest path) per tx from the graph payload.
    events_by_id = {e.get("event_id"): e for e in loaded_events if e.get("event_id")}
    nodes_by_tx = defaultdict(list)
    for e in loaded_events:
        if e.get("tx_hash") and e.get("event_id"):
            nodes_by_tx[e["tx_hash"]].append(e)

    edges_by_tx = defaultdict(list)
    for edge in edges:
        parent = events_by_id.get(edge.get("from"))
        child = events_by_id.get(edge.get("to"))
        if parent and child and parent.get("tx_hash") == child.get("tx_hash"):
            edges_by_tx[parent["tx_hash"]].append(edge)

    def depth_for_tx(tx_hash: str) -> int:
        tx_nodes = nodes_by_tx.get(tx_hash, [])
        if not tx_nodes:
            return 0
        parents = defaultdict(list)
        for edge in edges_by_tx.get(tx_hash, []):
            parents[edge["to"]].append(edge["from"])
        ordered = sorted(tx_nodes, key=lambda e: int(e.get("log_index") or -1))
        dp = {}
        for e in ordered:
            eid = e["event_id"]
            best = 1
            for p in parents.get(eid, []):
                best = max(best, dp.get(p, 1) + 1)
            dp[eid] = best
        return max(dp.values()) if dp else 0

    depths = [depth_for_tx(tx) for tx in edges_by_tx.keys() if edges_by_tx[tx]]
    avg_depth = (sum(depths) / len(depths)) if depths else 0

    uri = "neo4j://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))
    try:
        with driver.session() as session:
            counts = session.run(
                """
                MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:Event)
                WITH r, count(DISTINCT e) AS events
                MATCH (r)-[:INCLUDES]->(a:Event)
                MATCH (r)-[:INCLUDES]->(b:Event)
                OPTIONAL MATCH (a)-[rel:CAUSES]->(b)
                RETURN events AS events, count(DISTINCT rel) AS rels
                """,
                run_id=run_id,
            ).single()

            assert (
                counts is not None
                and int(counts["events"]) == len(loaded_events)
                and int(counts["rels"]) == len(edges)
                and avg_depth >= 2.0
            )
    finally:
        driver.close()


 