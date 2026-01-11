"""Unit tests for causal edge inference rules."""

from dags.dag_helper_functions.neo4j_graph import build_causes_edges


def test_build_causes_edges_links_state_change_to_sync_in_same_tx():
    """Sync should depend on the last pool state-changing event in the tx."""
    events = [
        {
            "event_id": "0xaaa:1",
            "tx_hash": "0xaaa",
            "log_index": 1,
            "event_name": "Swap",
        },
        {
            "event_id": "0xaaa:2",
            "tx_hash": "0xaaa",
            "log_index": 2,
            "event_name": "Sync",
        },
    ]

    edges = build_causes_edges(events)

    assert edges == [("0xaaa:1", "0xaaa:2")]


def test_build_causes_edges_chains_unknown_events_within_tx():
    """Unknown events are chained within a tx to increase connectedness."""
    events = [
        {
            "event_id": "0xbbb:3",
            "tx_hash": "0xbbb",
            "log_index": 3,
            "event_name": "ExecutionSuccess",
        },
        {
            "event_id": "0xbbb:4",
            "tx_hash": "0xbbb",
            "log_index": 4,
            "event_name": "SafeMultiSigTransaction",
        },
    ]

    edges = build_causes_edges(events)

    assert edges == [("0xbbb:3", "0xbbb:4")]
