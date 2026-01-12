import pytest


@pytest.mark.unit
def test_transform_events_adds_edge_from_last_sync_to_swap_in_same_tx():
	from dags.helpers.neo4j.transformer import transform_events

	events = [
		{"event_id": "0xabc:1", "tx_hash": "0xabc", "log_index": 1, "event_name": "Sync"},
		{"event_id": "0xabc:2", "tx_hash": "0xabc", "log_index": 2, "event_name": "Swap"},
	]

	edges = transform_events(events)

	assert edges == [{"from": "0xabc:1", "to": "0xabc:2"}]


@pytest.mark.unit
def test_transform_events_adds_edges_from_prior_transfers_to_swap_in_same_tx():
	from dags.helpers.neo4j.transformer import transform_events

	events = [
		{"event_id": "0xabc:0", "tx_hash": "0xabc", "log_index": 0, "event_name": "Transfer"},
		{"event_id": "0xabc:1", "tx_hash": "0xabc", "log_index": 1, "event_name": "Swap"},
	]

	edges = transform_events(events)

	assert edges == [{"from": "0xabc:0", "to": "0xabc:1"}]


@pytest.mark.unit
def test_transform_events_adds_edges_from_last_sync_and_prior_transfers_to_mint_in_same_tx():
	from dags.helpers.neo4j.transformer import transform_events

	events = [
		{"event_id": "0xabc:0", "tx_hash": "0xabc", "log_index": 0, "event_name": "Transfer"},
		{"event_id": "0xabc:1", "tx_hash": "0xabc", "log_index": 1, "event_name": "Sync"},
		{"event_id": "0xabc:2", "tx_hash": "0xabc", "log_index": 2, "event_name": "Mint"},
	]

	edges = transform_events(events)

	assert edges == [
		{"from": "0xabc:0", "to": "0xabc:2"},
		{"from": "0xabc:1", "to": "0xabc:2"},
	]


@pytest.mark.unit
def test_transform_events_adds_edge_from_last_sync_to_burn_in_same_tx():
	from dags.helpers.neo4j.transformer import transform_events

	events = [
		{"event_id": "0xabc:0", "tx_hash": "0xabc", "log_index": 0, "event_name": "Sync"},
		{"event_id": "0xabc:1", "tx_hash": "0xabc", "log_index": 1, "event_name": "Burn"},
	]

	edges = transform_events(events)

	assert edges == [{"from": "0xabc:0", "to": "0xabc:1"}]


@pytest.mark.unit
def test_transform_events_adds_edges_from_pool_transfers_to_sync_in_same_tx():
	from dags.helpers.neo4j.transformer import transform_events

	pool = "0xPOOL"

	events = [
		{
			"event_id": "0xabc:0",
			"tx_hash": "0xabc",
			"log_index": 0,
			"event_name": "Transfer",
			"decoded": {"from": "0xSENDER", "to": pool, "value": 1},
		},
		{
			"event_id": "0xabc:1",
			"tx_hash": "0xabc",
			"log_index": 1,
			"event_name": "Sync",
			"address": pool,
		},
	]

	edges = transform_events(events)

	assert edges == [{"from": "0xabc:0", "to": "0xabc:1"}]