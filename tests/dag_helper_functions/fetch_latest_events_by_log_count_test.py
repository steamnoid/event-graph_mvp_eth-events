"""Unit test: fetch latest events by log count.

This avoids provider eth_getLogs limitations by scanning backward in fixed-size
block windows.
"""

from dags.dag_helper_functions import events as events_module


def test_fetch_latest_events_by_log_count_scans_backwards_and_trims(monkeypatch):
    """Given chunked results, the helper returns the latest N events in order."""
    calls = []

    def fake_fetch_events_in_block_range(*, contract_address, provider_url, from_block, to_block):
        calls.append((from_block, to_block))

        # Return 2 events per call with increasing block numbers.
        if (from_block, to_block) == (91, 100):
            return [
                {"event_id": "e1", "block_number": 95, "transaction_index": 0, "log_index": 1},
                {"event_id": "e2", "block_number": 99, "transaction_index": 0, "log_index": 2},
            ]
        if (from_block, to_block) == (81, 90):
            return [
                {"event_id": "e0", "block_number": 85, "transaction_index": 0, "log_index": 0},
                {"event_id": "e1b", "block_number": 86, "transaction_index": 0, "log_index": 1},
            ]

        return []

    monkeypatch.setattr(events_module, "fetch_events_in_block_range", fake_fetch_events_in_block_range)

    events = events_module.fetch_latest_events_by_log_count(
        contract_address="0x0000000000000000000000000000000000000000",
        provider_url="http://example.invalid",
        to_block=100,
        logs_number=3,
        chunk_size=10,
    )

    # Scans backward in chunk windows until >= logs_number events collected.
    assert calls == [(91, 100), (81, 90)]

    # Returns the latest 3 events in canonical order.
    assert [e["event_id"] for e in events] == ["e1b", "e1", "e2"]


def test_fetch_latest_events_by_log_count_respects_max_scan_blocks(monkeypatch):
    """If max_scan_blocks is too small, the helper fails fast (no endless scan)."""
    calls = []

    def fake_fetch_events_in_block_range(*, contract_address, provider_url, from_block, to_block):
        calls.append((from_block, to_block))
        return []

    monkeypatch.setattr(events_module, "fetch_events_in_block_range", fake_fetch_events_in_block_range)

    try:
        events_module.fetch_latest_events_by_log_count(
            contract_address="0x0000000000000000000000000000000000000000",
            provider_url="http://example.invalid",
            to_block=100,
            logs_number=1,
            chunk_size=10,
            max_scan_blocks=20,
        )
    except ValueError as e:
        assert "max_scan_blocks" in str(e)
    else:
        raise AssertionError("Expected ValueError when max_scan_blocks prevents reaching logs_number")

    assert calls == [(91, 100), (81, 90)]


def test_fetch_latest_events_by_log_count_shrinks_chunk_on_provider_too_large(monkeypatch):
    """When provider rejects a window as too large, the helper retries with smaller chunk_size."""
    calls = []

    def fake_fetch_events_in_block_range(*, contract_address, provider_url, from_block, to_block):
        calls.append((from_block, to_block))
        window_size = to_block - from_block + 1
        if window_size > 5:
            raise ValueError("Log response size exceeded")
        return [{"event_id": "eX", "block_number": to_block, "transaction_index": 0, "log_index": 0}]

    monkeypatch.setattr(events_module, "fetch_events_in_block_range", fake_fetch_events_in_block_range)

    events = events_module.fetch_latest_events_by_log_count(
        contract_address="0x0000000000000000000000000000000000000000",
        provider_url="http://example.invalid",
        to_block=100,
        logs_number=1,
        chunk_size=10,
        max_scan_blocks=20,
    )

    assert [e["event_id"] for e in events] == ["eX"]
    # First attempt uses 10 blocks (91-100), then shrinks and retries to 5 blocks (96-100).
    assert calls[:2] == [(91, 100), (96, 100)]
