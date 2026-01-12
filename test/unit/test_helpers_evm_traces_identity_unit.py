import pytest


@pytest.mark.unit
def test_trace_event_id_is_tx_hash_plus_trace_address_compact_json():
    from helpers.evm.traces.identity import trace_event_id

    assert trace_event_id(tx_hash="0xabc", trace_address=[0, 1, 0]) == "0xabc:[0,1,0]"


@pytest.mark.unit
def test_parent_trace_address_for_root_is_none():
    from helpers.evm.traces.identity import parent_trace_address

    assert parent_trace_address([]) is None


@pytest.mark.unit
def test_normalize_trace_address_hex_strings_to_ints():
    from helpers.evm.traces.identity import normalize_trace_address

    assert normalize_trace_address(["0x0", "0x1"]) == [0, 1]


@pytest.mark.unit
def test_trace_event_id_accepts_raw_hex_string_trace_address():
    from helpers.evm.traces.identity import trace_event_id

    assert trace_event_id(tx_hash="0xabc", trace_address=["0x0", "0x1"]) == "0xabc:[0,1]"


@pytest.mark.unit
def test_parent_trace_address_accepts_raw_hex_string_trace_address():
    from helpers.evm.traces.identity import parent_trace_address

    assert parent_trace_address(["0x0", "0x1", "0x0"]) == [0, 1]
