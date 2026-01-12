import pytest


@pytest.mark.unit
def test_post_json_rpc_posts_payload_and_returns_json(monkeypatch):
    from helpers.evm.traces.http import post_json_rpc

    calls: list[dict] = []

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"result": "ok"}

    def _fake_post(url, json, timeout):
        calls.append({"url": url, "json": json, "timeout": timeout})
        return _Resp()

    import requests

    monkeypatch.setattr(requests, "post", _fake_post)

    payload = {"jsonrpc": "2.0", "id": 1, "method": "trace_block", "params": ["0x10"]}
    assert post_json_rpc(url="http://example", payload=payload, timeout_s=3) == {"result": "ok"}

    assert calls == [{"url": "http://example", "json": payload, "timeout": 3}]


@pytest.mark.unit
def test_post_json_rpc_supports_batch_payload_list(monkeypatch):
    from helpers.evm.traces.http import post_json_rpc

    calls: list[dict] = []

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return [{"id": 1, "result": "ok"}]

    def _fake_post(url, json, timeout):
        calls.append({"url": url, "json": json, "timeout": timeout})
        return _Resp()

    import requests

    monkeypatch.setattr(requests, "post", _fake_post)

    payloads = [
        {"jsonrpc": "2.0", "id": 1, "method": "trace_block", "params": ["0x10"]},
        {"jsonrpc": "2.0", "id": 2, "method": "trace_block", "params": ["0x11"]},
    ]
    assert post_json_rpc(url="http://example", payload=payloads, timeout_s=3) == [{"id": 1, "result": "ok"}]

    assert calls == [{"url": "http://example", "json": payloads, "timeout": 3}]


@pytest.mark.unit
def test_post_json_rpc_raises_type_error_on_invalid_payload_type(monkeypatch):
    from helpers.evm.traces.http import post_json_rpc

    def _fake_post(url, json, timeout):
        raise AssertionError("should not call requests.post")

    import requests

    monkeypatch.setattr(requests, "post", _fake_post)

    with pytest.raises(TypeError, match="payload must be a dict or list"):
        post_json_rpc(url="http://example", payload="nope", timeout_s=3)
