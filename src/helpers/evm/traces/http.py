import requests


def post_json_rpc(*, url: str, payload: dict, timeout_s: int = 30) -> dict:
    if not isinstance(payload, (dict, list)):
        raise TypeError("payload must be a dict or list")

    resp = requests.post(url, json=payload, timeout=timeout_s)
    resp.raise_for_status()
    return resp.json()
