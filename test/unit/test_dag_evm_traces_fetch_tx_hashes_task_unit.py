import json
import os
from pathlib import Path

import pytest


@pytest.mark.unit
def test_fetch_tx_hashes_to_file_uses_erc20_receipt_filter_helper(monkeypatch, tmp_path: Path):
    import docker.dags.evm_traces_transaction_ingest_dag as dagmod

    monkeypatch.setenv("ALCHEMY_URL", "http://example.invalid")

    def fake_run_dir(run_id: str) -> Path:
        out = tmp_path / run_id
        out.mkdir(parents=True, exist_ok=True)
        return out

    monkeypatch.setattr(dagmod, "_run_dir", fake_run_dir)

    def fake_select(*, provider_url: str, from_block: int, to_block: int, max_tx_per_block: int | None) -> list[str]:
        assert provider_url == "http://example.invalid"
        assert (from_block, to_block) == (10, 11)
        assert max_tx_per_block == 3
        return ["0xB", "0xD"]

    from helpers.evm.traces import tx_select

    monkeypatch.setattr(tx_select, "select_tx_hashes_for_block_range_with_erc20_transfer_receipts", fake_select)

    block_range_file = tmp_path / "block_range.json"
    block_range_file.write_text(json.dumps({"from_block": 10, "to_block": 11}), encoding="utf-8")

    out_file = dagmod.fetch_tx_hashes_to_file(str(block_range_file), run_id="unit")

    assert json.loads(Path(out_file).read_text(encoding="utf-8")) == ["0xB", "0xD"]
