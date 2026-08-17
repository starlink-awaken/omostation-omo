from __future__ import annotations

from pathlib import Path

import pytest

from omo import omo_worker_promotion


@pytest.mark.parametrize("returncode", [0, 1])
def test_promotion_sync_uses_canonical_omo_state_broker(monkeypatch, tmp_path: Path, returncode: int) -> None:
    calls: list[tuple[Path, bool, str]] = []

    def fake_sync(omo_dir: Path, dry_run: bool, fmt: str) -> int:
        calls.append((omo_dir, dry_run, fmt))
        return returncode

    monkeypatch.setattr(omo_worker_promotion, "cmd_state_sync", fake_sync)

    if returncode:
        with pytest.raises(Exception, match="omo.*state.*sync"):
            omo_worker_promotion._sync_omo_state(tmp_path, "governed/omo")
    else:
        omo_worker_promotion._sync_omo_state(tmp_path, "governed/omo")

    assert calls == [(tmp_path / "governed/omo", False, "json")]
