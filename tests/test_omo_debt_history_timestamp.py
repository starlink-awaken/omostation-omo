"""omo-debt reclassify/escalate/revalidate 的 history 时间戳回归 (OMO-DEBT-HISTORY-EMPTY-TIMESTAMP).

缺陷: 这三处 `append_history(...)` 未传 `timestamp=`, 而 `omo_debt_lifecycle.py:9` 的签名默认
`timestamp=""` 且 `actor` 仅在有值时才写入条目 ⇒ 状态流转落盘成 `at: ''`, 审计时间线断裂。
同文件 close/desc/reopen 传 `_now_iso()`, 是正确写法。

实现: in-process 调 `omo.omo_debt_cli.main(argv)`。不用 subprocess —— 本仓不存在
`scripts/omo_debt.py`, `tests/test_omo_debt_cli.py` 里按该路径起子进程的用例在本布局下
必然红, 只靠 `requires_real_omo` 的默认 skip 掩盖, 断言写在那里钉不住任何东西。
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from omo.omo_debt_cli import main as cli_main

ITEM_ID = "TS-REGRESSION-ITEM"


@pytest.fixture
def debt_item(tmp_path: Path) -> Path:
    path = tmp_path / "debt" / "items" / f"{ITEM_ID}.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        f"id: {ITEM_ID}\n"
        "title: Timestamp regression\n"
        "description: history at must be a real UTC timestamp\n"
        "severity: low\n"
        "lifecycle_state: identified\n"
        "history: []\n",
        encoding="utf-8",
    )
    return path


def _last_history(path: Path) -> dict:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload["history"][-1]


def _assert_written_now(path: Path) -> None:
    entry = _last_history(path)
    at = entry["at"]
    assert at, f"history 写出空时间戳: {entry}"
    parsed = datetime.fromisoformat(at)
    assert parsed.tzinfo is not None, f"history 时间戳不是 aware: {at}"
    assert abs((datetime.now(UTC) - parsed).total_seconds()) < 120, f"history 时间戳不是本次运行写的: {at}"


def test_reclassify_history_has_timestamp(debt_item: Path, tmp_path: Path) -> None:
    rc = cli_main(
        [
            "reclassify",
            "--omo-dir",
            str(tmp_path),
            "--id",
            ITEM_ID,
            "--dimension",
            "governance",
            "--subdimension",
            "tooling",
        ]
    )
    assert rc == 0
    assert _last_history(debt_item)["action"] == "reclassify"
    _assert_written_now(debt_item)


def test_escalate_history_has_timestamp(debt_item: Path, tmp_path: Path) -> None:
    rc = cli_main(
        ["escalate", "--omo-dir", str(tmp_path), "--id", ITEM_ID, "--gate-level", "gate"],
    )
    assert rc == 0
    assert _last_history(debt_item)["action"] == "escalate"
    _assert_written_now(debt_item)


def test_revalidate_history_has_timestamp(debt_item: Path, tmp_path: Path) -> None:
    rc = cli_main(
        [
            "revalidate",
            "--omo-dir",
            str(tmp_path),
            "--id",
            ITEM_ID,
            "--reviewed-at",
            "2026-11-23T00:00:00+00:00",
        ]
    )
    assert rc == 0
    assert _last_history(debt_item)["action"] == "revalidate"
    _assert_written_now(debt_item)
