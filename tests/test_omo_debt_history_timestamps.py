"""债务状态流转的历史条目必须带真实时间戳 —— 审计时间线不得断裂。

缺陷 (2026-11-23, `OMO-DEBT-HISTORY-EMPTY-TIMESTAMP`)：`omo_debt_cli.py` 的
`reclassify` / `escalate` / `revalidate` 三处 `append_history(...)` **未传**
`timestamp=`，而 `omo_debt_lifecycle.append_history` 的签名默认 `timestamp=""`
⇒ 这三条状态流转落盘为 `at: ''`。同文件 `close` / `desc` / `reopen` / `register`
是正确写法（传 `_now_iso()`）。

## 为什么另起一个文件

既有的两条同类用例（`test_omo_debt_cli.py::test_debt_reclassify_updates_dimension_fields`、
`..._escalate_and_revalidate_...`）在 `tests/conftest.py` 的 `_EXPLICIT_MODULES` 里
**默认跳过**（要 `--run-real-omo`），且它们 subprocess 调的是 `scripts/omo_debt.py`
—— 那条路径在本仓根本不存在。所以钉不住这条。本文件改用 `register` 自建条目
（完全 hermetic，不碰真实 `.omo`），并经 `omo_debt_cli.main([...])` 进程内驱动。
"""

from __future__ import annotations

import ast
from pathlib import Path

import yaml

from omo.omo_debt_cli import main

_ITEM = "TS-PROBE"
_CLI_SOURCE = Path(__file__).resolve().parents[1] / "src" / "omo" / "omo_debt_cli.py"


def _run(omo_dir: Path, command: str, *argv: str) -> None:
    assert main([command, "--omo-dir", str(omo_dir), *argv]) == 0, command


def _seed(omo_dir: Path) -> None:
    omo_dir.mkdir(parents=True, exist_ok=True)
    _run(
        omo_dir,
        "register",
        "--id",
        _ITEM,
        "--title",
        "ts probe",
        "--dimension",
        "governance",
        "--subdimension",
        "consistency",
        "--severity",
        "medium",
        "--owner",
        "governance-team",
    )


def test_state_transitions_write_real_history_timestamps(tmp_path: Path) -> None:
    """行为面：reclassify / escalate 落盘的历史条目必须带非空 `at`。"""
    omo_dir = tmp_path / ".omo"
    _seed(omo_dir)
    _run(
        omo_dir,
        "reclassify",
        "--id",
        _ITEM,
        "--dimension",
        "governance_process",
        "--subdimension",
        "pointer_hygiene",
    )
    _run(omo_dir, "escalate", "--id", _ITEM, "--gate-level", "gate")

    payload = yaml.safe_load((omo_dir / "debt" / "items" / f"{_ITEM}.yaml").read_text(encoding="utf-8"))
    stamped = [entry for entry in payload["history"] if entry["action"] in ("reclassify", "escalate")]
    assert {entry["action"] for entry in stamped} == {"reclassify", "escalate"}
    for entry in stamped:
        assert entry["at"], f"{entry['action']} 落盘为 at: '' —— 审计时间线断裂"


def _append_history_calls(tree: ast.AST) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "append_history"
    ]


def test_every_append_history_call_passes_a_timestamp() -> None:
    """结构面：`omo_debt_cli.py` 里每一处 `append_history` 都要显式传 `timestamp=`。

    行为面只覆盖跑得到的那两处；这一条把 `revalidate` 以及**未来新增**的调用点一并盖住
    —— 漏传是这次的自伤形状，靠人记不可靠。
    """
    tree = ast.parse(_CLI_SOURCE.read_text(encoding="utf-8"), filename=str(_CLI_SOURCE))
    calls = _append_history_calls(tree)
    assert len(calls) >= 6, f"调用点数量异常（{len(calls)}），扫描器可能已失效"
    missing = [call.lineno for call in calls if not any(keyword.arg == "timestamp" for keyword in call.keywords)]
    assert missing == [], f"这些 append_history 调用未传 timestamp=: 行 {missing}"


def test_timestamp_detector_is_not_a_no_op() -> None:
    """反证：检测器必须能报出「漏传 timestamp」的调用，否则上面那条只是空转。"""
    good = ast.parse("append_history(payload, 'x', 'y', timestamp=_now_iso())\n")
    bad = ast.parse("append_history(payload, 'x', 'y')\n")
    assert not [
        call.lineno for call in _append_history_calls(good) if not any(kw.arg == "timestamp" for kw in call.keywords)
    ]
    assert [
        call.lineno for call in _append_history_calls(bad) if not any(kw.arg == "timestamp" for kw in call.keywords)
    ] == [1], "检测器对漏传 timestamp 的调用无感"
