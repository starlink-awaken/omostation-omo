"""ADR-0203 硬门下沉验证 — `python -m omo.workflow.cli start` 不再可绕过.

DECISION-SCENARIO-DERIVATION §5 (2026-08-17): cli.py 曾无 main guard +
无 chain_bind 检查, -m 调用实际只 import 不执行 (exit 0 零输出的假象)。
修复: main guard + start 分支 chain_bind 门 + sys.modules 注册 loader。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from omo.workflow import cli  # noqa: E402


def test_cli_has_main_guard():
    """main guard 必须存在 — 否则 -m 调用静默 no-op (回归防线)."""
    src = Path(cli.__file__).read_text(encoding="utf-8")
    assert 'if __name__ == "__main__":' in src


def test_start_without_bet_returns_reject(monkeypatch, capsys):
    """无 --bet 的 start: main() 返回 1 且 stderr 有拒绝信息 (不再静默)."""
    cb = cli._load_chain_bind()
    if cb is None:  # 环境 无 bin/plan/chain_bind.py 时门自动失效 (fail-open)
        pytest.skip("chain_bind not available in this layout")
    rc = cli.main(["start", "project-code-change", "--profile", "qa-agent", "--objective", "t"])
    assert rc == 1
    assert "requires --bet" in capsys.readouterr().err


def test_exempt_workflow_passes_without_bet(capsys, tmp_path):
    """observer-audit 豁免面: 无 bet 也可 start (独立 registry 避免环境锁干扰)."""
    from omo.workflow.core import REGISTRY_PATH

    rc = cli.main([
        "--registry", str(REGISTRY_PATH),
        "start", "observer-audit", "--profile", "observer-agent",
        "--objective", "t", "--dry-run",
    ])
    out = capsys.readouterr().out
    assert rc == 0, out
    assert "started" in out
