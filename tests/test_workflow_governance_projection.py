"""Governance-ratio projection: start 事件的 CI 可读镜像（去机器化）。

`.omo/_delivery/agent-workflows/**` 既被 gitignore 又被锚定到 state root，CI
干净检出结构性缺输入，40% 治理占比上限因此只在持有账本的机器上执法。本文件钉
发射器三件事：真实入口接线（走 `start_run`，不是直接调函数体）、投影不含自由
文本与绝对路径、以及分类输入与账本同口径。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import omo.workflow.core as core_mod
import omo.workflow.lifecycle as lifecycle_mod
from omo.workflow.governance_projection import (
    PROJECTION_RELATIVE,
    SCHEMA_VERSION,
    build_projection_event,
    projection_dir,
    write_projection,
)
from omo.workflow.lifecycle import start_run


@pytest.fixture()
def registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    monkeypatch.setattr(core_mod, "WORKSPACE", tmp_path)
    monkeypatch.setattr(lifecycle_mod, "WORKSPACE", tmp_path)
    return {
        "runner": {
            "workspace_root": str(tmp_path),
            "run_state_dir": "runs",
            "ledger_path": "ledger/events.jsonl",
        },
        "workflows": [
            {
                "id": "test-workflow",
                "title": "Test",
                "purpose": "test",
                "agents": {"test-agent": {"actor": "tester"}},
                "allowed_lanes": [],
                "lock_scopes": [".omo/_truth"],
                "phases": {},
            }
        ],
        "agent_profiles": {
            "test-agent": {"id": "test-agent", "actor": "tester", "allowed_workflows": ["*"]},
        },
    }


def _start(registry: dict, objective: str) -> dict:
    return start_run(
        registry,
        registry["workflows"][0],
        {"actor": "tester", "profile": "test-agent", "project": ""},
        objective,
        False,
        True,
    )


def _projections(registry: dict) -> list[dict]:
    root = projection_dir(registry)
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(root.glob("*.json"))]


def test_start_run_writes_one_projection_per_run(registry: dict) -> None:
    """真实入口接线：start_run 必须落投影，dry_run 不落。"""
    record = _start(registry, "治理占比 CI 可读面")
    rows = _projections(registry)
    assert [r["run_id"] for r in rows] == [record["run_id"]]
    assert rows[0]["workflow_id"] == "test-workflow"
    assert rows[0]["agent_profile"] == "test-agent"
    assert rows[0]["schema_version"] == SCHEMA_VERSION

    dry = start_run(
        registry,
        registry["workflows"][0],
        {"actor": "tester", "profile": "test-agent", "project": ""},
        "dry",
        True,
        False,
    )
    assert dry["run_id"]
    assert len(_projections(registry)) == 1, "dry_run 不该产生投影"


def test_projection_lands_in_tracked_plane(registry: dict, tmp_path: Path) -> None:
    """投影必须落在检出自身的 tracked 平面，且不跟 agent-workflows 一起被锚定。"""
    _start(registry, "落点")
    root = core_mod.registry_workspace_root(registry)
    assert projection_dir(registry) == root / PROJECTION_RELATIVE
    target = tmp_path / PROJECTION_RELATIVE
    assert target.is_dir()
    assert not target.is_symlink()


def test_projection_carries_no_machine_identity_or_free_text(registry: dict, tmp_path: Path) -> None:
    _start(registry, f"[human] 以 xiamingxing 身份收敛 /Users/{'x'}/Repo 路径")
    rows = _projections(registry)
    assert len(rows) == 1
    blob = json.dumps(rows[0], ensure_ascii=False)
    for forbidden in ("objective", "actor", "/Users/", "xiamingxing"):
        assert forbidden not in blob, f"投影泄漏了 {forbidden!r}"
    assert rows[0]["human_approval"] is True


def test_lock_paths_are_repo_relative_and_foreign_lose_identity(tmp_path: Path) -> None:
    """根内转相对；根外保留尾段但脱掉机器身份 —— 不能整条丢弃。

    丢弃会让投影与账本分类不等价：判据对锁串做 `.omo/` 子串匹配
    （check-governance-ratio.py 的 GOVERNANCE_PATHS 用法）。
    """
    root = tmp_path / "repo"
    (root / ".omo" / "_delivery" / "agent-workflows" / "locks").mkdir(parents=True)
    event = {
        "event": "agent_workflow_start",
        "run_id": "r1",
        "workflow_id": "wf",
        "agent_profile": "p",
        "objective": "x",
        "locks": [
            str(root / ".omo/_delivery/agent-workflows/locks/docs.lock.yaml"),
            str(root / ".omo/_truth/registry/x.yaml"),
            "/Users/someone-else/.omo/_truth/y.yaml",
            "already/relative.lock.yaml",
        ],
    }
    projected = build_projection_event(event, root)
    assert projected is not None
    assert set(projected["locks"]) == {
        ".omo/_delivery/agent-workflows/locks/docs.lock.yaml",
        ".omo/_truth/registry/x.yaml",
        "already/relative.lock.yaml",
        "~/.omo/_truth/y.yaml",
    }
    blob = json.dumps(projected, ensure_ascii=False)
    assert "someone-else" not in blob and "/Users/" not in blob


def test_classifier_flags_are_derived_without_free_text(tmp_path: Path) -> None:
    """两个 flag 是分类器读到的唯一 objective 语义，必须各自可钉住。"""
    root = tmp_path / "repo"
    root.mkdir(parents=True)
    base = {"event": "agent_workflow_start", "workflow_id": "wf", "agent_profile": "p", "locks": []}
    collab = build_projection_event({**base, "run_id": "c1", "objective": "跨机 Collaboration 收敛"}, root)
    plain = build_projection_event({**base, "run_id": "c2", "objective": "归档报告"}, root)
    assert collab["is_collaboration"] is True, "协作腿不再被识别会把它算进治理桶"
    assert plain["is_collaboration"] is False
    assert collab["human_approval"] is False, "无 [human] 前缀不该自带全局豁免"
    assert "跨机" not in json.dumps(collab, ensure_ascii=False)


def test_non_start_event_is_not_projected(tmp_path: Path) -> None:
    assert build_projection_event({"event": "agent_workflow_closeout", "run_id": "r"}, tmp_path) is None
    assert write_projection({"runner": {"workspace_root": str(tmp_path)}}, {"event": "other"}) is None


def test_write_projection_is_idempotent_and_fail_soft(tmp_path: Path) -> None:
    reg = {"runner": {"workspace_root": str(tmp_path)}}
    event = {
        "event": "agent_workflow_start",
        "run_id": "dup",
        "workflow_id": "w",
        "agent_profile": "p",
        "objective": "o",
        "locks": [],
    }
    first = write_projection(reg, event)
    before = first.read_text(encoding="utf-8")
    assert write_projection(reg, event) == first
    assert first.read_text(encoding="utf-8") == before, "重复写不该改动已入账投影"

    blocked = tmp_path / "blocked"
    blocked.mkdir()
    reg_block = {"runner": {"workspace_root": str(blocked)}}
    target = projection_dir(reg_block)
    target.mkdir(parents=True)
    target.chmod(0o500)  # 不可写：投影失败不得打断 start
    try:
        assert write_projection(reg_block, {**event, "run_id": "never"}) is None
    finally:
        target.chmod(0o700)
