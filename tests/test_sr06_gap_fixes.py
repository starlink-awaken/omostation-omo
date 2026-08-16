"""BET-Y1Q3-T5-04: SR-06 四缺口修复测试 (T1-18 retro Q3 转正).

gap-1: AdmissionRenewed 自环续期 (dispatched 态)
gap-2: prompt 契约含 filesModified 强制条目
gap-3: collect 的 changed_paths 含 untracked 扫描
gap-4: supervisor terminal fallback limit 环境变量化
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "projects/omo/src"))

from omo.workflow_mesh import EVENT_STATE, WorkflowMeshStore  # noqa: E402
from omo import workflow_dispatch as wd  # noqa: E402


def test_gap1_admission_renewed_event_registered():
    assert EVENT_STATE["AdmissionRenewed"] == "dispatched"


def test_gap1_renew_admission_roundtrip(tmp_path):
    store_dir = tmp_path / ".omo"
    store_dir.mkdir()
    store = WorkflowMeshStore(store_dir)
    store.append(__import__("omo.workflow_mesh", fromlist=["new_workflow_event"]).new_workflow_event(
        "WorkflowRequested", "run-x", producer="t", payload={}, idempotency_key="a"))
    admission = {
        "admission_id": "admit-1",
        "status": "admitted",
        "workflow_run_id": "run-x",
        "trace_id": "run-x",
        "backend": "supervised-worker",
        "step_run_ids": ["run-x:execute"],
        "capabilities": ["code_change"],
        "policy_digest": "0" * 64,
        "issued_at": "2026-08-15T00:00:00+00:00",
        "expires_at": "2026-08-16T00:00:00+00:00",
    }
    from omo.workflow_dispatch import _proof
    admission["proof"] = _proof(admission)
    store.append(__import__("omo.workflow_mesh", fromlist=["new_workflow_event"]).new_workflow_event(
        "WorkflowAdmitted", "run-x", producer="t",
        payload={"admission": admission},
        idempotency_key="b"))
    store.append(__import__("omo.workflow_mesh", fromlist=["new_workflow_event"]).new_workflow_event(
        "StepDispatched", "run-x", producer="t",
        payload={"step_run_id": "run-x:execute", "admission_id": "admit-1"},
        idempotency_key="c"))
    # dispatched 态续期成功
    r = wd.renew_admission(
        store_dir.parent, workflow_run_id="run-x", admission_id="admit-1",
        ttl_seconds=600, now="2026-08-16T01:00:00+00:00", omo_dir=store_dir)
    assert r["renewed"] is True
    assert r["expires_at"].startswith("2026-08-16T01:10")
    # 身份不匹配拒绝
    try:
        wd.renew_admission(store_dir.parent, workflow_run_id="run-x",
                           admission_id="admit-wrong", omo_dir=store_dir)
        raise AssertionError("should reject mismatched admission")
    except wd.WorkflowDispatchError:
        pass


def test_gap2_prompt_contract_mentions_files_modified():
    src = (ROOT / "projects/omo/src/omo/omo_worker_dispatch.py").read_text(encoding="utf-8")
    assert "filesModified" in src and "non-empty" in src


def test_gap3_untracked_scan_in_collect():
    src = (ROOT / "projects/omo/src/omo/blueprint_control.py").read_text(encoding="utf-8")
    assert '"ls-files", "--others", "--exclude-standard"' in src


def test_gap4_supervisor_limit_env():
    src = (ROOT / "bin/gac/orca-codex-supervisor.py").read_text(encoding="utf-8")
    assert "ORCA_TERMINAL_READ_LIMIT" in src
