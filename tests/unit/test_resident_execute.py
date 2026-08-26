"""Unit tests for omo.resident.execute — execution-adapter (WP-G / M3.2 双载体).

M3.2 执行闭环契约 (对齐 bin/plan/bet-ledger.py validate_worker_instruction_binding):
- pi 分支必须从真实 run 文件 (.omo/_delivery/agent-workflows/runs/<run_id>.yaml)
  解析完整 delivery_binding (work_packet.packet_id + work_packet_hash + dict
  instruction_binding), 否则 fail-closed → binding_run_unavailable
- multica 分支走 autopilot create+trigger (默认 binding 兜底)
- 缺 prompt → execution_requires_prompt (fail-closed)
- 异常路径 → execution_failed 且保留真实 run binding 以便排查
"""

from __future__ import annotations

import pytest
import yaml

from omo.resident import execute

REAL_IB = {
    "instruction_ref": "resident-workpacket-v1",
    "instruction_version": "1",
    "content_digest": "sha256:" + "b" * 64,
    "instruction_profile": "resident",
}
REAL_HASH = "sha256:" + "a" * 64


class _FakePi:
    """Minimal pi-worker-adapter stand-in capturing the call."""

    def __init__(self, *, error: bool = False) -> None:
        self.error = error
        self.last_kwargs: dict | None = None

    def run_worker(self, **kwargs):
        self.last_kwargs = kwargs
        if self.error:
            raise RuntimeError("boom")
        return {"status": "ok", "prompt": kwargs["prompt"], "execute": kwargs["execute"]}


@pytest.fixture(autouse=True)
def _isolate_workspace(tmp_path: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point WORKSPACE at a temp dir with an empty runs/ tree (never touch real runs)."""
    runs_dir = tmp_path / ".omo" / "_delivery" / "agent-workflows" / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(execute, "WORKSPACE", tmp_path)


def _write_run(
    tmp_path,
    run_id: str,
    *,
    packet_id: str = "WP-TEST-1",
    hash_val: str | None = None,
    ib=None,
) -> object:
    """Write a governed run file with complete work_packet + instruction_binding."""
    runs_dir = tmp_path / ".omo" / "_delivery" / "agent-workflows" / "runs"
    run_path = runs_dir / f"{run_id}.yaml"
    data = {
        "run_id": run_id,
        "work_packet": {"packet_id": packet_id},
        "work_packet_hash": hash_val if hash_val is not None else REAL_HASH,
        "instruction_binding": ib if ib is not None else dict(REAL_IB),
    }
    run_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return run_path


def test_execute_missing_prompt_fails_closed() -> None:
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: _FakePi())
    try:
        receipt = execute._execute({"event_type": "ExecutionRequested"}, execute=True)
        assert receipt == {"error": "execution_requires_prompt"}
    finally:
        monkeypatch.undo()


def test_execute_prompt_runs_pi_adapter(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """pi 分支: 从真实 run 文件解析的 binding 含真实 packet_id / hash / dict ib."""
    fake = _FakePi()
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: fake)
    run_id = "run-42"
    _write_run(tmp_path, run_id, packet_id="WP-REAL-1")
    event = {
        "event_type": "ExecutionRequested",
        "workflow_run_id": run_id,
        "event_id": "evt-9",
        "payload": {"prompt": "do the thing"},
    }
    receipt = execute._execute(event, execute=True)
    assert receipt["status"] == "ok"
    assert fake.last_kwargs is not None
    assert fake.last_kwargs["prompt"] == "do the thing"
    assert fake.last_kwargs["execute"] is True
    binding = fake.last_kwargs["delivery_binding"]
    assert binding["run_id"] == run_id
    assert binding["packet_id"] == "WP-REAL-1"
    assert binding["packet_hash"] == REAL_HASH
    assert isinstance(binding["instruction_binding"], dict)
    assert binding["instruction_binding"] == REAL_IB


def test_execute_pi_without_run_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """pi 分支缺真实 run 文件 → fail-closed binding_run_unavailable (不猜测)."""
    fake = _FakePi()
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: fake)
    event = {
        "event_type": "ExecutionRequested",
        "workflow_run_id": "no-such-run",
        "payload": {"prompt": "p"},
    }
    receipt = execute._execute(event, execute=True)
    assert receipt["error"] == "binding_run_unavailable"
    assert receipt["run_id"] == "no-such-run"
    assert fake.last_kwargs is None  # never touched the pi adapter


def test_execute_pi_incomplete_run_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """run 存在但 instruction_binding 是字符串 (旧简化契约) → fail-closed."""
    fake = _FakePi()
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: fake)
    run_id = "run-ib-str"
    _write_run(tmp_path, run_id, ib="resident-workpacket-v1")
    event = {
        "event_type": "ExecutionRequested",
        "workflow_run_id": run_id,
        "payload": {"prompt": "p"},
    }
    receipt = execute._execute(event, execute=True)
    assert receipt["error"] == "binding_run_unavailable"
    assert fake.last_kwargs is None


def test_execute_pi_placeholder_hash_run_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """run 存在但 work_packet_hash 是占位符 (非 sha256:64hex) → fail-closed."""
    fake = _FakePi()
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: fake)
    run_id = "run-ph"
    _write_run(tmp_path, run_id, hash_val="sha256:0" * 4)
    event = {
        "event_type": "ExecutionRequested",
        "workflow_run_id": run_id,
        "payload": {"prompt": "p"},
    }
    receipt = execute._execute(event, execute=True)
    assert receipt["error"] == "binding_run_unavailable"
    assert fake.last_kwargs is None


def test_execute_instruction_keyword_used(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    fake = _FakePi()
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: fake)
    run_id = "run-keyword"
    _write_run(tmp_path, run_id)
    event = {
        "event_type": "ExecutionRequested",
        "workflow_run_id": run_id,
        "payload": {"instruction": "do it"},
    }
    receipt = execute._execute(event, execute=False)
    assert receipt["status"] == "ok"
    assert fake.last_kwargs["prompt"] == "do it"


def test_execute_exception_returns_error_with_binding(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """异常路径保留真实 run binding (非默认兜底), 便于排查."""
    fake = _FakePi(error=True)
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: fake)
    run_id = "run-boom"
    _write_run(tmp_path, run_id)
    event = {
        "event_type": "ExecutionRequested",
        "workflow_run_id": run_id,
        "payload": {"prompt": "boom"},
    }
    receipt = execute._execute(event, execute=True)
    assert receipt["error"].startswith("execution_failed: RuntimeError: boom")
    assert isinstance(receipt["binding"]["instruction_binding"], dict)
    assert receipt["binding"]["instruction_binding"] == REAL_IB


class _FakeCompleted:
    """Minimal subprocess.CompletedProcess stand-in for multica CLI calls."""

    def __init__(self, *, returncode: int, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _stub_multica(monkeypatch: pytest.MonkeyPatch, create: _FakeCompleted, trigger: _FakeCompleted) -> list:
    """Replace subprocess.run with a call recorder returning canned results."""
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):  # noqa: ANN001 - test stub
        calls.append(list(cmd))
        if cmd[1] == "autopilot" and cmd[2] == "create":
            return create
        return trigger

    monkeypatch.setattr(execute.subprocess, "run", fake_run)
    return calls


def test_execute_multica_backend_dispatches(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _stub_multica(
        monkeypatch,
        create=_FakeCompleted(returncode=0, stdout='{"id": "ap-123"}'),
        trigger=_FakeCompleted(returncode=0, stdout='{"ok": true}'),
    )
    event = {
        "event_type": "ExecutionRequested",
        "workflow_run_id": "run-77",
        "payload": {"prompt": "do remote work", "backend": "multica"},
    }
    receipt = execute._execute(event, execute=True)
    assert receipt["status"] == "dispatched"
    assert receipt["backend"] == "multica"
    assert receipt["autopilot_id"] == "ap-123"
    assert receipt["agent"] == "Mika"
    assert len(calls) == 2
    assert calls[0][:4] == ["multica", "autopilot", "create", "--agent"]
    assert calls[0][4] == "Mika"
    assert calls[1][:4] == ["multica", "autopilot", "trigger", "ap-123"]


def test_execute_multica_create_failure_reports_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_multica(
        monkeypatch,
        create=_FakeCompleted(returncode=1, stderr="no auth token"),
        trigger=_FakeCompleted(returncode=0, stdout="{}"),
    )
    event = {"event_type": "ExecutionRequested", "payload": {"prompt": "p", "backend": "multica"}}
    receipt = execute._execute(event, execute=True)
    assert "multica_create_failed" in receipt["error"]


def test_execute_multica_trigger_failure_reports_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_multica(
        monkeypatch,
        create=_FakeCompleted(returncode=0, stdout='{"id": "ap-9"}'),
        trigger=_FakeCompleted(returncode=1, stderr="trigger refused"),
    )
    event = {"event_type": "ExecutionRequested", "payload": {"prompt": "p", "backend": "multica"}}
    receipt = execute._execute(event, execute=True)
    assert "multica_trigger_failed" in receipt["error"]
    assert receipt["autopilot_id"] == "ap-9"


def test_execute_multica_skipped_reflected_in_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """agent runtime 离线 → 平台跳过运行, 顶层 status 应为 dispatched_skipped 而非 dispatched."""
    _stub_multica(
        monkeypatch,
        create=_FakeCompleted(returncode=0, stdout='{"id": "ap-77"}'),
        trigger=_FakeCompleted(
            returncode=0,
            stdout='{"status": "skipped", "reason_code": "runtime_offline"}',
        ),
    )
    event = {"event_type": "ExecutionRequested", "payload": {"prompt": "p", "backend": "multica"}}
    receipt = execute._execute(event, execute=True)
    assert receipt["status"] == "dispatched_skipped"
    assert receipt["trigger"]["reason_code"] == "runtime_offline"


def test_execute_unknown_backend_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakePi()
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: fake)
    event = {"event_type": "ExecutionRequested", "payload": {"prompt": "p", "backend": "bogus"}}
    receipt = execute._execute(event, execute=True)
    assert receipt["error"] == "unknown_backend: bogus"
    assert fake.last_kwargs is None  # never touched the pi adapter
