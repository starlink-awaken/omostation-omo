"""Unit tests for omo.resident.execute — execution-adapter (WP-G).

M2.1d: 覆盖执行链:
- 缺 prompt → execution_requires_prompt (fail-closed)
- 有 prompt → 构造 delivery_binding 并调用 pi adapter
- binding 字段 (run_id/packet_id/packet_hash/instruction_binding)
- 异常路径 → execution_failed 且保留 binding
"""

from __future__ import annotations

import pytest

from omo.resident import execute


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
def _isolate_workspace(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub out the pi adapter load so tests never touch the real worker."""
    monkeypatch.setattr(execute, "WORKSPACE", __import__("pathlib").Path("/tmp/fake-workspace"))


def test_execute_missing_prompt_fails_closed() -> None:
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: _FakePi())
    try:
        receipt = execute._execute({"event_type": "ExecutionRequested"}, execute=True)
        assert receipt == {"error": "execution_requires_prompt"}
    finally:
        monkeypatch.undo()


def test_execute_prompt_runs_pi_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakePi()
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: fake)
    event = {
        "event_type": "ExecutionRequested",
        "workflow_run_id": "run-42",
        "event_id": "evt-9",
        "payload": {"prompt": "do the thing", "packet_id": "pkt-1", "packet_hash": "sha256:abcd"},
    }
    receipt = execute._execute(event, execute=True)
    assert receipt["status"] == "ok"
    assert fake.last_kwargs is not None
    assert fake.last_kwargs["prompt"] == "do the thing"
    assert fake.last_kwargs["execute"] is True
    binding = fake.last_kwargs["delivery_binding"]
    assert binding["run_id"] == "run-42"
    assert binding["packet_id"] == "pkt-1"
    assert binding["packet_hash"] == "sha256:abcd"
    assert binding["instruction_binding"] == "resident-workpacket-v1"


def test_execute_defaults_binding_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakePi()
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: fake)
    event = {"event_type": "WorkPacketDispatched", "event_id": "evt-5", "payload": {"prompt": "p"}}
    execute._execute(event, execute=False)
    assert fake.last_kwargs is not None
    binding = fake.last_kwargs["delivery_binding"]
    assert binding["run_id"].startswith("exec-")
    assert binding["packet_id"].startswith("packet-")
    assert binding["packet_hash"] == "sha256:0" * 4
    assert fake.last_kwargs["execute"] is False


def test_execute_instruction_keyword_used() -> None:
    monkeypatch = pytest.MonkeyPatch()
    fake = _FakePi()
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: fake)
    try:
        event = {"event_type": "ExecutionRequested", "payload": {"instruction": "do it"}}
        receipt = execute._execute(event, execute=False)
        assert receipt["status"] == "ok"
        assert fake.last_kwargs["prompt"] == "do it"
    finally:
        monkeypatch.undo()


def test_execute_exception_returns_error_with_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakePi(error=True)
    monkeypatch.setattr(execute, "_load_pi_adapter", lambda: fake)
    event = {"event_type": "ExecutionRequested", "payload": {"prompt": "boom"}}
    receipt = execute._execute(event, execute=True)
    assert receipt["error"].startswith("execution_failed: RuntimeError: boom")
    assert receipt["binding"]["instruction_binding"] == "resident-workpacket-v1"
