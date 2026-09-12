"""test_task_queue.py — Resident 异步任务队列测试 (BET-Y1Q4-T10-125).

覆盖:
  1. submit/poll/complete/fail 状态机转换
  2. 容量保护 (max_queue 溢出拒绝)
  3. 重试 (attempts < max_attempts 时重置为 queued)
  4. 终态 (attempts >= max_attempts 时标 failed)
  5. 非法状态转换拒绝 (e.g. completed → running)
  6. 并发安全 (BEGIN IMMEDIATE 锁)
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from omo.resident.task_queue import (
    DEFAULT_BACKOFF_BASE,
    DEFAULT_BACKOFF_JITTER,
    DEFAULT_BACKOFF_MAX,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_QUEUE,
    SubmitResult,
    Task,
    TaskQueue,
    TaskStatus,
)


@pytest.fixture
def q(tmp_path: Path) -> TaskQueue:
    return TaskQueue(tmp_path / "test-queue.sqlite3")


# ── 1. submit/poll/complete/fail 状态机 ──────────────────────
class TestStateMachine:
    def test_submit_returns_ok_with_task_id(self, q: TaskQueue) -> None:
        r = q.submit("bos://resident/sediment/trigger", {"path": "/data/x.md"})
        assert r.ok
        assert r.task_id
        assert r.reason == ""

    def test_poll_returns_running_tasks(self, q: TaskQueue) -> None:
        r = q.submit("bos://resident/task/submit", {"q": "卫生健康数字化"})
        tasks = q.poll()
        assert len(tasks) == 1
        assert tasks[0].id == r.task_id
        assert tasks[0].status == TaskStatus.RUNNING
        assert tasks[0].attempts == 1

    def test_complete_transitions_running_to_completed(self, q: TaskQueue) -> None:
        r = q.submit("bos://resident/task/submit", {})
        q.poll()
        ok = q.complete(r.task_id, result={"ok": True, "score": 0.95})
        assert ok
        task = q.get(r.task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.result == {"ok": True, "score": 0.95}

    def test_poll_empty_returns_empty_list(self, q: TaskQueue) -> None:
        assert q.poll() == []

    def test_poll_returns_in_creation_order(self, q: TaskQueue) -> None:
        ids = []
        for i in range(3):
            r = q.submit("bos://resident/task/submit", {"i": i})
            ids.append(r.task_id)
            time.sleep(0.001)  # 保证 created_at 严格递增
        tasks = q.poll(limit=10)
        assert [t.id for t in tasks] == ids

    def test_poll_returns_higher_priority_first(self, tmp_path: Path) -> None:
        """高优先级任务应先被 poll."""
        q = TaskQueue(tmp_path / "prio.sqlite3")
        # 先提交低优先, 再提交高优先
        r_low = q.submit("bos://resident/task/submit", {"p": 0}, priority=0)
        r_high = q.submit("bos://resident/task/submit", {"p": 10}, priority=10)
        tasks = q.poll(limit=10)
        assert [t.id for t in tasks] == [r_high.task_id, r_low.task_id]

    def test_poll_same_priority_falls_back_to_creation_order(self, tmp_path: Path) -> None:
        """同优先级时按 created_at 排序."""
        q = TaskQueue(tmp_path / "same-prio.sqlite3")
        ids = []
        for i in range(3):
            r = q.submit("bos://resident/task/submit", {"p": 5})
            ids.append(r.task_id)
            time.sleep(0.001)
        tasks = q.poll(limit=10)
        assert [t.id for t in tasks] == ids


# ── 2. 容量保护 ───────────────────────────────────────
class TestCapacityGuard:
    def test_overflow_returns_failure(self, tmp_path: Path) -> None:
        q = TaskQueue(tmp_path / "cap.sqlite3", max_queue=3)
        # 直接插 3 个 queued, 不 poll (否则会转 running 并出队列)
        for i in range(3):
            r = q.submit("bos://resident/task/submit", {"i": i})
            assert r.ok
        # 第 4 个应被拒
        r = q.submit("bos://resident/task/submit", {"i": 4})
        assert not r.ok
        assert "queue full" in r.reason

    def test_polled_tasks_free_capacity(self, tmp_path: Path) -> None:
        """poll() 把 queued → running, 但仍计入 active (queued+running)."""
        q = TaskQueue(tmp_path / "cap2.sqlite3", max_queue=2)
        for i in range(2):
            q.submit("bos://resident/task/submit", {"i": i})
        # poll 后两个都是 running, 但 active 仍 = 2
        q.poll()
        r = q.submit("bos://resident/task/submit", {})
        assert not r.ok


# ── 3. 重试机制 ───────────────────────────────────────
class TestRetry:
    def test_fail_with_low_attempts_resets_to_queued(self, tmp_path: Path) -> None:
        q = TaskQueue(tmp_path / "retry.sqlite3", max_attempts=3, backoff_base=0)
        r = q.submit("bos://resident/task/submit", {"q": "x"})
        q.poll()
        # attempts=1, 然后 fail → 重置 queued
        assert q.fail(r.task_id, "transient error")
        task = q.get(r.task_id)
        assert task is not None
        assert task.status == TaskStatus.QUEUED
        assert task.error_message == "transient error"

    def test_fail_with_max_attempts_marks_failed(self, tmp_path: Path) -> None:
        q = TaskQueue(tmp_path / "retry2.sqlite3", max_attempts=2, backoff_base=0)
        r = q.submit("bos://resident/task/submit", {})
        q.poll()  # attempts=1
        q.fail(r.task_id, "err1")  # 1 < 2 → queued
        q.poll()  # attempts=2
        q.fail(r.task_id, "err2")  # 2 >= 2 → failed
        task = q.get(r.task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert "err2" in task.error_message

    def test_error_message_truncated_to_500(self, q: TaskQueue) -> None:
        r = q.submit("bos://resident/task/submit", {})
        q.poll()
        long_err = "X" * 1000
        q.fail(r.task_id, long_err)
        task = q.get(r.task_id)
        assert task is not None
        assert len(task.error_message) == 500

    def test_fail_sets_backoff_next_attempt_at(self, tmp_path: Path) -> None:
        """fail 重试时应设置 next_attempt_at (exponential backoff)."""
        q = TaskQueue(tmp_path / "backoff.sqlite3", max_attempts=3)
        r = q.submit("bos://resident/task/submit", {})
        q.poll()  # attempts=1
        before = time.time()
        q.fail(r.task_id, "transient")
        task = q.get(r.task_id)
        assert task is not None
        assert task.status == TaskStatus.QUEUED
        assert task.next_attempt_at >= before
        assert task.next_attempt_at <= before + DEFAULT_BACKOFF_MAX

    def test_poll_skips_backoff_tasks(self, tmp_path: Path) -> None:
        """poll 应跳过处于 backoff 期的任务."""
        q = TaskQueue(tmp_path / "skip.sqlite3", max_attempts=3)
        # 提交两个任务
        r1 = q.submit("bos://resident/task/submit", {"p": 1})
        r2 = q.submit("bos://resident/task/submit", {"p": 2})
        # poll 第一个, fail 使其进入 backoff
        tasks = q.poll(limit=1)
        assert len(tasks) == 1
        q.fail(tasks[0].id, "err")
        # 再次 poll, backoff 中的任务应被跳过
        tasks = q.poll(limit=10)
        assert len(tasks) == 1
        assert tasks[0].id == r2.task_id


# ── 4. 非法状态转换 ────────────────────────────────
class TestTransitionGuards:
    def test_cannot_complete_queued_task(self, q: TaskQueue) -> None:
        """不 poll 直接 complete 应失败 (queued → completed 不合法)."""
        r = q.submit("bos://resident/task/submit", {})
        # 不 poll, 直接 complete
        assert not q.complete(r.task_id, result={})

    def test_cannot_poll_completed_task(self, q: TaskQueue) -> None:
        r = q.submit("bos://resident/task/submit", {})
        q.poll()
        q.complete(r.task_id)
        # 再 poll 应找不到 (status=completed, 不在 WHERE)
        assert q.poll() == []

    def test_cannot_complete_unknown_task(self, q: TaskQueue) -> None:
        assert not q.complete("nonexistent-id", result={})

    def test_cannot_fail_unknown_task(self, q: TaskQueue) -> None:
        assert not q.fail("nonexistent-id", "err")


# ── 5. 持久化 ─────────────────────────────────────────
class TestPersistence:
    def test_queue_survives_restart(self, tmp_path: Path) -> None:
        db = tmp_path / "persist.sqlite3"
        q1 = TaskQueue(db)
        r = q1.submit("bos://resident/task/submit", {"key": "value"})
        # 模拟 daemon 重启
        q2 = TaskQueue(db)
        task = q2.get(r.task_id)
        assert task is not None
        assert task.payload == {"key": "value"}
        assert task.status == TaskStatus.QUEUED


# ── 6. 并发安全 ───────────────────────────────────────
class TestConcurrency:
    def test_concurrent_submit_no_loss(self, tmp_path: Path) -> None:
        """10 线程 × 10 submit = 100 tasks, 无丢失."""
        db = tmp_path / "concurrent.sqlite3"
        q = TaskQueue(db, max_queue=1000)
        results: list[str] = []
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(idx: int) -> None:
            for j in range(10):
                try:
                    r = q.submit("bos://resident/task/submit", {"w": idx, "j": j})
                    if r.ok:
                        with lock:
                            results.append(r.task_id)
                    else:
                        with lock:
                            errors.append(ValueError(f"submit failed: {r.reason}"))
                except Exception as exc:  # noqa: BLE001
                    with lock:
                        errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(errors) == 0, f"errors: {errors[:3]}"
        assert len(results) == 100, f"only {len(results)} ok"
        # 唯一 ID
        assert len(set(results)) == 100

    def test_concurrent_poll_no_double_dispatch(self, tmp_path: Path) -> None:
        """5 daemon 线程同时 poll, 每个 task 只被一个线程拿到."""
        db = tmp_path / "poll.sqlite3"
        q = TaskQueue(db)
        for i in range(50):
            q.submit("bos://resident/task/submit", {"i": i})

        all_picked: list[str] = []
        lock = threading.Lock()

        def daemon() -> None:
            tasks = q.poll(limit=20)
            with lock:
                for t in tasks:
                    all_picked.append(t.id)

        threads = [threading.Thread(target=daemon) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 无重复派发 (BEGIN IMMEDIATE 保证原子 pick)
        assert len(all_picked) == len(set(all_picked))
        assert len(all_picked) == 50


# ── 7. stats ───────────────────────────────────────────
class TestStats:
    def test_initial_empty(self, q: TaskQueue) -> None:
        assert q.stats() == {"queued": 0, "running": 0, "completed": 0, "failed": 0, "expired": 0, "canceled": 0}

    def test_mixed_states(self, q: TaskQueue) -> None:
        q.submit("bos://resident/a", {})
        q.submit("bos://resident/b", {})
        q.submit("bos://resident/c", {})
        # poll(1) picks oldest (a) → running
        polled = q.poll(limit=1)
        assert len(polled) == 1
        polled_id = polled[0].id
        q.complete(polled_id)  # a → completed
        s = q.stats()
        assert s["queued"] == 2  # b, c 仍 queued
        assert s["running"] == 0  # a 已 complete
        assert s["completed"] == 1  # a


# ── 8. Constants ─────────────────────────────────────────
def test_defaults() -> None:
    assert DEFAULT_MAX_QUEUE == 1000
    assert DEFAULT_MAX_ATTEMPTS == 3


# ── 9. task_gateway CLI (T10-125 code batch: `omo resident task ...`) ──
class TestTaskCLI:
    def test_submit_status_roundtrip(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        from omo.resident import task_queue as tq

        db = str(tmp_path / "cli-queue.sqlite3")
        rc = tq.main(["submit", "--uri", "bos://resident/sediment/trigger", "--json", '{"k": 1}', "--db", db])
        assert rc == 0
        task_id = json.loads(capsys.readouterr().out)["task_id"]
        assert task_id
        rc = tq.main(["status", "--id", task_id, "--db", db])
        assert rc == 0
        shown = json.loads(capsys.readouterr().out)
        assert shown["found"] is True
        assert shown["status"] == "queued"
        assert shown["payload"] == {"k": 1}

    def test_status_missing_returns_3(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        from omo.resident import task_queue as tq

        db = str(tmp_path / "cli-missing.sqlite3")
        rc = tq.main(["status", "--id", "NOPE", "--db", db])
        assert rc == 3
        assert json.loads(capsys.readouterr().out)["found"] is False

    def test_submit_rejects_bad_payload(self, tmp_path: Path) -> None:
        from omo.resident import task_queue as tq

        db = str(tmp_path / "cli-bad.sqlite3")
        assert tq.main(["submit", "--uri", "bos://resident/x", "--json", "{bad", "--db", db]) == 2

    def test_submit_stdin_payload(
        self, tmp_path: Path, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from omo.resident import task_queue as tq

        db = str(tmp_path / "cli-stdin.sqlite3")
        monkeypatch.setattr("sys.stdin", _StdinStub('{"via": "stdin"}'))
        assert tq.main(["submit", "--uri", "bos://resident/y", "--db", db]) == 0
        assert json.loads(capsys.readouterr().out)["task_id"]

    def test_sediment_async_enqueues(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        from omo.resident import sediment as _sed

        db = str(tmp_path / "cli-sed.sqlite3")
        rc = _sed.main(["--async", "--json", '{"event_type": "T"}', "--db", db])
        assert rc == 0
        out = json.loads(capsys.readouterr().out)
        assert out["queued"] is True and out["task_id"]
        assert TaskQueue(db).get(out["task_id"]).uri == "bos://resident/sediment/trigger"

    def test_decision_async_enqueues(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        from omo.resident import decision as _dec

        db = str(tmp_path / "cli-dec.sqlite3")
        rc = _dec.main(["--async", "--json", '{"event_type": "T"}', "--db", db])
        assert rc == 0
        out = json.loads(capsys.readouterr().out)
        assert out["queued"] is True and out["task_id"]
        assert TaskQueue(db).get(out["task_id"]).uri == "bos://resident/decision/trigger"


# ── 10. TTL 与过期 ─────────────────────────────────────
class TestTTL:
    def test_submit_with_ttl_sets_expires_at(self, tmp_path: Path) -> None:
        q = TaskQueue(tmp_path / "ttl.sqlite3")
        before = time.time()
        r = q.submit("bos://resident/x", {}, ttl=60)
        assert r.ok
        task = q.get(r.task_id)
        assert task is not None
        assert task.expires_at >= before + 60
        assert task.expires_at <= before + 61

    def test_submit_without_ttl_never_expires(self, tmp_path: Path) -> None:
        q = TaskQueue(tmp_path / "no-ttl.sqlite3")
        r = q.submit("bos://resident/x", {})
        assert r.ok
        task = q.get(r.task_id)
        assert task is not None
        assert task.expires_at == 0.0

    def test_poll_skips_expired_tasks(self, tmp_path: Path) -> None:
        q = TaskQueue(tmp_path / "skip-expired.sqlite3")
        r_expired = q.submit("bos://resident/x", {"k": 1}, ttl=-1)  # 已过期
        r_ok = q.submit("bos://resident/y", {"k": 2})
        tasks = q.poll(limit=10)
        assert len(tasks) == 1
        assert tasks[0].id == r_ok.task_id

    def test_purge_expired_marks_expired(self, tmp_path: Path) -> None:
        q = TaskQueue(tmp_path / "purge.sqlite3")
        r1 = q.submit("bos://resident/x", {"k": 1}, ttl=-1)
        r2 = q.submit("bos://resident/y", {"k": 2}, ttl=-1)
        count = q.purge_expired()
        assert count == 2
        assert q.get(r1.task_id).status == TaskStatus.EXPIRED
        assert q.get(r2.task_id).status == TaskStatus.EXPIRED


# ── 11. 取消任务 ─────────────────────────────────────
class TestCancel:
    def test_cancel_queued_task(self, tmp_path: Path) -> None:
        q = TaskQueue(tmp_path / "cancel.sqlite3")
        r = q.submit("bos://resident/x", {})
        assert q.cancel(r.task_id) is True
        assert q.get(r.task_id).status == TaskStatus.CANCELED

    def test_cancel_running_task_fails(self, tmp_path: Path) -> None:
        q = TaskQueue(tmp_path / "cancel-run.sqlite3")
        r = q.submit("bos://resident/x", {})
        q.poll()
        assert q.cancel(r.task_id) is False

    def test_cancel_unknown_task_fails(self, tmp_path: Path) -> None:
        q = TaskQueue(tmp_path / "cancel-unk.sqlite3")
        assert q.cancel("nonexistent") is False


# ── 12. list / metrics CLI ───────────────────────────
class TestListAndMetrics:
    def test_list_empty(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        from omo.resident import task_queue as tq

        db = str(tmp_path / "list-empty.sqlite3")
        assert tq.main(["list", "--db", db]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["count"] == 0
        assert out["tasks"] == []

    def test_list_filters_by_status(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        from omo.resident import task_queue as tq

        db = str(tmp_path / "list-filter.sqlite3")
        q = TaskQueue(db)
        r1 = q.submit("bos://resident/a", {})
        q.poll()
        q.complete(r1.task_id, result={"ok": True})
        r2 = q.submit("bos://resident/b", {})
        assert tq.main(["list", "--status", "completed", "--db", db]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["count"] == 1
        assert out["tasks"][0]["id"] == r1.task_id
        assert tq.main(["list", "--status", "queued", "--db", db]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["count"] == 1
        assert out["tasks"][0]["id"] == r2.task_id

    def test_list_filters_by_labels(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        from omo.resident import task_queue as tq

        db = str(tmp_path / "list-labels.sqlite3")
        q = TaskQueue(db)
        r1 = q.submit("bos://resident/a", {}, labels=["urgent", "vip"])
        r2 = q.submit("bos://resident/b", {}, labels=["normal"])
        r3 = q.submit("bos://resident/c", {}, labels=["urgent"])
        assert tq.main(["list", "--label", "urgent", "--db", db]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["count"] == 2
        ids = {t["id"] for t in out["tasks"]}
        assert ids == {r1.task_id, r3.task_id}
        assert tq.main(["list", "--label", "urgent", "--label", "vip", "--db", db]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["count"] == 1
        assert out["tasks"][0]["id"] == r1.task_id

    def test_metrics_counts(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        from omo.resident import task_queue as tq

        db = str(tmp_path / "metrics.sqlite3")
        q = TaskQueue(db, max_attempts=1)
        r1 = q.submit("bos://resident/a", {})
        q.poll()
        q.complete(r1.task_id, result={"ok": True})
        r2 = q.submit("bos://resident/b", {})
        q.poll()
        q.fail(r2.task_id, "err")
        assert tq.main(["metrics", "--db", db]) == 0
        m = json.loads(capsys.readouterr().out)
        assert m["total"] == 2
        assert m["completed"] == 1
        assert m["failed"] == 1
        assert m["retried"] == 0
        assert m["success_rate"] == 0.5

    def test_search_by_payload(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        from omo.resident import task_queue as tq

        db = str(tmp_path / "search-payload.sqlite3")
        q = TaskQueue(db)
        r1 = q.submit("bos://resident/a", {"action": "deploy"})
        r2 = q.submit("bos://resident/b", {"action": "build"})
        assert tq.main(["search", "--query", "deploy", "--field", "payload", "--db", db]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["count"] == 1
        assert out["tasks"][0]["id"] == r1.task_id

    def test_search_by_result(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        from omo.resident import task_queue as tq

        db = str(tmp_path / "search-result.sqlite3")
        q = TaskQueue(db)
        r1 = q.submit("bos://resident/a", {})
        q.poll()
        q.complete(r1.task_id, result={"status": "success"})
        assert tq.main(["search", "--query", "success", "--field", "result", "--db", db]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["count"] == 1
        assert out["tasks"][0]["id"] == r1.task_id

    def test_search_by_error(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        from omo.resident import task_queue as tq

        db = str(tmp_path / "search-error.sqlite3")
        q = TaskQueue(db)
        r1 = q.submit("bos://resident/a", {})
        q.poll()
        q.fail(r1.task_id, "timeout exceeded")
        assert tq.main(["search", "--query", "timeout", "--field", "error", "--db", db]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["count"] == 1
        assert out["tasks"][0]["id"] == r1.task_id


class _StdinStub:
    """最小 stdin 桩 (read() 一次性返回固定文本)."""

    def __init__(self, text: str) -> None:
        self._text = text

    def read(self) -> str:
        return self._text
