#!/usr/bin/env python3
"""task_queue — Resident 异步任务就绪队列与状态机 (BET-Y1Q4-T10-125).

常驻 daemon 在每次 tick 中扫描 queued 任务、派发到 handler、持久化状态机。
不引入 Redis 或外部 MQ — 依托 SQLite 单一文件 + 原子事务实现。

状态机: queued → running → completed | failed
- queued: 入队等待 daemon 拉取
- running: daemon 已 pick 但尚未完成（防重复派发）
- completed: handler 成功执行完毕
- failed: handler 抛异常或超过 max_attempts

队列容量: 默认 1000, 溢出拒绝并 log warning (不阻塞外部 submit).
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterator


class TaskStatus(str, Enum):
    """任务状态机 enum.

    严格单向: queued → running → completed | failed
    """

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# 状态转换合法性表 (T10-125: 禁止反向与跳跃)
_ALLOWED: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.QUEUED: {TaskStatus.RUNNING, TaskStatus.FAILED},
    TaskStatus.RUNNING: {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.QUEUED},
    TaskStatus.COMPLETED: set(),
    TaskStatus.FAILED: {TaskStatus.QUEUED},  # 失败可重试 (重新入队)
}


DEFAULT_MAX_QUEUE = 1000
DEFAULT_MAX_ATTEMPTS = 3


@dataclass
class Task:
    """任务条目.

    payload JSON-serializable. result 与 error_message 在终态填充.
    """

    id: str
    uri: str  # 触发该任务的 BOS URI (e.g. "bos://resident/sediment/trigger")
    payload: dict[str, Any]
    status: TaskStatus
    attempts: int = 0
    result: Any = None
    error_message: str = ""
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class SubmitResult:
    """submit() 返回值."""

    ok: bool
    task_id: str = ""
    reason: str = ""


class TaskQueue:
    """SQLite-backed 任务队列.

    单文件持久化 (默认 runtime/omo/resident-task-queue.sqlite3),
    原子事务保证 status 转换的原子性.
    """

    def __init__(
        self,
        db_path: Path,
        *,
        max_queue: int = DEFAULT_MAX_QUEUE,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.max_queue = max_queue
        self.max_attempts = max_attempts
        self._init_schema()

    # ── Schema ─────────────────────────────────────
    def _init_schema(self) -> None:
        with self._conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS resident_tasks (
                    id TEXT PRIMARY KEY,
                    uri TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    result TEXT,
                    error_message TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON resident_tasks(status, created_at)")

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, isolation_level=None)  # autocommit; we use BEGIN explicitly
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            yield conn
        finally:
            conn.close()

    # ── Public API ─────────────────────────────────
    def submit(self, uri: str, payload: dict[str, Any]) -> SubmitResult:
        """提交任务到队列. 队列满时拒绝.

        返回 SubmitResult: ok=True 时 task_id 非空.
        """
        # 容量检查 (placed in事务外避免对 hot row 加锁)
        with self._conn() as c:
            cur = c.execute(
                "SELECT COUNT(*) FROM resident_tasks WHERE status IN (?, ?)",
                (TaskStatus.QUEUED.value, TaskStatus.RUNNING.value),
            )
            active = cur.fetchone()[0]
        if active >= self.max_queue:
            return SubmitResult(ok=False, reason=f"queue full ({active}/{self.max_queue})")

        task_id = uuid.uuid4().hex
        now = time.time()
        with self._conn() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                c.execute(
                    "INSERT INTO resident_tasks (id, uri, payload, status, attempts, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, 0, ?, ?)",
                    (
                        task_id,
                        uri,
                        json.dumps(payload, ensure_ascii=False),
                        TaskStatus.QUEUED.value,
                        now,
                        now,
                    ),
                )
                c.execute("COMMIT")
            except Exception:
                c.execute("ROLLBACK")
                raise
        return SubmitResult(ok=True, task_id=task_id)

    def poll(self, limit: int = 10) -> list[Task]:
        """拉取并标记为 running (原子: pick 一个就 lock 住, 防多 daemon 并发).

        返回 running 状态任务列表 (待 handler 执行).
        """
        tasks: list[Task] = []
        with self._conn() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                rows = c.execute(
                    "SELECT id, uri, payload, status, attempts, result, error_message, created_at, updated_at "
                    "FROM resident_tasks WHERE status = ? ORDER BY created_at ASC LIMIT ?",
                    (TaskStatus.QUEUED.value, limit),
                ).fetchall()
                for r in rows:
                    new_attempts = r["attempts"] + 1
                    c.execute(
                        "UPDATE resident_tasks SET status = ?, attempts = ?, updated_at = ? WHERE id = ?",
                        (TaskStatus.RUNNING.value, new_attempts, time.time(), r["id"]),
                    )
                    tasks.append(
                        self._row_to_task(r, status_override=TaskStatus.RUNNING, attempts_override=new_attempts)
                    )
                c.execute("COMMIT")
            except Exception:
                c.execute("ROLLBACK")
                raise
        return tasks

    def complete(self, task_id: str, result: Any = None) -> bool:
        """标记任务完成. 返回是否成功转换."""
        return self._transition(task_id, TaskStatus.COMPLETED, result=result)

    def fail(self, task_id: str, error_message: str) -> bool:
        """标记任务失败. 若 attempts < max_attempts 则重置为 queued (重试)."""
        with self._conn() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute("SELECT status, attempts FROM resident_tasks WHERE id = ?", (task_id,)).fetchone()
                if row is None:
                    c.execute("ROLLBACK")
                    return False
                if row["status"] != TaskStatus.RUNNING.value:
                    c.execute("ROLLBACK")
                    return False
                if row["attempts"] < self.max_attempts:
                    # 重试: 重置为 queued
                    c.execute(
                        "UPDATE resident_tasks SET status = ?, error_message = ?, updated_at = ? WHERE id = ?",
                        (
                            TaskStatus.QUEUED.value,
                            error_message[:500],
                            time.time(),
                            task_id,
                        ),
                    )
                else:
                    # 终态失败
                    c.execute(
                        "UPDATE resident_tasks SET status = ?, error_message = ?, updated_at = ? WHERE id = ?",
                        (
                            TaskStatus.FAILED.value,
                            error_message[:500],
                            time.time(),
                            task_id,
                        ),
                    )
                c.execute("COMMIT")
                return True
            except Exception:
                c.execute("ROLLBACK")
                raise

    def get(self, task_id: str) -> Task | None:
        """查询任务当前状态."""
        with self._conn() as c:
            row = c.execute(
                "SELECT id, uri, payload, status, attempts, result, error_message, created_at, updated_at "
                "FROM resident_tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
        return self._row_to_task(row) if row else None

    def stats(self) -> dict[str, int]:
        """返回队列统计: 各状态任务数."""
        with self._conn() as c:
            rows = c.execute("SELECT status, COUNT(*) FROM resident_tasks GROUP BY status").fetchall()
        out = {s.value: 0 for s in TaskStatus}
        for r in rows:
            # SQLite Row index access: r[0] = status, r[1] = count
            status_str = r[0] if isinstance(r[0], str) else str(r[0])
            count = r[1]
            if status_str in out:
                out[status_str] = count
        return out

    # ── Internal ──────────────────────────────────
    def _transition(
        self,
        task_id: str,
        target: TaskStatus,
        *,
        result: Any = None,
    ) -> bool:
        """通用状态转换. 检查 _ALLOWED 表后原子更新."""
        with self._conn() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute("SELECT status FROM resident_tasks WHERE id = ?", (task_id,)).fetchone()
                if row is None:
                    c.execute("ROLLBACK")
                    return False
                current = TaskStatus(row["status"])
                if target not in _ALLOWED[current]:
                    c.execute("ROLLBACK")
                    return False
                result_json = json.dumps(result, ensure_ascii=False) if result is not None else None
                c.execute(
                    "UPDATE resident_tasks SET status = ?, result = COALESCE(?, result), updated_at = ? WHERE id = ?",
                    (target.value, result_json, time.time(), task_id),
                )
                c.execute("COMMIT")
                return True
            except Exception:
                c.execute("ROLLBACK")
                raise

    def _row_to_task(
        self,
        row: sqlite3.Row | None,
        *,
        status_override: TaskStatus | None = None,
        attempts_override: int | None = None,
    ) -> Task:
        if row is None:
            raise ValueError("row is None")
        status = status_override if status_override is not None else TaskStatus(row["status"])
        attempts = attempts_override if attempts_override is not None else row["attempts"]
        # sqlite3.Row 可能不支持 __contains__, 改用 keys() 检查
        result_raw = row["result"] if "result" in row.keys() else None
        return Task(
            id=row["id"],
            uri=row["uri"],
            payload=json.loads(row["payload"]) if row["payload"] else {},
            status=status,
            attempts=attempts,
            result=json.loads(result_raw) if result_raw else None,
            error_message=row["error_message"] or "",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


__all__ = (
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_QUEUE",
    "SubmitResult",
    "Task",
    "TaskQueue",
    "TaskStatus",
)
