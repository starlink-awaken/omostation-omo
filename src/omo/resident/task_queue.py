#!/usr/bin/env python3
"""task_queue — Resident 异步任务就绪队列与状态机 (BET-Y1Q4-T10-125).

常驻 daemon 在每次 tick 中扫描 queued 任务、派发到 handler、持久化状态机。
不引入 Redis 或外部 MQ — 依托 SQLite 单一文件 + 原子事务实现。

状态机: queued → running → completed | failed | expired | canceled
- queued: 入队等待 daemon 拉取
- running: daemon 已 pick 但尚未完成（防重复派发）
- completed: handler 成功执行完毕
- failed: handler 抛异常或超过 max_attempts
- expired: 超过 TTL 未处理
- canceled: 用户主动取消

队列容量: 默认 1000, 溢出拒绝并 log warning (不阻塞外部 submit).

改进 (T10-125 wave 2):
- 优先级队列: submit 时指定 priority (默认 0, 高优先任务先被 poll)
- 指数退避: fail 重试时根据 attempts 计算 backoff delay, 避免 thundering herd
- 任务过期: submit 时可指定 ttl 秒数, 超时未处理自动 expired
- 主动取消: cancel() 允许取消 queued 任务
"""

from __future__ import annotations

import json
import math
import random
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

    严格单向: queued → running → completed | failed | expired | canceled
    """

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
    CANCELED = "canceled"


# 状态转换合法性表 (T10-125: 禁止反向与跳跃)
_ALLOWED: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.QUEUED: {TaskStatus.RUNNING, TaskStatus.FAILED, TaskStatus.EXPIRED, TaskStatus.CANCELED},
    TaskStatus.RUNNING: {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.QUEUED},
    TaskStatus.COMPLETED: set(),
    TaskStatus.FAILED: {TaskStatus.QUEUED},  # 失败可重试 (重新入队)
    TaskStatus.EXPIRED: set(),
    TaskStatus.CANCELED: set(),
}


DEFAULT_MAX_QUEUE = 1000
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_BASE = 1.0
DEFAULT_BACKOFF_MAX = 60.0
DEFAULT_BACKOFF_JITTER = 0.25


@dataclass
class Task:
    """任务条目.

    payload JSON-serializable. result 与 error_message 在终态填充.
    priority: 优先级 (默认 0, 高优先任务先被 poll).
    next_attempt_at: 下次允许尝试的 Unix timestamp (用于指数退避).
    expires_at: 过期时间戳 (0 表示永不过期).
    labels: 任务标签列表 (用于过滤和组织).
    """

    id: str
    uri: str  # 触发该任务的 BOS URI (e.g. "bos://resident/sediment/trigger")
    payload: dict[str, Any]
    status: TaskStatus
    attempts: int = 0
    result: Any = None
    error_message: str = ""
    priority: int = 0
    next_attempt_at: float = 0.0
    expires_at: float = 0.0
    labels: list[str] = field(default_factory=list)
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
        backoff_base: float = DEFAULT_BACKOFF_BASE,
        backoff_max: float = DEFAULT_BACKOFF_MAX,
        backoff_jitter: float = DEFAULT_BACKOFF_JITTER,
        default_ttl: float = 0.0,
    ) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.max_queue = max_queue
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.backoff_jitter = backoff_jitter
        self.default_ttl = default_ttl
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
                    priority INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL DEFAULT 0,
                    expires_at REAL NOT NULL DEFAULT 0,
                    labels TEXT NOT NULL DEFAULT '[]',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status_priority ON resident_tasks(status, priority DESC, created_at ASC)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_next_attempt ON resident_tasks(next_attempt_at) WHERE status = 'queued'")
            c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_expires_at ON resident_tasks(expires_at) WHERE status = 'queued' AND expires_at > 0")

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
    def submit(self, uri: str, payload: dict[str, Any], *, priority: int = 0, ttl: float = 0.0, labels: list[str] | None = None) -> SubmitResult:
        """提交任务到队列. 队列满时拒绝.

        返回 SubmitResult: ok=True 时 task_id 非空.
        priority: 优先级 (默认 0, 高优先任务先被 poll).
        ttl: 任务存活时间(秒), 0 表示永不过期.
        labels: 任务标签列表 (用于过滤和组织).
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
        expires_at = now + ttl if ttl != 0 else 0.0
        labels_json = json.dumps(labels or [], ensure_ascii=False)
        with self._conn() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                c.execute(
                    "INSERT INTO resident_tasks (id, uri, payload, status, attempts, priority, next_attempt_at, expires_at, labels, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, 0, ?, 0, ?, ?, ?, ?)",
                    (
                        task_id,
                        uri,
                        json.dumps(payload, ensure_ascii=False),
                        TaskStatus.QUEUED.value,
                        priority,
                        expires_at,
                        labels_json,
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
        按 priority DESC, created_at ASC 排序 (高优先任务先执行).
        跳过处于 backoff 期或已过期的任务.
        """
        tasks: list[Task] = []
        with self._conn() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                rows = c.execute(
                    "SELECT id, uri, payload, status, attempts, result, error_message, priority, next_attempt_at, expires_at, created_at, updated_at "
                    "FROM resident_tasks "
                    "WHERE status = ? AND next_attempt_at <= ? AND (expires_at = 0 OR expires_at > ?) "
                    "ORDER BY priority DESC, created_at ASC LIMIT ?",
                    (TaskStatus.QUEUED.value, time.time(), time.time(), limit),
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
        """标记任务失败. 若 attempts < max_attempts 则重置为 queued (重试).

        重试时设置 exponential backoff: delay = min(base * 2^attempts + jitter, max).
        """
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
                    # 重试: 计算 exponential backoff + jitter
                    delay = self._backoff_delay(row["attempts"])
                    next_attempt = time.time() + delay
                    c.execute(
                        "UPDATE resident_tasks SET status = ?, error_message = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?",
                        (
                            TaskStatus.QUEUED.value,
                            error_message[:500],
                            next_attempt,
                            time.time(),
                            task_id,
                        ),
                    )
                else:
                    # 终态失败
                    c.execute(
                        "UPDATE resident_tasks SET status = ?, error_message = ?, next_attempt_at = 0, updated_at = ? WHERE id = ?",
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

    def _backoff_delay(self, attempts: int) -> float:
        """计算 exponential backoff delay (秒).

        delay = min(base * 2^attempts + uniform_jitter, max).
        """
        delay = self.backoff_base * (2 ** attempts)
        delay = min(delay, self.backoff_max)
        jitter = random.uniform(0, self.backoff_jitter * delay)  # noqa: S311
        return delay + jitter

    def get(self, task_id: str) -> Task | None:
        """查询任务当前状态."""
        with self._conn() as c:
            row = c.execute(
                "SELECT id, uri, payload, status, attempts, result, error_message, priority, next_attempt_at, expires_at, labels, created_at, updated_at "
                "FROM resident_tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
        return self._row_to_task(row) if row else None

    def cancel(self, task_id: str) -> bool:
        """取消 queued 任务. 返回是否成功."""
        with self._conn() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute("SELECT status FROM resident_tasks WHERE id = ?", (task_id,)).fetchone()
                if row is None or row["status"] != TaskStatus.QUEUED.value:
                    c.execute("ROLLBACK")
                    return False
                c.execute(
                    "UPDATE resident_tasks SET status = ?, updated_at = ? WHERE id = ?",
                    (TaskStatus.CANCELED.value, time.time(), task_id),
                )
                c.execute("COMMIT")
                return True
            except Exception:
                c.execute("ROLLBACK")
                raise

    def retry(self, task_id: str) -> bool:
        """将 failed 任务重新入队. 返回是否成功."""
        with self._conn() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute(
                    "SELECT status, attempts FROM resident_tasks WHERE id = ?", (task_id,)
                ).fetchone()
                if row is None or row["status"] != TaskStatus.FAILED.value:
                    c.execute("ROLLBACK")
                    return False
                now = time.time()
                c.execute(
                    "UPDATE resident_tasks SET status = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?",
                    (TaskStatus.QUEUED.value, 0.0, now, task_id),
                )
                c.execute("COMMIT")
                return True
            except Exception:
                c.execute("ROLLBACK")
                raise

    def purge_expired(self, older_than: float = 0.0) -> int:
        """将超时未处理的 queued 任务标记为 expired.
        
        older_than: 仅处理 expires_at <= older_than 的任务 (默认 0 = 所有超时任务).
        返回被标记为 expired 的任务数.
        """
        now = time.time()
        with self._conn() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                if older_than > 0:
                    rows = c.execute(
                        "SELECT id FROM resident_tasks WHERE status = ? AND expires_at > 0 AND expires_at <= ?",
                        (TaskStatus.QUEUED.value, older_than),
                    ).fetchall()
                else:
                    rows = c.execute(
                        "SELECT id FROM resident_tasks WHERE status = ? AND expires_at > 0 AND expires_at <= ?",
                        (TaskStatus.QUEUED.value, now),
                    ).fetchall()
                for r in rows:
                    c.execute(
                        "UPDATE resident_tasks SET status = ?, updated_at = ? WHERE id = ?",
                        (TaskStatus.EXPIRED.value, now, r["id"]),
                    )
                c.execute("COMMIT")
                return len(rows)
            except Exception:
                c.execute("ROLLBACK")
                raise

    def stats(self) -> dict[str, int]:
        """返回队列统计: 各状态任务数."""
        with self._conn() as c:
            rows = c.execute("SELECT status, COUNT(*) FROM resident_tasks GROUP BY status").fetchall()
        out = {s.value: 0 for s in TaskStatus}
        for r in rows:
            status_str = r[0] if isinstance(r[0], str) else str(r[0])
            count = r[1]
            if status_str in out:
                out[status_str] = count
        return out

    def metrics(self) -> dict[str, Any]:
        """返回队列指标: 吞吐量、延迟分布、重试率."""
        with self._conn() as c:
            total = c.execute("SELECT COUNT(*) FROM resident_tasks").fetchone()[0]
            completed = c.execute("SELECT COUNT(*) FROM resident_tasks WHERE status = ?", (TaskStatus.COMPLETED.value,)).fetchone()[0]
            failed = c.execute("SELECT COUNT(*) FROM resident_tasks WHERE status = ?", (TaskStatus.FAILED.value,)).fetchone()[0]
            retried = c.execute("SELECT COUNT(*) FROM resident_tasks WHERE attempts > 1").fetchone()[0]
            avg_duration = c.execute(
                "SELECT AVG(updated_at - created_at) FROM resident_tasks WHERE status IN (?, ?)",
                (TaskStatus.COMPLETED.value, TaskStatus.FAILED.value),
            ).fetchone()[0]
        return {
            "total": total,
            "completed": completed,
            "failed": failed,
            "retried": retried,
            "retry_rate": retried / total if total else 0.0,
            "success_rate": completed / total if total else 0.0,
            "avg_duration_s": avg_duration or 0.0,
        }

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
        result_raw = row["result"] if "result" in row.keys() else None
        priority = row["priority"] if "priority" in row.keys() else 0
        next_attempt_at = row["next_attempt_at"] if "next_attempt_at" in row.keys() else 0.0
        expires_at = row["expires_at"] if "expires_at" in row.keys() else 0.0
        labels_raw = row["labels"] if "labels" in row.keys() else "[]"
        labels = json.loads(labels_raw) if labels_raw else []
        if not isinstance(labels, list):
            labels = []
        return Task(
            id=row["id"],
            uri=row["uri"],
            payload=json.loads(row["payload"]) if row["payload"] else {},
            status=status,
            attempts=attempts,
            result=json.loads(result_raw) if result_raw else None,
            error_message=row["error_message"] or "",
            priority=priority,
            next_attempt_at=next_attempt_at,
            expires_at=expires_at,
            labels=labels,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


__all__ = (
    "DEFAULT_BACKOFF_BASE",
    "DEFAULT_BACKOFF_JITTER",
    "DEFAULT_BACKOFF_MAX",
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_QUEUE",
    "SubmitResult",
    "Task",
    "TaskQueue",
    "TaskStatus",
    "default_db_path",
)


def default_db_path() -> Path:
    """默认队列 DB 路径 (与 daemon tick 共用同一文件).

    与 daemon.py `_process_task_queue` 同源, CLI 与守护进程读写同一 SQLite 文件.
    """
    from omo.resident import WORKSPACE  # noqa: PLC0415 - 延迟导入, 与 daemon 保持同解析

    return WORKSPACE / "runtime" / "omo" / "resident-task-queue.sqlite3"


def _task_to_json(task: Task) -> dict[str, Any]:
    return {
        "id": task.id,
        "uri": task.uri,
        "payload": task.payload,
        "status": task.status.value,
        "attempts": task.attempts,
        "result": task.result,
        "error_message": task.error_message,
        "priority": task.priority,
        "next_attempt_at": task.next_attempt_at,
        "expires_at": task.expires_at,
        "labels": task.labels,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    }


def main(argv: list[str] | None = None) -> int:
    """CLI: `omo resident task submit|status` (T10-125 task_gateway 执行面).

    - submit: `task submit --uri <bos-uri> [--json <payload>] [--db <path>]`
      payload 缺省时读 stdin (空输入视为 {}); 队列满时 exit 2 并输出 reason.
    - status: `task status --id <task-id> [--db <path>] [--json <{"id":...}>]`
      输出任务 JSON (含 found 标志); 不存在时 exit 3.
    """
    import argparse  # noqa: PLC0415
    import sys  # noqa: PLC0415

    parser = argparse.ArgumentParser(prog="omo resident task", description=__doc__)
    parser.add_argument(
        "--db", default=None, help="队列 sqlite 路径 (默认Workspace runtime/omo/resident-task-queue.sqlite3)"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p_submit = sub.add_parser("submit", help="提交任务入队")
    p_submit.add_argument("--uri", required=True, help="目标 BOS URI (由 daemon 按 URI 分发)")
    p_submit.add_argument("--json", default=None, help="payload JSON 字符串 (缺省读 stdin)")
    p_submit.add_argument("--priority", type=int, default=0, help="任务优先级 (默认 0, 高优先先执行)")
    p_submit.add_argument("--ttl", type=float, default=0, help="任务 TTL(秒), 0=永不过期")
    p_submit.add_argument("--label", action="append", default=[], help="任务标签 (可多次指定)")
    p_submit.add_argument(
        "--db", default=None, help="队列 sqlite 路径 (默认Workspace runtime/omo/resident-task-queue.sqlite3)"
    )
    p_status = sub.add_parser("status", help="查询任务状态")
    p_status.add_argument("--id", default=None, help="任务 id")
    p_status.add_argument("--json", default=None, help='请求 JSON 字符串 (如 {"id": "<task-id>"})')
    p_status.add_argument(
        "--db", default=None, help="队列 sqlite 路径 (默认Workspace runtime/omo/resident-task-queue.sqlite3)"
    )
    p_list = sub.add_parser("list", help="列出队列任务 (支持 --status/--uri/--label 过滤)")
    p_list.add_argument("--status", default=None, help="按状态过滤 (queued/running/completed/failed/expired/canceled)")
    p_list.add_argument("--uri", default=None, help="按 URI 前缀过滤")
    p_list.add_argument("--label", action="append", default=[], help="按标签过滤 (可多次指定, 任务需包含所有指定标签)")
    p_list.add_argument("--limit", type=int, default=50, help="最多返回条数 (默认 50)")
    p_list.add_argument("--watch", action="store_true", help="实时监控模式 (每 2 秒刷新)")
    p_list.add_argument(
        "--db", default=None, help="队列 sqlite 路径 (默认Workspace runtime/omo/resident-task-queue.sqlite3)"
    )
    p_metrics = sub.add_parser("metrics", help="队列指标 (吞吐量/成功率/重试率/平均耗时)")
    p_metrics.add_argument(
        "--db", default=None, help="队列 sqlite 路径 (默认Workspace runtime/omo/resident-task-queue.sqlite3)"
    )
    p_search = sub.add_parser("search", help="搜索任务 (按 payload/result/error_message 内容)")
    p_search.add_argument("--query", required=True, help="搜索关键词 (大小写不敏感)")
    p_search.add_argument("--field", default="all", help="搜索字段: payload/result/error/all (默认 all)")
    p_search.add_argument("--limit", type=int, default=50, help="最多返回条数 (默认 50)")
    p_search.add_argument(
        "--db", default=None, help="队列 sqlite 路径 (默认Workspace runtime/omo/resident-task-queue.sqlite3)"
    )
    p_cancel = sub.add_parser("cancel", help="取消 queued 任务")
    p_cancel.add_argument("--id", required=True, help="任务 id")
    p_cancel.add_argument(
        "--db", default=None, help="队列 sqlite 路径 (默认Workspace runtime/omo/resident-task-queue.sqlite3)"
    )
    p_retry = sub.add_parser("retry", help="重试 failed 任务 (重新入队)")
    p_retry.add_argument("--id", required=True, help="任务 id")
    p_retry.add_argument(
        "--db", default=None, help="队列 sqlite 路径 (默认Workspace runtime/omo/resident-task-queue.sqlite3)"
    )
    args = parser.parse_args(argv)

    # --db 可放顶层 (task --db X submit ...) 或子命令级 (task submit --db X ...), 同 dest
    db_path = Path(args.db) if args.db else default_db_path()
    queue = TaskQueue(db_path)

    if args.command == "submit":
        raw = args.json if args.json is not None else sys.stdin.read()
        try:
            payload = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError as exc:
            print(json.dumps({"ok": False, "reason": f"invalid payload JSON: {exc}"}), flush=True)
            return 2
        if not isinstance(payload, dict):
            print(json.dumps({"ok": False, "reason": "payload must be a JSON object"}), flush=True)
            return 2
        result = queue.submit(args.uri, payload, priority=args.priority, ttl=args.ttl, labels=args.label)
        if result.ok:
            print(json.dumps({"ok": True, "task_id": result.task_id}), flush=True)
            return 0
        print(json.dumps({"ok": False, "reason": result.reason}), flush=True)
        return 2

    if args.command == "list":
        status_filter = args.status
        uri_prefix = args.uri
        label_filters = args.label or []
        watch = args.watch

        def _fetch_tasks() -> list[Task]:
            with queue._conn() as c:
                query = "SELECT id, uri, payload, status, attempts, result, error_message, priority, next_attempt_at, expires_at, labels, created_at, updated_at FROM resident_tasks"
                params: list[Any] = []
                where: list[str] = []
                if status_filter:
                    where.append("status = ?")
                    params.append(status_filter)
                if uri_prefix:
                    where.append("uri LIKE ?")
                    params.append(f"{uri_prefix}%")
                if where:
                    query += " WHERE " + " AND ".join(where)
                query += " ORDER BY created_at DESC LIMIT ?"
                params.append(args.limit)
                rows = c.execute(query, params).fetchall()
            tasks = [queue._row_to_task(r) for r in rows]
            if label_filters:
                tasks = [t for t in tasks if all(label in t.labels for label in label_filters)]
            return tasks

        if watch:
            try:
                import time as _time
                import sys as _sys

                last_count = -1
                while True:
                    tasks = _fetch_tasks()
                    count = len(tasks)
                    if count != last_count:
                        print(json.dumps({"tasks": [_task_to_json(t) for t in tasks], "count": count}, ensure_ascii=False), flush=True)
                        last_count = count
                    _time.sleep(2)
            except KeyboardInterrupt:
                return 0
        tasks = _fetch_tasks()
        print(json.dumps({"tasks": [_task_to_json(t) for t in tasks], "count": len(tasks)}, ensure_ascii=False), flush=True)
        return 0

    if args.command == "metrics":
        m = queue.metrics()
        print(json.dumps(m, ensure_ascii=False), flush=True)
        return 0

    if args.command == "search":
        query = args.query.strip().lower()
        field = args.field.lower()
        if not query:
            print(json.dumps({"tasks": [], "count": 0, "query": query}), flush=True)
            return 0
        with queue._conn() as c:
            rows = c.execute(
                "SELECT id, uri, payload, status, attempts, result, error_message, priority, next_attempt_at, expires_at, labels, created_at, updated_at FROM resident_tasks"
            ).fetchall()
        matched: list[Task] = []
        for r in rows:
            task = queue._row_to_task(r)
            haystack = ""
            if field in ("payload", "all"):
                haystack += json.dumps(task.payload, ensure_ascii=False).lower()
            if field in ("result", "all"):
                haystack += json.dumps(task.result, ensure_ascii=False).lower()
            if field in ("error", "error_message", "all"):
                haystack += task.error_message.lower()
            if query in haystack:
                matched.append(task)
            if len(matched) >= args.limit:
                break
        print(json.dumps({"tasks": [_task_to_json(t) for t in matched], "count": len(matched), "query": query}, ensure_ascii=False), flush=True)
        return 0

    if args.command == "cancel":
        ok = queue.cancel(args.id)
        if ok:
            print(json.dumps({"ok": True, "task_id": args.id, "status": "canceled"}), flush=True)
            return 0
        print(json.dumps({"ok": False, "reason": "task not found or not queued", "task_id": args.id}), flush=True)
        return 2

    if args.command == "retry":
        ok = queue.retry(args.id)
        if ok:
            print(json.dumps({"ok": True, "task_id": args.id, "status": "queued"}), flush=True)
            return 0
        print(json.dumps({"ok": False, "reason": "task not found or not failed", "task_id": args.id}), flush=True)
        return 2

    task_id = args.id
    if task_id is None and args.json:
        try:
            task_id = json.loads(args.json).get("id")
        except json.JSONDecodeError:
            task_id = None
    if not task_id:
        print(json.dumps({"found": False, "reason": "missing --id"}), flush=True)
        return 2
    task = queue.get(task_id)
    if task is None:
        print(json.dumps({"found": False, "id": task_id}), flush=True)
        return 3
    print(json.dumps({"found": True, **_task_to_json(task)}, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    import sys  # noqa: PLC0415

    sys.exit(main())
