"""Lock helpers for workflow lifecycle."""

from __future__ import annotations

import hashlib
import os
import re
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from .core import (
    RUN_UPDATE_LOCK_TIMEOUT_SECONDS,
    WorkflowError,
    display_path,
    lock_state_dir,
    utc_now,
)

_LOCK_FILENAME_MAX_LEN = 255
_RUN_UPDATE_LOCK_NAME_MAX_LEN = _LOCK_FILENAME_MAX_LEN - len("run_.update.lock")
_PATH_LOCK_NAME_MAX_LEN = _LOCK_FILENAME_MAX_LEN - len(".lock.yaml")
_HEARTBEAT_STALE_SECONDS = 3600


def sanitize_lock_name(scope: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", scope).strip("_") or "workspace"


def _bounded_lock_name(scope: str, max_len: int) -> str:
    """锁文件名上限保护: 超长时截断 + 内容 hash 后缀降低碰撞风险.

    macOS filename 上限 255 bytes; `verify` 不带 run_id 时 lifecycle 会把
    argv 串拼进 run_id → 锁名可达数千字节 → Errno 63 崩溃 (T1-05A 修复轮实测).
    """
    name = sanitize_lock_name(scope)
    if len(name) <= max_len:
        return name
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
    return f"{name[: max_len - len(digest) - 1]}-{digest}"


@contextmanager
def run_update_lock(registry: dict[str, Any], run_id: str):
    lock_dir = lock_state_dir(registry)
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_name = _bounded_lock_name(run_id, _RUN_UPDATE_LOCK_NAME_MAX_LEN)
    lock_path = lock_dir / f"run_{lock_name}.update.lock"
    deadline = time.monotonic() + RUN_UPDATE_LOCK_TIMEOUT_SECONDS
    acquired = False
    try:
        while not acquired:
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(f"run_id: {run_id}\ncreated_at: {utc_now()}\n")
                acquired = True
            except FileExistsError:
                try:
                    if time.time() - lock_path.stat().st_mtime > RUN_UPDATE_LOCK_TIMEOUT_SECONDS:
                        lock_path.unlink(missing_ok=True)
                        continue
                except FileNotFoundError:
                    continue
                if time.monotonic() >= deadline:
                    raise WorkflowError(f"timed out waiting for run update lock: {display_path(lock_path)}")
                time.sleep(0.05)
        yield
    finally:
        if acquired:
            lock_path.unlink(missing_ok=True)


def _classify_existing_lock(lock_path: Path) -> dict[str, Any]:
    """Classify an existing lock as live, zombie_expired, or zombie_stale_heartbeat."""
    try:
        payload = yaml.safe_load(lock_path.read_text(encoding="utf-8")) or {}
    except (yaml.YAMLError, OSError):
        return {"kind": "zombie_stale_heartbeat", "detail": "unreadable lock file"}
    expires = payload.get("expires_at", "")
    if expires:
        try:
            exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
            if datetime.now(UTC) > exp_dt:
                return {
                    "kind": "zombie_expired",
                    "detail": f"expired at {expires}",
                    "payload": payload,
                }
        except ValueError:
            pass
    heartbeat = payload.get("last_heartbeat", "")
    if heartbeat:
        try:
            hb_dt = datetime.fromisoformat(heartbeat.replace("Z", "+00:00"))
            age = (datetime.now(UTC) - hb_dt).total_seconds()
            if age > _HEARTBEAT_STALE_SECONDS:
                return {
                    "kind": "zombie_stale_heartbeat",
                    "detail": f"heartbeat {age:.0f}s ago (> {_HEARTBEAT_STALE_SECONDS}s)",
                    "payload": payload,
                }
        except ValueError:
            pass
    return {"kind": "live", "detail": "holder active", "payload": payload}


def lock_scopes_past_expiry(lock_dir: Path) -> set[str]:
    """Return the lock scopes whose ``expires_at`` is provably in the past.

    A2A 冲突谓词用它放行「对方 run 仍 active、但覆盖该 claim 的锁早已过期」的形态。
    只读 ``expires_at``, 不读 ``last_heartbeat``: `_HEARTBEAT_STALE_SECONDS` 会把闲置
    一小时的活 run 判成僵尸 (PITFALL-COO-008 实测 29 条里 22 条), 那个宽度不得进入本判据.
    任何不能证明过期的情形 (锁缺失/不可读/非 mapping/缺 scope 或 expires_at/时间戳
    不可解析或无时区) 都不入选, 即维持冲突 —— fail-closed.
    """
    try:
        candidates = sorted(lock_dir.glob("*.lock.yaml"))
    except OSError:
        return set()

    expired: set[str] = set()
    now = datetime.now(UTC)
    for lock_path in candidates:
        try:
            payload = yaml.safe_load(lock_path.read_text(encoding="utf-8"))
        except (yaml.YAMLError, OSError):
            continue
        if not isinstance(payload, dict):
            continue
        scope = payload.get("scope")
        expires = payload.get("expires_at")
        if not isinstance(scope, str) or not scope:
            continue
        if not isinstance(expires, str) or not expires:
            continue
        try:
            exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
        except ValueError:
            continue
        if exp_dt.tzinfo is None:
            continue
        if now > exp_dt:
            expired.add(scope)
    return expired


def lock_scope_object_present(lock_dir: Path, scope: str) -> bool:
    """该 scope 现在是否有锁文件对象挂在盘上 (不问 TTL, 不问可读性)。

    ``lock_scopes_past_expiry()`` 只覆盖「有对象且能证明过期」那一类; 对象不在时两者
    都给不出答案, 于是「锁已释放」与「锁坏了读不出」在 A2A 谓词里同形, 而这条路径
    再没有自愈通道 (release 只 unlink run_id 等于自己的, prune 只遍历存在的文件)。
    锁写在 claim 记录之前、且记录写入失败会回滚刚建的锁
    (`lifecycle.py` 的 ``acquire_locks()`` → ``write_run()``), 所以「记录里 advertise
    这条路径」蕴含「claim 当时锁存在」—— 缺席即说明它事后已消失, 没有活持有者。
    不用 ``Path.is_file()``: 它把 OSError 吞成 False, 会让「不可探测」冒充「无持有者」。
    """
    name = _bounded_lock_name(scope, _PATH_LOCK_NAME_MAX_LEN)
    lock_file = lock_dir / f"{name}.lock.yaml"
    try:
        lock_file.stat()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


def heartbeat_lock(lock_path: Path) -> None:
    """Update last_heartbeat on a lock file to signal liveness."""
    try:
        payload = yaml.safe_load(lock_path.read_text(encoding="utf-8")) or {}
    except (yaml.YAMLError, OSError):
        return
    payload["last_heartbeat"] = utc_now()
    try:
        lock_path.write_text(
            yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
    except OSError:
        pass


def acquire_locks(
    registry: dict[str, Any],
    scopes: list[str],
    run_id: str,
    actor: str,
    force: bool,
) -> list[str]:
    lock_dir = lock_state_dir(registry)
    lock_dir.mkdir(parents=True, exist_ok=True)
    acquired: list[str] = []
    acquired_paths: list[Path] = []
    ttl_hours = float(registry.get("runner", {}).get("lock_ttl_hours", 24))
    expires_at = (datetime.now(UTC) + timedelta(hours=ttl_hours)).replace(microsecond=0)
    try:
        for scope in scopes:
            lock_name = _bounded_lock_name(scope, _PATH_LOCK_NAME_MAX_LEN)
            lock_path = lock_dir / f"{lock_name}.lock.yaml"
            now_ts = utc_now()
            payload = {
                "run_id": run_id,
                "actor": actor,
                "scope": scope,
                "created_at": now_ts,
                "last_heartbeat": now_ts,
                "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
            }
            if lock_path.exists() and not force:
                classification = _classify_existing_lock(lock_path)
                existing = lock_path.read_text(encoding="utf-8").strip()
                if classification["kind"] == "live":
                    # 幂等续期要求 run_id 与 scope 两个身份同时成立: 锁文件名经过 sanitize 与
                    # 截断+hash, 文件名相同不能证明是同一个 scope。payload 不可解析成 mapping、
                    # 缺 run_id 或 scope 与本次不一致, 都算不出「这把锁是我自己持有的」,
                    # 维持原有冲突 —— fail-closed.
                    held = classification.get("payload")
                    if isinstance(held, dict) and held.get("run_id") == run_id and held.get("scope") == scope:
                        held["last_heartbeat"] = now_ts
                        held["expires_at"] = expires_at.isoformat().replace("+00:00", "Z")
                        lock_path.write_text(
                            yaml.safe_dump(held, allow_unicode=True, sort_keys=False),
                            encoding="utf-8",
                        )
                        acquired.append(display_path(lock_path))
                        continue
                    raise WorkflowError(
                        f"lock HELD (live) for {scope}: {lock_path}\n"
                        f"  holder is active — {classification['detail']}\n"
                        f"{existing}"
                    )
                lock_path.unlink(missing_ok=True)
            with lock_path.open("w" if force else "x", encoding="utf-8") as handle:
                yaml.safe_dump(payload, handle, allow_unicode=True, sort_keys=False)
            acquired_paths.append(lock_path)
            acquired.append(display_path(lock_path))
    except WorkflowError:
        raise
    except Exception:
        for path in acquired_paths:
            path.unlink(missing_ok=True)
        raise
    return acquired


def release_locks(registry: dict[str, Any], run_id: str) -> list[str]:
    lock_dir = lock_state_dir(registry)
    released: list[str] = []
    if not lock_dir.exists():
        return released
    for lock_path in lock_dir.glob("*.lock.yaml"):
        try:
            payload = yaml.safe_load(lock_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            continue
        if payload.get("run_id") == run_id:
            lock_path.unlink()
            released.append(display_path(lock_path))
    return released


def scan_locks(registry: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a report of all path locks with live/zombie classification."""
    lock_dir = lock_state_dir(registry)
    results: list[dict[str, Any]] = []
    if not lock_dir.exists():
        return results
    for lock_path in sorted(lock_dir.glob("*.lock.yaml")):
        classification = _classify_existing_lock(lock_path)
        entry: dict[str, Any] = {
            "path": display_path(lock_path),
            "kind": classification["kind"],
            "detail": classification["detail"],
        }
        payload = classification.get("payload") or {}
        if payload:
            entry["run_id"] = payload.get("run_id", "")
            entry["actor"] = payload.get("actor", "")
            entry["scope"] = payload.get("scope", "")
            entry["created_at"] = payload.get("created_at", "")
            entry["last_heartbeat"] = payload.get("last_heartbeat", "")
            entry["expires_at"] = payload.get("expires_at", "")
        results.append(entry)
    return results


def prune_stale_locks(registry: dict[str, Any]) -> list[dict[str, Any]]:
    """Remove zombie locks (expired or stale heartbeat). Return pruned entries."""
    from .core import WORKSPACE

    pruned: list[dict[str, Any]] = []
    for entry in scan_locks(registry):
        if entry["kind"] in ("zombie_expired", "zombie_stale_heartbeat"):
            lock_file = Path(entry["path"])
            if not lock_file.is_absolute():
                lock_file = WORKSPACE / lock_file
            lock_file.unlink(missing_ok=True)
            pruned.append(entry)
    return pruned
