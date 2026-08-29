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
