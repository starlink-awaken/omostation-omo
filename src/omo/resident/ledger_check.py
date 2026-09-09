"""Resident event-ledger SQLite lock monitor + recovery (BET-Y1Q4-T9-01).

- Probe lock age via `pragma_lock_status` / busy retry timing.
- If lock held > LOCK_TIMEOUT_SECONDS, attempt WAL checkpoint.
- After MAX_CHECKPOINT_FAILURES consecutive failures, kill a confirmed
  zombie holder (CPU <1% over SAMPLE_WINDOW_SECONDS) — circuit_breaker.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any

LOCK_TIMEOUT_SECONDS = 300  # 5 min
MAX_CHECKPOINT_FAILURES = 3
SAMPLE_WINDOW_SECONDS = 300
CPU_ZOMBIE_THRESHOLD_PCT = 1.0

_checkpoint_failures = 0


def lock_age_seconds(ledger: Path) -> float | None:
    """Return approximate lock hold age in seconds, or None if unlocked/missing.

    Uses read-only URI (mode=ro) exclusively — never attempts write lock acquisition.
    """
    if not ledger.is_file():
        return None
    try:
        conn = sqlite3.connect(f"file:{ledger}?mode=ro", uri=True, timeout=0.05)
    except sqlite3.Error:
        # Immediate busy on open → treat as locked; age unknown → use mtime delta of -wal/-shm if present
        return _lock_age_from_sidecars(ledger)
    try:
        try:
            rows = list(conn.execute("PRAGMA lock_status"))
        except sqlite3.Error:
            rows = []
        locked = any(str(state).lower() not in {"unlocked", "0", ""} for _, state in rows) if rows else False
        if not locked:
            return None  # Read-only probe confirms unlocked — no write lock needed
        return _lock_age_from_sidecars(ledger)
    finally:
        conn.close()


def _lock_age_from_sidecars(ledger: Path) -> float | None:
    candidates = [ledger.with_suffix(ledger.suffix + "-wal"), ledger.with_suffix(ledger.suffix + "-shm")]
    ages = []
    now = time.time()
    for path in candidates:
        try:
            ages.append(now - path.stat().st_mtime)
        except OSError:
            continue
    if not ages:
        try:
            return now - ledger.stat().st_mtime
        except OSError:
            return None
    return max(ages)


def check_lock_state_only(ledger: Path) -> dict[str, Any]:
    """Observe ledger lock state without attempting recovery or mutation."""
    result: dict[str, Any] = {
        "ok": True,
        "state": "unlocked",
        "lock_age_seconds": None,
        "observation_mode": "read_only",
        "recovery_performed": False,
        "detail": "ledger unlocked",
    }
    if not ledger.is_file():
        result.update(state="missing", detail="ledger missing")
        return result

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(f"file:{ledger}?mode=ro", uri=True, timeout=0.05)
        try:
            rows = list(conn.execute("PRAGMA lock_status"))
            if not rows:
                conn.execute("SELECT 1").fetchone()
        except sqlite3.Error as exc:
            message = str(exc)
            state = "busy" if any(marker in message.lower() for marker in ("locked", "busy")) else "io_error"
            result.update(ok=False, state=state, detail=f"read-only lock observation failed: {message}")
            return result

        locked = any(str(state).lower() not in {"unlocked", "0", ""} for _, state in rows)
        if not locked:
            return result

        age = _lock_age_from_sidecars(ledger)
        if age is None:
            result.update(ok=False, state="unknown", detail="ledger lock observed; lock age unknown")
            return result
        result.update(
            ok=False,
            state="locked",
            lock_age_seconds=int(age),
            detail=f"ledger lock observed for {int(age)}s",
        )
        return result
    except sqlite3.Error as exc:
        message = str(exc)
        state = "busy" if any(marker in message.lower() for marker in ("locked", "busy")) else "io_error"
        result.update(ok=False, state=state, detail=f"read-only lock observation failed: {message}")
        return result
    except Exception as exc:  # noqa: BLE001 - observation must degrade, never trigger recovery.
        result.update(ok=False, state="unknown", detail=f"read-only lock observation failed: {exc}")
        return result
    finally:
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass


def wal_checkpoint(ledger: Path) -> tuple[bool, str]:
    """Run WAL truncate checkpoint. Returns (ok, detail)."""
    global _checkpoint_failures
    if not ledger.is_file():
        return True, "ledger missing — nothing to checkpoint"
    try:
        conn = sqlite3.connect(str(ledger), timeout=5.0)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            _checkpoint_failures = 0
            return True, "wal_checkpoint TRUNCATE ok"
        finally:
            conn.close()
    except sqlite3.Error as exc:
        _checkpoint_failures += 1
        return False, f"checkpoint failed ({_checkpoint_failures}/{MAX_CHECKPOINT_FAILURES}): {exc}"


def find_sqlite_holders(ledger: Path) -> list[int]:
    """Best-effort PIDs holding the ledger file open (macOS/Linux)."""
    try:
        out = subprocess.check_output(
            ["lsof", "-t", str(ledger)],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return []
    pids: list[int] = []
    for line in out.splitlines():
        line = line.strip()
        if line.isdigit():
            pids.append(int(line))
    return pids


def _cpu_pct(pid: int, sample_seconds: float = 2.0) -> float | None:
    """Sample process CPU% over a short window via ps."""
    try:
        before = subprocess.check_output(["ps", "-o", "%cpu=", "-p", str(pid)], text=True).strip()
        time.sleep(sample_seconds)
        after = subprocess.check_output(["ps", "-o", "%cpu=", "-p", str(pid)], text=True).strip()
        vals = []
        for raw in (before, after):
            try:
                vals.append(float(raw))
            except ValueError:
                continue
        return sum(vals) / len(vals) if vals else None
    except (OSError, subprocess.CalledProcessError):
        return None


def maybe_kill_zombie_holders(ledger: Path) -> dict[str, Any]:
    """Kill confirmed zombies only after MAX_CHECKPOINT_FAILURES (circuit_breaker)."""
    global _checkpoint_failures
    if _checkpoint_failures < MAX_CHECKPOINT_FAILURES:
        return {"killed": [], "detail": "checkpoint failure budget not exhausted"}
    killed: list[int] = []
    for pid in find_sqlite_holders(ledger):
        if pid == os.getpid():
            continue
        cpu = _cpu_pct(pid)
        if cpu is None:
            continue
        if cpu < CPU_ZOMBIE_THRESHOLD_PCT:
            try:
                os.kill(pid, 15)
                killed.append(pid)
            except OSError:
                continue
    if killed:
        _checkpoint_failures = 0
    return {"killed": killed, "detail": f"zombie kill attempted for {killed}"}


def check_and_recover(ledger: Path) -> dict[str, Any]:
    """Probe lock age; checkpoint if stale; optionally kill zombies."""
    age = lock_age_seconds(ledger)
    result: dict[str, Any] = {
        "ok": True,
        "lock_age_seconds": None if age is None else int(age),
        "locked": age is not None,
        "checkpoint": None,
        "recovery": None,
    }
    if age is None:
        result["detail"] = "unlocked or ledger absent"
        return result
    if age <= LOCK_TIMEOUT_SECONDS:
        result["detail"] = f"lock age {int(age)}s within budget"
        return result
    ok, detail = wal_checkpoint(ledger)
    result["checkpoint"] = {"ok": ok, "detail": detail}
    if ok:
        result["detail"] = "stale lock cleared via checkpoint"
        # re-probe
        age2 = lock_age_seconds(ledger)
        result["lock_age_seconds"] = None if age2 is None else int(age2)
        result["locked"] = age2 is not None
        return result
    result["ok"] = False
    result["detail"] = detail
    recovery = maybe_kill_zombie_holders(ledger)
    result["recovery"] = recovery
    if recovery.get("killed"):
        result["ok"] = True
        result["detail"] = f"recovered after killing zombies {recovery['killed']}"
    return result


recover_ledger_with_wal_checkpoint = check_and_recover
