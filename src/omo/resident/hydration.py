"""Agent hydration state machine — cognitive frames persisted in SQLite WAL (BET-Y1Q4-T10-132).

Decouples persistent agent mental state from physical compute: cognitive
frames live in a WAL-mode SQLite database; an external event triggers
``hydrate`` (<15ms target) which rebuilds the in-memory frame, and after a
unit of work ``dehydrate`` commits the frame atomically and releases the
in-memory reference (no GPU tensors held ⇒ memory delta 0 by construction).

States: DORMANT → HYDRATING → ACTIVE → DEHYDRATING → DORMANT.
Illegal transitions fail closed. All writes are transactional; WAL keeps
concurrent hydrate/dehydrate across agents non-blocking.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

SCHEMA_V3 = Path(__file__).resolve().parent / "schema_v3.sql"

STATES = ("DORMANT", "HYDRATING", "ACTIVE", "DEHYDRATING")

# Legal transitions: current -> set of allowed next states
_TRANSITIONS: dict[str, set[str]] = {
    "DORMANT": {"HYDRATING", "DEHYDRATING"},  # DEHYDRATING from DORMANT = 初始帧直接落盘
    "HYDRATING": {"ACTIVE", "DORMANT"},  # hydrate may abort back to DORMANT
    "ACTIVE": {"DEHYDRATING"},
    "DEHYDRATING": {"DORMANT"},
}

HYDRATE_BUDGET_MS = 15.0


class IllegalTransitionError(ValueError):
    """Raised when a state transition is not allowed."""


class HydrationStateMachine:
    """Per-agent state machine backed by a WAL-mode SQLite database."""

    def __init__(self, db_path: str | Path, agent_id: str) -> None:
        self.agent_id = agent_id
        self.db_path = Path(db_path)
        self._ensure_schema()
        self._state = self._read_state() or "DORMANT"
        self._frame: dict[str, Any] | None = None

    # -- schema ------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA_V3.read_text(encoding="utf-8"))

    def _read_state(self) -> str | None:
        with self._connect() as conn:
            row = conn.execute("SELECT state FROM hydration_frames WHERE agent_id = ?", (self.agent_id,)).fetchone()
            return row[0] if row else None

    def _set_state(self, state: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO hydration_frames (agent_id, frame, state, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(agent_id) DO UPDATE
                   SET state = excluded.state, updated_at = excluded.updated_at""",
                (self.agent_id, json.dumps(self._frame or {}), state, time.time()),
            )
        self._state = state

    def _check(self, nxt: str) -> None:
        if nxt not in _TRANSITIONS.get(self._state, set()):
            raise IllegalTransitionError(
                f"{self._state} -> {nxt} 不合法（合法: {sorted(_TRANSITIONS.get(self._state, set()))}）"
            )

    # -- hydrate -------------------------------------------------------------

    def hydrate(self) -> dict[str, Any]:
        """DORMANT -> HYDRATING -> ACTIVE: rebuild in-memory frame from WAL SQLite."""
        self._check("HYDRATING")
        self._set_state("HYDRATING")
        t0 = time.perf_counter()
        with self._connect() as conn:
            row = conn.execute("SELECT frame FROM hydration_frames WHERE agent_id = ?", (self.agent_id,)).fetchone()
        frame = json.loads(row[0]) if row else {}
        elapsed_ms = (time.perf_counter() - t0) * 1000
        self._check("ACTIVE")
        self._set_state("ACTIVE")
        self._frame = frame
        self.last_hydrate_ms = elapsed_ms
        return {"ok": True, "frame": frame, "hydrate_ms": round(elapsed_ms, 3), "budget_ms": HYDRATE_BUDGET_MS}

    # -- dehydrate -------------------------------------------------------------

    def dehydrate(self, frame: dict[str, Any] | None = None) -> dict[str, Any]:
        """ACTIVE -> DEHYDRATING -> DORMANT: atomically persist frame, release memory."""
        self._check("DEHYDRATING")
        self._set_state("DEHYDRATING")
        t0 = time.perf_counter()
        if frame is not None:
            self._frame = frame
        payload = json.dumps(self._frame or {}, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO hydration_frames (agent_id, frame, state, updated_at)
                   VALUES (?, ?, 'DORMANT', ?)
                   ON CONFLICT(agent_id) DO UPDATE
                   SET frame = excluded.frame, state = 'DORMANT', updated_at = excluded.updated_at""",
                (self.agent_id, payload, time.time()),
            )
        self._frame = None  # release in-memory reference (no tensor held)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        self._state = "DORMANT"
        return {"ok": True, "dehydrate_ms": round(elapsed_ms, 3)}

    # -- introspection -------------------------------------------------------------

    @property
    def state(self) -> str:
        return self._state

    def probe_read_latency(self, iterations: int = 5) -> float:
        """WAL mode=ro read-only probe; returns worst latency in ms (target ≤15)."""
        worst = 0.0
        for _ in range(iterations):
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=10.0)
            t0 = time.perf_counter()
            conn.execute("SELECT count(*) FROM hydration_frames").fetchone()
            worst = max(worst, (time.perf_counter() - t0) * 1000)
            conn.close()
        return worst
