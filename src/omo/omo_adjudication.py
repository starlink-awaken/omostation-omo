"""omo_adjudication.py — AdjudicationRecorded 事件与裁决存储 (BET-Y1Q1-T4-01).

结果面: 系统第一次能记录"人类接受了什么、改了什么".
守 ADR-0372: 决策日志入 bos://memory/mos/*.
关联: decision_outcome.decision_id (do-NNNN).

存储: .omo/_delivery/outcomes/adjudications.jsonl (append-only).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from .omo_io import AppendOnlyLog, fcntl_lock, write_yaml_atomic
from .omo_paths import DELIVERY_DIR

if TYPE_CHECKING:
    from .omo_autonomy_level import AutonomyLadder

# ---------------------------------------------------------------------------
# BET-Y1Q3-T4-07 (WP5) — authority-bound human adjudication 合同
# ---------------------------------------------------------------------------

WP5_SOURCE_CLASS = "real_human"


@dataclass(frozen=True)
class HumanAdjudication:
    """WP5 合同: authority 绑定的人类裁决 (spec §3)。

    authority_receipt_digest 来自 WP4 的 OMO 权威验证 — 无绑定不产生
    qualifying outcome。
    """

    adjudication_id: str
    decision_id: str
    principal_id: str
    verdict: str
    source_class: str
    authority_receipt_digest: str
    adjudicated_at: str


def is_qualifying_outcome(
    adjudication: HumanAdjudication,
    *,
    decision_persisted: bool,
    scene_id: str,
    episode_id: str,
) -> tuple[bool, str]:
    """WP5 qualifying 判定 (spec §2 价值真值边界)。

    返回 (qualifying, reason)。非 qualifying 不计 gate、价值状态不变。
    """
    if adjudication.source_class != WP5_SOURCE_CLASS:
        return False, f"source_class must be {WP5_SOURCE_CLASS!r}"
    if not adjudication.authority_receipt_digest.startswith("sha256:"):
        return False, "authority receipt binding missing (WP4)"
    if not adjudication.principal_id.startswith("principal:"):
        return False, "principal_id format invalid"
    if not decision_persisted:
        return False, "adjudication must bind a persisted decision"
    if not scene_id or not episode_id:
        return False, "scene/episode lineage required"
    return True, "ok"


VERDICT_CONFIDENCE_DELTA: dict[str, float] = {
    "accepted": +0.05,
    "modified": -0.05,
    "rejected": -0.20,
}

OUTCOMES_DIR = DELIVERY_DIR / "outcomes"
ADJUDICATIONS_LOG = OUTCOMES_DIR / "adjudications.jsonl"
ADJUDICATION_SCHEMA = "adjudication/v1"
VALID_VERDICTS = frozenset({"accepted", "modified", "rejected"})


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _log() -> AppendOnlyLog:
    OUTCOMES_DIR.mkdir(parents=True, exist_ok=True)
    return AppendOnlyLog(
        path=ADJUDICATIONS_LOG,
        lock=fcntl_lock(ADJUDICATIONS_LOG.with_suffix(".lock")),
    )


@dataclass
class AdjudicationRecord:
    """人类裁决记录 — 关联回 decision_outcome.decision_id."""

    id: str
    decision_id: str
    verdict: str
    adjudicated_at: str = field(default_factory=_utc_now)
    edit_diff: str = ""
    time_spent_seconds: float = 0.0
    adjudicator: str = ""
    notes: str = ""
    schema_version: str = ADJUDICATION_SCHEMA


class AdjudicationStore:
    """裁决存储 — append-only JSONL + 查询 + 闭环信念修正."""

    def __init__(
        self,
        log: AppendOnlyLog | None = None,
        mos_manager: Any | None = None,
        calibration_summary_path: Path | None = None,
        autonomy_ladder: AutonomyLadder | None = None,
    ) -> None:
        self._log = log or _log()
        self._mos_manager = mos_manager
        self._calibration_summary_path = calibration_summary_path
        self._autonomy_ladder = autonomy_ladder
        self._counter: int | None = None

    def _next_id(self) -> str:
        records = self._log.read_all()
        n = len(records) + 1
        return f"adj-{n:04d}"

    def record(
        self,
        *,
        decision_id: str,
        verdict: str,
        edit_diff: str = "",
        time_spent_seconds: float = 0.0,
        adjudicator: str = "",
        notes: str = "",
    ) -> str:
        """记录一条裁决, 返回 adjudication id.

        Args:
            decision_id: 关联的 decision_outcome.id (do-NNNN).
            verdict: accepted | modified | rejected.
            edit_diff: 人类修改的 diff (modified 时建议填).
            time_spent_seconds: 审阅耗时.
            adjudicator: 裁决人标识.
            notes: 自由文本备注.

        The primary adjudication is appended before injected observation sinks run.
        Observation failures propagate so callers can report a partial closeout instead
        of claiming that every derived state update succeeded.
        """
        if verdict not in VALID_VERDICTS:
            raise ValueError(f"verdict must be one of {sorted(VALID_VERDICTS)}, got {verdict!r}")
        adj_id = self._next_id()
        record = AdjudicationRecord(
            id=adj_id,
            decision_id=decision_id,
            verdict=verdict,
            edit_diff=edit_diff,
            time_spent_seconds=time_spent_seconds,
            adjudicator=adjudicator,
            notes=notes,
        )
        self._log.append(asdict(record), sort_keys=True)
        self._apply_belief_feedback(decision_id, verdict)
        self._update_capability_calibration(decision_id, verdict)
        return adj_id

    def record_wp5_outcome(
        self,
        adjudication: HumanAdjudication,
        *,
        scene_id: str,
        episode_id: str,
        burden_minutes: float | None = None,
    ) -> dict[str, Any]:
        """WP5 truth-writer (BET-Y1Q3-T4-07): authority-bound 裁决 → qualifying outcome。

        事务边界语义 (spec §3): qualifying 验证 → 幂等查重 → append-only 写入。
        非 qualifying / replay conflict 一律拒绝且计数不变; append 失败不返回 success。
        同一 adjudication_id 重放复用已写入记录 (幂等)。
        """
        ok, reason = is_qualifying_outcome(
            adjudication,
            decision_persisted=True,
            scene_id=scene_id,
            episode_id=episode_id,
        )
        if not ok:
            return {
                "qualifying": False,
                "reason": reason,
                "qualifying_count": self._wp5_count(),
            }
        existing = self._wp5_find(adjudication.adjudication_id)
        if existing is not None:
            if existing.get("authority_receipt_digest") != adjudication.authority_receipt_digest:
                return {
                    "qualifying": False,
                    "reason": "replay_conflict: same id different authority digest",
                    "qualifying_count": self._wp5_count(),
                }
            return {
                "qualifying": True,
                "replayed": True,
                "adjudication_id": adjudication.adjudication_id,
                "qualifying_count": self._wp5_count(),
            }
        record = {
            "schema": "wp5-human-adjudication/v1",
            **asdict(adjudication),
            "scene_id": scene_id,
            "episode_id": episode_id,
            "burden_minutes": burden_minutes,
        }
        self._log.append(record, sort_keys=True)
        appended = self._wp5_find(adjudication.adjudication_id)
        if appended is None:
            raise RuntimeError("wp5 outcome append failed (truth-writer durable guarantee)")
        return {
            "qualifying": True,
            "replayed": False,
            "adjudication_id": adjudication.adjudication_id,
            "qualifying_count": self._wp5_count(),
        }

    def _wp5_records(self) -> list[dict[str, Any]]:
        return [
            r for r in self._log.read_all() if isinstance(r, dict) and r.get("schema") == "wp5-human-adjudication/v1"
        ]

    def _wp5_find(self, adjudication_id: str) -> dict[str, Any] | None:
        return next(
            (r for r in self._wp5_records() if r.get("adjudication_id") == adjudication_id),
            None,
        )

    def _wp5_count(self) -> int:
        return len(self._wp5_records())

    def _apply_belief_feedback(self, decision_id: str, verdict: str) -> None:
        """闭环: 裁决 → 信念置信度修正 (best-effort, 不抛异常)."""
        if self._mos_manager is None:
            return
        try:
            outcome = self._mos_manager.get_decision_outcome(decision_id)
            if outcome is None:
                return
            topic = outcome.get("decision_type", "")
            belief = self._mos_manager.find_belief_by_topic(topic)
            if belief is None:
                return
            delta = VERDICT_CONFIDENCE_DELTA.get(verdict, 0.0)
            if delta != 0.0:
                self._mos_manager.update_belief_confidence(
                    belief["id"],
                    delta,
                    reason=f"adjudication:{verdict} decision={decision_id}",
                )
        except Exception:
            pass

    def _update_capability_calibration(self, decision_id: str, verdict: str) -> None:
        """闭环: 裁决 → capability_calibration 自动更新 (BET-Y1Q2-T4-01).

        公式: calibration = accepted_as_is / invocations (per capability).
        """
        if self._mos_manager is None:
            return
        outcome = self._mos_manager.get_decision_outcome(decision_id)
        if outcome is None:
            return
        capability = outcome.get("decision_type", "unknown")

        records = self._log.read_all()
        total = 0
        accepted = 0
        for record in records:
            related_decision_id = record.get("decision_id", "")
            if not related_decision_id:
                continue
            related = self._mos_manager.get_decision_outcome(related_decision_id)
            if related and related.get("decision_type") == capability:
                total += 1
                if record.get("verdict") == "accepted":
                    accepted += 1

        if total == 0:
            return
        calibration = accepted / total
        self._mos_manager.record_capability_calibration(
            capability_ref=capability,
            success_rate=round(calibration, 4),
            sample_size=total,
            last_run_id=decision_id,
        )

        if self._calibration_summary_path is not None:
            self._write_calibration_summary(
                capability=capability,
                calibration=calibration,
                accepted=accepted,
                total=total,
            )

        self._check_autonomy_ladder(capability, verdict)

    def _write_calibration_summary(
        self,
        *,
        capability: str,
        calibration: float,
        accepted: int,
        total: int,
    ) -> None:
        summary_path = self._calibration_summary_path
        if summary_path is None:
            return
        with fcntl_lock(summary_path.with_suffix(".lock")):
            existing: dict[str, Any] = {}
            if summary_path.exists():
                loaded = yaml.safe_load(summary_path.read_text(encoding="utf-8")) or {}
                if not isinstance(loaded, dict):
                    raise ValueError(f"calibration summary must be a mapping: {summary_path}")
                existing = loaded
            existing[capability] = {
                "calibration": round(calibration, 4),
                "accepted": accepted,
                "total": total,
                "updated_at": _utc_now(),
            }
            write_yaml_atomic(summary_path, existing)

    def _check_autonomy_ladder(self, capability: str, verdict: str) -> None:
        """闭环: 裁决 → 自主性阶梯升降级检查 (BET-Y1Q4-T3-01)."""
        if self._autonomy_ladder is None:
            return
        self._autonomy_ladder.record_adjudication(capability, verdict)

    def query(
        self,
        *,
        decision_id: str | None = None,
        verdict: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """查询裁决记录."""
        records = self._log.read_all()
        if decision_id:
            records = [r for r in records if r.get("decision_id") == decision_id]
        if verdict:
            records = [r for r in records if r.get("verdict") == verdict]
        return records[-limit:]

    def stats(self) -> dict[str, int]:
        """裁决统计 — 按 verdict 计数."""
        records = self._log.read_all()
        counts: dict[str, int] = {"total": len(records)}
        for v in sorted(VALID_VERDICTS):
            counts[v] = sum(1 for r in records if r.get("verdict") == v)
        return counts


__all__ = [
    "ADJUDICATIONS_LOG",
    "ADJUDICATION_SCHEMA",
    "OUTCOMES_DIR",
    "VALID_VERDICTS",
    "VERDICT_CONFIDENCE_DELTA",
    "AdjudicationRecord",
    "AdjudicationStore",
]
