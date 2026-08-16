"""Pitch 全链路效果追踪器"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml


@dataclass
class PitchLifecycle:
    pitch_id: str
    pitch_path: str
    created_at: str
    generated_tasks: list[str] = field(default_factory=list)
    completed_tasks: list[str] = field(default_factory=list)
    failed_tasks: list[str] = field(default_factory=list)
    success_score: float = 0.0
    lessons_learned: list[str] = field(default_factory=list)
    total_iterations: int = 0
    status: str = "active"


@dataclass
class SuccessFactors:
    high_success_patterns: list[str] = field(default_factory=list)
    failure_patterns: list[str] = field(default_factory=list)
    recommended_appetite: dict[str, str] = field(default_factory=dict)
    upstream_alignment_importance: float = 0.85


@dataclass
class Suggestion:
    category: str
    priority: int
    message: str
    examples: list[str] = field(default_factory=list)


class OutcomeTracker:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.outcomes_file = data_dir / "pitch-outcomes.yaml"
        self._outcomes = self._load_outcomes()

    def _load_outcomes(self):
        if self.outcomes_file.exists():
            with open(self.outcomes_file, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    def _save_outcomes(self):
        with open(self.outcomes_file, "w", encoding="utf-8") as f:
            yaml.dump(self._outcomes, f, allow_unicode=True, sort_keys=False)

    def track_pitch_creation(self, pitch_id, pitch_path):
        now = (
            datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        )
        self._outcomes.setdefault(
            pitch_id,
            {
                "pitch_id": pitch_id,
                "pitch_path": pitch_path,
                "created_at": now,
                "generated_tasks": [],
                "completed_tasks": [],
                "failed_tasks": [],
                "success_score": 0.0,
                "lessons_learned": [],
                "total_iterations": 0,
                "status": "active",
            },
        )
        self._save_outcomes()

    def track_task_generation(self, pitch_id, task_ids):
        if pitch_id in self._outcomes:
            self._outcomes[pitch_id]["generated_tasks"].extend(task_ids)
            self._save_outcomes()

    def track_task_completion(self, pitch_id, task_id, success=True):
        if pitch_id in self._outcomes:
            if success:
                if task_id not in self._outcomes[pitch_id]["completed_tasks"]:
                    self._outcomes[pitch_id]["completed_tasks"].append(task_id)
            else:
                if task_id not in self._outcomes[pitch_id]["failed_tasks"]:
                    self._outcomes[pitch_id]["failed_tasks"].append(task_id)
            self._update_success_score(pitch_id)
            self._save_outcomes()

    def _update_success_score(self, pitch_id):
        if pitch_id in self._outcomes:
            outcome = self._outcomes[pitch_id]
            total = len(outcome["generated_tasks"])
            completed = len(outcome["completed_tasks"])
            failed = len(outcome["failed_tasks"])
            if total > 0:
                outcome["success_score"] = max(
                    0.0, min(1.0, (completed - failed) / total)
                )

    def add_lessons_learned(self, pitch_id, lesson):
        if pitch_id in self._outcomes:
            self._outcomes[pitch_id]["lessons_learned"].append(lesson)
            self._save_outcomes()

    def track_pitch_lifecycle(self, pitch_id):
        if pitch_id not in self._outcomes:
            return None
        data = self._outcomes[pitch_id]
        return PitchLifecycle(
            pitch_id=data["pitch_id"],
            pitch_path=data["pitch_path"],
            created_at=data["created_at"],
            generated_tasks=data.get("generated_tasks", []),
            completed_tasks=data.get("completed_tasks", []),
            failed_tasks=data.get("failed_tasks", []),
            success_score=data.get("success_score", 0.0),
            lessons_learned=data.get("lessons_learned", []),
            total_iterations=data.get("total_iterations", 0),
            status=data.get("status", "active"),
        )

    def analyze_pitch_success_factors(self):
        factors = SuccessFactors()
        factors.high_success_patterns = [
            "明确 Upstream",
            "合理 Appetite",
            "可衡量验收标准",
        ]
        factors.recommended_appetite = {"小实验": "2小时-1天"}
        return factors

    def suggest_pitch_improvements(self, pitch_content):
        suggestions = []
        if "Upstream" not in pitch_content or "待填" in pitch_content:
            suggestions.append(
                Suggestion("战略对齐", 1, "建议明确 Upstream", ["提升效率"])
            )
        if "Appetite" not in pitch_content or "待填" in pitch_content:
            suggestions.append(Suggestion("范围控制", 2, "建议明确时间预算", ["1天"]))
        return suggestions

    def get_leaderboard(self, top_n=10):
        return sorted(
            self._outcomes.values(),
            key=lambda o: o.get("success_score", 0),
            reverse=True,
        )[:top_n]

    def backtest_report(self) -> dict[str, Any]:
        """Wave 2 Phase A (ADR-0183): read-only closed-loop report over stored outcomes.

        Pure analysis — no strategy mutation. Empty store is a valid zero baseline.
        """
        rows = list(self._outcomes.values())
        n = len(rows)
        if n == 0:
            return {
                "pitch_count": 0,
                "mean_success_score": 0.0,
                "completed_tasks": 0,
                "failed_tasks": 0,
                "top": [],
                "status": "empty",
            }
        scores = [float(r.get("success_score") or 0.0) for r in rows]
        completed = sum(len(r.get("completed_tasks") or []) for r in rows)
        failed = sum(len(r.get("failed_tasks") or []) for r in rows)
        top = self.get_leaderboard(5)
        return {
            "pitch_count": n,
            "mean_success_score": round(sum(scores) / n, 4),
            "completed_tasks": completed,
            "failed_tasks": failed,
            "top": [
                {
                    "pitch_id": r.get("pitch_id"),
                    "success_score": r.get("success_score", 0.0),
                    "status": r.get("status"),
                }
                for r in top
            ],
            "status": "ok",
        }

    def predictive_report(self, horizon: int = 3) -> dict[str, Any]:
        """Wave 2 Phase B (ADR-0185): forecast + risk heatmap over stored outcomes."""
        from c2g.predictive import (
            PredictiveModel,
            outcomes_time_series,
            render_heatmap_markdown,
            risk_heatmap,
        )

        series = outcomes_time_series(self._outcomes)
        forecast = PredictiveModel(horizon=horizon).fit_series(series)
        heat = risk_heatmap(self._outcomes)
        return {
            "phase": "wave2-b",
            "backtest": self.backtest_report(),
            "forecast": forecast,
            "heatmap": heat,
            "heatmap_markdown": render_heatmap_markdown(heat),
            "status": "ok",
        }

    def publish_outcome_to_knowledge(
        self, pitch_id: str, agora_endpoint: str | None = None
    ) -> dict[str, Any] | None:
        """Wave 2 Phase C (ADR-0296): publish historical outcome card to Knowledge Graph."""
        if pitch_id not in self._outcomes:
            return None
        from c2g.knowledge_publisher import publish_outcome_card

        return publish_outcome_card(
            pitch_id=pitch_id,
            outcome_data=self._outcomes[pitch_id],
            agora_endpoint=agora_endpoint,
        )
