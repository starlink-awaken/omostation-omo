"""Pitch 智能分析模块"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class StrategicIntent:
    core_goal: str
    key_objectives: list[str] = field(default_factory=list)
    success_metrics: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)


class PitchIntelligenceAnalyzer:
    def __init__(self, outcome_tracker):
        self.outcome_tracker = outcome_tracker

    def predict_pitch_success_probability(self, pitch_content):
        """Heuristic content score, shrunk toward historical base rate (Wave2-B).

        Phase A used pure heuristics. Phase B (ADR-0185) blends EMA/mean of
        stored outcomes so predictions improve as OutcomeTracker fills up.
        """
        score = 0.5
        if "Upstream" in pitch_content and "待填" not in pitch_content:
            score += 0.15
        if "Appetite" in pitch_content and "待填" not in pitch_content:
            score += 0.1
        # Optional Acceptance / Risk sections strengthen signal
        if "Acceptance" in pitch_content or "验收" in pitch_content:
            score += 0.05
        if "Risk" in pitch_content or "风险" in pitch_content:
            score += 0.05
        score = max(0.0, min(1.0, score))

        try:
            from c2g.predictive import blend_prior, outcomes_time_series

            series = outcomes_time_series(
                getattr(self.outcome_tracker, "_outcomes", {})
            )
            if series:
                hist_mean = sum(series) / len(series)
                score = blend_prior(score, hist_mean, n=len(series))
        except Exception:  # noqa: BLE001, S110  # never fail prediction on tracker edge cases
            pass
        return max(0.0, min(1.0, score))

    def extract_strategic_intent(self, pitch_content):
        intent = StrategicIntent(core_goal="")
        lines = pitch_content.split("\n")
        if lines and lines[0].startswith("# "):
            intent.core_goal = lines[0][2:].strip()
        return intent

    def suggest_pitch_improvements(self, pitch_content):
        suggestions = []
        checks = [
            ("Upstream", "Upstream", "战略对齐", "建议明确 Upstream"),
            ("Appetite", "Appetite", "范围控制", "建议明确时间预算"),
        ]
        for key, check_str, category, message in checks:
            if key not in pitch_content or "待填" in pitch_content:
                suggestions.append(
                    {
                        "category": category,
                        "priority": len(suggestions) + 1,
                        "message": message,
                        "status": "missing",
                    }
                )
            else:
                suggestions.append(
                    {
                        "category": category,
                        "priority": len(suggestions) + 1,
                        "message": f"✓ {category} 已设置",
                        "status": "ok",
                    }
                )
        return suggestions
