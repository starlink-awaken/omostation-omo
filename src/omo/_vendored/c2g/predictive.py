"""Wave 2 Phase B — predictive governance (stdlib time-series, no ARIMA/Prophet).

ADR-0185. Uses exponential moving average + linear trend over OutcomeTracker
success scores. Heavy libraries (statsmodels/prophet) are explicit non-goals
for this slice so CI stays dep-light.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timezone
from typing import Any


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _ema(values: Sequence[float], alpha: float = 0.4) -> list[float]:
    if not values:
        return []
    out = [float(values[0])]
    for v in values[1:]:
        out.append(alpha * float(v) + (1.0 - alpha) * out[-1])
    return out


def _linear_trend(values: Sequence[float]) -> tuple[float, float]:
    """Return (intercept, slope) for y ~ a + b*x, x=0..n-1. Empty → (0, 0)."""
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    if n == 1:
        return float(values[0]), 0.0
    xs = list(range(n))
    mean_x = (n - 1) / 2.0
    mean_y = sum(values) / n
    num = sum((x - mean_x) * (float(y) - mean_y) for x, y in zip(xs, values))
    den = sum((x - mean_x) ** 2 for x in xs) or 1.0
    slope = num / den
    intercept = mean_y - slope * mean_x
    return intercept, slope


@dataclass(frozen=True)
class ForecastPoint:
    index: int
    predicted: float
    lower: float
    upper: float


class PredictiveModel:
    """Stdlib forecaster over ordered success scores."""

    def __init__(self, alpha: float = 0.4, horizon: int = 3, band: float = 0.15):
        self.alpha = alpha
        self.horizon = max(1, horizon)
        self.band = max(0.01, band)

    def fit_series(self, scores: Sequence[float]) -> dict[str, Any]:
        series = [max(0.0, min(1.0, float(s))) for s in scores]
        ema = _ema(series, self.alpha)
        intercept, slope = _linear_trend(series)
        n = len(series)
        forecast: list[dict[str, Any]] = []
        last_ema = ema[-1] if ema else 0.5
        for h in range(1, self.horizon + 1):
            # Blend EMA level with linear extrapolation
            linear = intercept + slope * (n - 1 + h)
            pred = 0.6 * last_ema + 0.4 * linear
            # Mild mean-reversion toward 0.5 for stability on short series
            if n < 5:
                pred = 0.7 * pred + 0.3 * 0.5
            pred = max(0.0, min(1.0, pred))
            half = self.band * (1.0 + 0.1 * h)
            forecast.append(
                {
                    "index": n + h - 1,
                    "horizon": h,
                    "predicted": round(pred, 4),
                    "lower": round(max(0.0, pred - half), 4),
                    "upper": round(min(1.0, pred + half), 4),
                }
            )
            last_ema = 0.5 * pred + 0.5 * last_ema

        residual = 0.0
        if n >= 2 and ema:
            residual = sum(abs(series[i] - ema[i]) for i in range(n)) / n

        trend = "flat"
        if slope > 0.02:
            trend = "improving"
        elif slope < -0.02:
            trend = "declining"

        return {
            "n": n,
            "mean": round(sum(series) / n, 4) if n else 0.0,
            "ema_last": round(ema[-1], 4) if ema else 0.0,
            "slope": round(slope, 6),
            "trend": trend,
            "mean_abs_residual": round(residual, 4),
            "series": [round(s, 4) for s in series],
            "ema": [round(v, 4) for v in ema],
            "forecast": forecast,
            "model": "ema+linear",
            "deps": "stdlib-only",
        }


def outcomes_time_series(outcomes: dict[str, Any]) -> list[float]:
    """Order outcomes by created_at ascending; emit success_score series."""
    rows = list(outcomes.values()) if isinstance(outcomes, dict) else []
    decorated: list[tuple[datetime, float]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        ts = _parse_ts(str(r.get("created_at") or "")) or datetime.min.replace(tzinfo=UTC)
        score = float(r.get("success_score") or 0.0)
        decorated.append((ts, score))
    decorated.sort(key=lambda x: x[0])
    return [s for _, s in decorated]


def risk_heatmap(outcomes: dict[str, Any]) -> dict[str, Any]:
    """Build status × score-bucket risk matrix for viz consumers.

    Buckets: low [0,0.33) · mid [0.33,0.67) · high [0.67,1]
    Risk cells count pitches; failed_task pressure elevates risk class.
    """
    statuses = ("active", "completed", "failed", "other")
    buckets = ("low", "mid", "high")
    matrix: dict[str, dict[str, int]] = {st: {b: 0 for b in buckets} for st in statuses}
    risk_cells: list[dict[str, Any]] = []

    for r in (outcomes or {}).values():
        if not isinstance(r, dict):
            continue
        status = str(r.get("status") or "other")
        if status not in matrix:
            status = "other"
        score = float(r.get("success_score") or 0.0)
        if score < 0.33:
            bucket = "low"
        elif score < 0.67:
            bucket = "mid"
        else:
            bucket = "high"
        matrix[status][bucket] += 1
        failed = len(r.get("failed_tasks") or [])
        gen = len(r.get("generated_tasks") or []) or 1
        fail_rate = failed / gen
        # risk_level for heatmap intensity
        if score < 0.33 or fail_rate >= 0.5:
            risk = "critical"
        elif score < 0.67 or fail_rate >= 0.25:
            risk = "elevated"
        else:
            risk = "ok"
        risk_cells.append(
            {
                "pitch_id": r.get("pitch_id"),
                "status": status,
                "score_bucket": bucket,
                "success_score": round(score, 4),
                "fail_rate": round(fail_rate, 4),
                "risk": risk,
            }
        )

    # intensity grid for simple viz: rows=status, cols=bucket, value=count
    grid = [[matrix[st][b] for b in buckets] for st in statuses]
    return {
        "statuses": list(statuses),
        "buckets": list(buckets),
        "matrix": matrix,
        "grid": grid,
        "cells": risk_cells,
        "totals": {
            "pitches": len(risk_cells),
            "critical": sum(1 for c in risk_cells if c["risk"] == "critical"),
            "elevated": sum(1 for c in risk_cells if c["risk"] == "elevated"),
            "ok": sum(1 for c in risk_cells if c["risk"] == "ok"),
        },
    }


def render_heatmap_markdown(heatmap: dict[str, Any]) -> str:
    """ASCII/Markdown table for agents and closeout docs."""
    statuses: list[str] = list(heatmap.get("statuses") or [])
    buckets: list[str] = list(heatmap.get("buckets") or [])
    matrix: dict[str, dict[str, int]] = heatmap.get("matrix") or {}
    lines = [
        "| status \\ score | " + " | ".join(buckets) + " |",
        "|---|" + "|".join(["---"] * len(buckets)) + "|",
    ]
    for st in statuses:
        row = matrix.get(st) or {}
        cells = [str(int(row.get(b, 0))) for b in buckets]
        lines.append(f"| {st} | " + " | ".join(cells) + " |")
    totals = heatmap.get("totals") or {}
    lines.append("")
    lines.append(
        f"_totals: pitches={totals.get('pitches', 0)} "
        f"critical={totals.get('critical', 0)} "
        f"elevated={totals.get('elevated', 0)} "
        f"ok={totals.get('ok', 0)}_"
    )
    return "\n".join(lines)


def blend_prior(heuristic: float, historical_mean: float, n: int, k: float = 5.0) -> float:
    """Bayesian-ish shrink of content heuristic toward historical base rate.

    weight_hist = n / (n + k); more outcomes → more weight on history.
    """
    n = max(0, int(n))
    w = n / (n + k) if (n + k) else 0.0
    blended = (1.0 - w) * heuristic + w * historical_mean
    return max(0.0, min(1.0, blended))
