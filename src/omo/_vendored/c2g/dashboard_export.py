"""Wave2 dashboard export — single JSON contract for cockpit / agents (ADR-0190).

Combines Phase A backtest + B forecast/heatmap + C proposals into one payload.
Stdout only by default; optional --write under a delivery path (not .omo direct).

  python -m c2g.dashboard_export
  python -m c2g.dashboard_export --data-dir PATH --pretty
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo._vendored.c2g.governance_feedback import build_proposals
from omo._vendored.c2g.outcome_tracker import OutcomeTracker
from omo._vendored.c2g.predictive import (
    PredictiveModel,
    outcomes_time_series,
    render_heatmap_markdown,
    risk_heatmap,
)


def build_dashboard(
    data_dir: Path,
    *,
    horizon: int = 3,
) -> dict[str, Any]:
    tracker = OutcomeTracker(data_dir)
    series = outcomes_time_series(tracker._outcomes)
    forecast = PredictiveModel(horizon=horizon).fit_series(series)
    heat = risk_heatmap(tracker._outcomes)
    backtest = tracker.backtest_report()
    proposals = build_proposals(tracker._outcomes, horizon=horizon)

    cards = {
        "pitch_count": backtest.get("pitch_count", 0),
        "mean_success": backtest.get("mean_success_score", 0.0),
        "trend": forecast.get("trend"),
        "critical": (heat.get("totals") or {}).get("critical", 0),
        "elevated": (heat.get("totals") or {}).get("elevated", 0),
        "proposal_count": proposals.get("proposal_count", 0),
        "p0_proposals": sum(1 for p in proposals.get("proposals") or [] if p.get("priority") == "P0"),
    }

    return {
        "schema": "c2g.wave2.dashboard.v1",
        "adr": "0190",
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "data_dir": str(data_dir),
        "cards": cards,
        "backtest": backtest,
        "forecast": forecast,
        "heatmap": heat,
        "heatmap_markdown": render_heatmap_markdown(heat),
        "proposals": proposals.get("proposals") or [],
        "auto_mutate_rules": False,
        "status": "ok",
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Wave2 dashboard JSON export for cockpit (ADR-0190)")
    ap.add_argument(
        "--data-dir",
        type=Path,
        default=Path("runtime/c2g/outcomes"),
        help="OutcomeTracker directory",
    )
    ap.add_argument("--horizon", type=int, default=3)
    ap.add_argument(
        "--write",
        type=Path,
        default=None,
        help="Optional write path (e.g. runtime/c2g/dashboard.json) — not .omo/",
    )
    ap.add_argument("--pretty", action="store_true", help="Indent JSON on stdout")
    args = ap.parse_args(argv)

    payload = build_dashboard(args.data_dir, horizon=args.horizon)
    text = json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None)

    if args.write:
        out = args.write
        if ".omo" in out.parts:
            print(
                "❌ refuse write under .omo/ — use runtime/ or broker path",
                file=sys.stderr,
            )
            return 2
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
        print(f"wrote {out}", file=sys.stderr)

    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
