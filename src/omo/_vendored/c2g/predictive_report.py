"""Wave 2 Phase B CLI — predictive report + risk heatmap (stdout JSON).

uv run --directory projects/c2g python -m c2g.predictive_report
uv run --directory projects/c2g python -m c2g.predictive_report --markdown
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from omo._vendored.c2g.knowledge_publisher import publish_predictive_card
from omo._vendored.c2g.outcome_tracker import OutcomeTracker
from omo._vendored.c2g.predictive import (
    PredictiveModel,
    outcomes_time_series,
    render_heatmap_markdown,
    risk_heatmap,
)


def _default_data_dir() -> Path:
    # Prefer env / conventional runtime path; fall back to cwd-local
    import os

    env = os.environ.get("C2G_OUTCOMES_DIR")
    if env:
        return Path(env)
    # workspace-relative if present
    # file = workspace/projects/c2g/src/c2g/predictive_report.py → parents[3]=c2g, [4]=projects
    ws = Path(__file__).resolve().parents[4]
    candidate = ws / "runtime" / "c2g" / "outcomes"
    if candidate.parent.exists() or True:
        return candidate
    return Path("runtime/c2g/outcomes")


def build_report(data_dir: Path, horizon: int = 3, publish_knowledge: bool = False) -> dict:
    tracker = OutcomeTracker(data_dir)
    series = outcomes_time_series(tracker._outcomes)
    model = PredictiveModel(horizon=horizon)
    forecast = model.fit_series(series)
    heat = risk_heatmap(tracker._outcomes)
    backtest = tracker.backtest_report()
    report = {
        "phase": "wave2-b",
        "adr": "0185",
        "data_dir": str(data_dir),
        "backtest": backtest,
        "forecast": forecast,
        "heatmap": heat,
        "heatmap_markdown": render_heatmap_markdown(heat),
        "status": "ok",
    }
    if publish_knowledge:
        pub_res = publish_predictive_card(report)
        report["knowledge_publish"] = pub_res
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="C2G predictive governance report (ADR-0185 Phase B / ADR-0296 Phase C)")
    ap.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="OutcomeTracker data directory (default: runtime/c2g/outcomes)",
    )
    ap.add_argument("--horizon", type=int, default=3, help="Forecast steps ahead")
    ap.add_argument(
        "--markdown",
        action="store_true",
        help="Print heatmap markdown only (for humans)",
    )
    ap.add_argument(
        "--publish-knowledge",
        action="store_true",
        help="Publish predictive report as a knowledge card to KOS (ADR-0296 Phase C)",
    )
    args = ap.parse_args(argv)
    data_dir = args.data_dir or _default_data_dir()
    report = build_report(
        data_dir,
        horizon=args.horizon,
        publish_knowledge=args.publish_knowledge,
    )
    if args.markdown:
        print(report["heatmap_markdown"])
        return 0
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
