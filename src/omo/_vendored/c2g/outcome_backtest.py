"""Wave 2 Phase A — OutcomeTracker backtest CLI (stdout JSON, no .omo writes).

Usage:
  uv run --directory projects/c2g python -m c2g.outcome_backtest [--data-dir PATH]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from omo._vendored.c2g.outcome_tracker import OutcomeTracker


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="C2G OutcomeTracker backtest (ADR-0183 Phase A)")
    ap.add_argument(
        "--data-dir",
        type=Path,
        default=Path("runtime/c2g/outcomes"),
        help="OutcomeTracker data directory (default: runtime/c2g/outcomes)",
    )
    args = ap.parse_args(argv)
    tracker = OutcomeTracker(args.data_dir)
    report = tracker.backtest_report()
    report["data_dir"] = str(args.data_dir)
    json.dump(report, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
