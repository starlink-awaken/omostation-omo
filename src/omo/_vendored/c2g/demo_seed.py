"""Wave2 demo OutcomeTracker seed (ADR-0193).

Populates a small, deterministic pitch-outcomes set so dashboard / heatmap /
proposals have something to render in empty workspaces.

  python -m c2g.demo_seed --data-dir runtime/c2g/outcomes
  python -m c2g.demo_seed --reset   # wipe then seed
  python -m c2g.demo_seed --json

Never writes under .omo/ — only OutcomeTracker data_dir.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from omo._vendored.c2g.outcome_tracker import OutcomeTracker

# Deterministic demo corpus: mix of improving early pitches + recent decline
# so forecast trend + critical/elevated cells light up.
_DEMO_PITCHES: list[dict[str, Any]] = [
    {
        "pitch_id": "demo-upstream-clarity",
        "pitch_path": "sandbox/pitches/demo-upstream-clarity.md",
        "created_at": "2026-06-01T10:00:00Z",
        "generated": ["t-a1", "t-a2"],
        "completed": ["t-a1", "t-a2"],
        "failed": [],
        "status": "completed",
        "lessons": ["明确 Upstream 提升交付率"],
    },
    {
        "pitch_id": "demo-appetite-sized",
        "pitch_path": "sandbox/pitches/demo-appetite-sized.md",
        "created_at": "2026-06-08T10:00:00Z",
        "generated": ["t-b1", "t-b2", "t-b3"],
        "completed": ["t-b1", "t-b2"],
        "failed": [],
        "status": "completed",
        "lessons": ["Appetite 1 天边界清晰"],
    },
    {
        "pitch_id": "demo-partial-delivery",
        "pitch_path": "sandbox/pitches/demo-partial-delivery.md",
        "created_at": "2026-06-15T10:00:00Z",
        "generated": ["t-c1", "t-c2", "t-c3", "t-c4"],
        "completed": ["t-c1", "t-c2"],
        "failed": ["t-c3"],
        "status": "active",
        "lessons": ["缺验收标准导致返工"],
    },
    {
        "pitch_id": "demo-failing-batch",
        "pitch_path": "sandbox/pitches/demo-failing-batch.md",
        "created_at": "2026-06-22T10:00:00Z",
        "generated": ["t-d1", "t-d2", "t-d3"],
        "completed": [],
        "failed": ["t-d1", "t-d2"],
        "status": "failed",
        "lessons": ["未对齐 Risk 与依赖"],
    },
    {
        "pitch_id": "demo-recovery",
        "pitch_path": "sandbox/pitches/demo-recovery.md",
        "created_at": "2026-06-29T10:00:00Z",
        "generated": ["t-e1", "t-e2"],
        "completed": ["t-e1"],
        "failed": [],
        "status": "active",
        "lessons": [],
    },
    {
        "pitch_id": "demo-critical-recent",
        "pitch_path": "sandbox/pitches/demo-critical-recent.md",
        "created_at": "2026-07-10T10:00:00Z",
        "generated": ["t-f1", "t-f2", "t-f3", "t-f4"],
        "completed": ["t-f1"],
        "failed": ["t-f2", "t-f3", "t-f4"],
        "status": "failed",
        "lessons": ["近期失败率升高 — 需策略复盘"],
    },
]


def seed_demo_outcomes(
    data_dir: Path,
    *,
    reset: bool = False,
) -> dict[str, Any]:
    """Write demo pitches into OutcomeTracker store. Returns summary."""
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    outcomes_file = data_dir / "pitch-outcomes.yaml"

    if reset and outcomes_file.exists():
        outcomes_file.unlink()

    tracker = OutcomeTracker(data_dir)
    created: list[str] = []
    skipped: list[str] = []

    for row in _DEMO_PITCHES:
        pid = row["pitch_id"]
        if pid in tracker._outcomes and not reset:
            skipped.append(pid)
            continue
        tracker.track_pitch_creation(pid, row["pitch_path"])
        tracker._outcomes[pid]["created_at"] = row["created_at"]
        tracker._outcomes[pid]["generated_tasks"] = list(row["generated"])
        tracker._outcomes[pid]["completed_tasks"] = list(row["completed"])
        tracker._outcomes[pid]["failed_tasks"] = list(row["failed"])
        tracker._outcomes[pid]["status"] = row["status"]
        tracker._outcomes[pid]["lessons_learned"] = list(row.get("lessons") or [])
        # recompute success_score from tasks
        tracker._update_success_score(pid)
        created.append(pid)

    tracker._save_outcomes()

    return {
        "schema": "c2g.wave2.demo_seed.v1",
        "adr": "0193",
        "data_dir": str(data_dir),
        "reset": reset,
        "seeded": created,
        "skipped": skipped,
        "pitch_count": len(tracker._outcomes),
        "mean_success_score": (
            round(
                sum(
                    float(o.get("success_score") or 0)
                    for o in tracker._outcomes.values()
                )
                / max(1, len(tracker._outcomes)),
                4,
            )
            if tracker._outcomes
            else 0.0
        ),
        "status": "ok",
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Seed Wave2 demo OutcomeTracker data (ADR-0193)"
    )
    ap.add_argument(
        "--data-dir",
        type=Path,
        default=Path("runtime/c2g/outcomes"),
        help="OutcomeTracker directory (default: runtime/c2g/outcomes)",
    )
    ap.add_argument(
        "--reset",
        action="store_true",
        help="Delete existing pitch-outcomes.yaml before seeding",
    )
    ap.add_argument("--json", action="store_true", help="JSON summary to stdout")
    args = ap.parse_args(argv)

    # Refuse .omo writes
    if ".omo" in args.data_dir.parts:
        print("❌ refuse data-dir under .omo/", file=sys.stderr)
        return 2

    summary = seed_demo_outcomes(args.data_dir, reset=args.reset)
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(
            f"[demo-seed] data_dir={summary['data_dir']} "
            f"seeded={len(summary['seeded'])} skipped={len(summary['skipped'])} "
            f"total={summary['pitch_count']} mean={summary['mean_success_score']}"
        )
        if summary["seeded"]:
            print("  +" + ", ".join(summary["seeded"]))
        if summary["skipped"]:
            print("  (skipped existing) " + ", ".join(summary["skipped"]))
        print("  next: python -m c2g.dashboard_export --data-dir", summary["data_dir"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
