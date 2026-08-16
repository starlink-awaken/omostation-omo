"""Wave 2 Phase C — C2G outcome → OMO governance *proposals* (no auto rule mutation).

ADR-0188. Reads predictive report, emits structured proposals. Optional broker
task creation via omo_client (planned tasks only). Never rewrites
x1-governance-policies or GaC rules automatically.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo._vendored.c2g.outcome_tracker import OutcomeTracker
from omo._vendored.c2g.predictive import PredictiveModel, outcomes_time_series, risk_heatmap


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_proposals(
    outcomes: dict[str, Any],
    *,
    horizon: int = 3,
) -> dict[str, Any]:
    series = outcomes_time_series(outcomes)
    forecast = PredictiveModel(horizon=horizon).fit_series(series)
    heat = risk_heatmap(outcomes)
    proposals: list[dict[str, Any]] = []

    critical = int((heat.get("totals") or {}).get("critical") or 0)
    elevated = int((heat.get("totals") or {}).get("elevated") or 0)
    trend = forecast.get("trend") or "flat"
    mean = float(forecast.get("mean") or 0.0)
    n = int(forecast.get("n") or 0)

    if critical >= 1:
        proposals.append(
            {
                "id": "prop-critical-pitches",
                "kind": "risk_attention",
                "priority": "P0",
                "title": f"{critical} critical pitch(es) in outcome heatmap",
                "rationale": "Wave2-B heatmap marked critical (low score and/or high fail rate)",
                "suggested_omo_action": "create_planned_task",
                "suggested_task": {
                    "title": f"[C2G feedback] Review {critical} critical pitch outcomes",
                    "priority": "P0",
                    "risk_level": "L2",
                    "description": (
                        "Auto-proposed from OutcomeTracker heatmap. "
                        "Inspect failed tasks and tighten pitch Acceptance/Risk sections."
                    ),
                },
            }
        )

    if trend == "declining" and n >= 3:
        proposals.append(
            {
                "id": "prop-declining-trend",
                "kind": "strategy_review",
                "priority": "P1",
                "title": "Pitch success trend declining",
                "rationale": f"EMA+linear slope trend=declining over n={n} (mean={mean})",
                "suggested_omo_action": "create_planned_task",
                "suggested_task": {
                    "title": "[C2G feedback] Strategy review — declining pitch success",
                    "priority": "P1",
                    "risk_level": "L1",
                    "description": (
                        "Forecast indicates declining success. Review Appetite sizing "
                        "and Upstream alignment before next pitch batch."
                    ),
                },
            }
        )

    if elevated >= 3 and critical == 0:
        proposals.append(
            {
                "id": "prop-elevated-cluster",
                "kind": "quality_watch",
                "priority": "P2",
                "title": f"{elevated} elevated-risk pitches clustered",
                "rationale": "Multiple mid-risk outcomes without critical; watch quality drift",
                "suggested_omo_action": "note_only",
                "suggested_task": None,
            }
        )

    if n == 0:
        proposals.append(
            {
                "id": "prop-empty-baseline",
                "kind": "bootstrap",
                "priority": "P3",
                "title": "No outcomes yet — baseline empty",
                "rationale": "Phase C needs OutcomeTracker data; record pitch lifecycles first",
                "suggested_omo_action": "none",
                "suggested_task": None,
            }
        )

    return {
        "phase": "wave2-c",
        "adr": "0188",
        "generated_at": _now(),
        "auto_mutate_rules": False,
        "forecast_summary": {
            "n": n,
            "mean": mean,
            "trend": trend,
            "horizon": horizon,
        },
        "heatmap_totals": heat.get("totals"),
        "proposal_count": len(proposals),
        "proposals": proposals,
        "status": "ok",
    }


def apply_proposals_as_tasks(
    proposals: dict[str, Any],
    omo_dir: Path,
    *,
    dry_run: bool = True,
) -> list[dict[str, Any]]:
    """Optionally materialize P0/P1 proposals as planned tasks via OMO broker.

    dry_run=True (default): only describe what would be created.
    dry_run=False: call create_planned_task_via_broker for each eligible proposal.
    """
    results: list[dict[str, Any]] = []
    for p in proposals.get("proposals") or []:
        if p.get("suggested_omo_action") != "create_planned_task":
            continue
        task = p.get("suggested_task") or {}
        if not task:
            continue
        entry: dict[str, Any] = {
            "proposal_id": p.get("id"),
            "title": task.get("title"),
            "dry_run": dry_run,
        }
        if dry_run:
            entry["would_create"] = True
            results.append(entry)
            continue
        try:
            from c2g.omo_client import create_planned_task_via_broker

            task_data = {
                "id": f"C2G-FB-{p.get('id', 'x')}",
                "title": task.get("title"),
                "description": task.get("description", ""),
                "priority": task.get("priority", "P2"),
                "risk_level": task.get("risk_level", "L1"),
                "status": "planned",
                "source": "c2g.governance_feedback",
            }
            created = create_planned_task_via_broker(
                omo_dir,
                task_data=task_data,
                source_ref="c2g:wave2-c:governance_feedback",
            )
            entry["created"] = True
            entry["result"] = created if isinstance(created, dict) else str(created)
        except Exception as e:  # noqa: BLE001  (per-entry defensive fallback)
            entry["created"] = False
            entry["error"] = f"{type(e).__name__}: {e}"[:200]
        results.append(entry)
    return results


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Wave2 Phase C: C2G → OMO governance proposals (ADR-0188)"
    )
    ap.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="OutcomeTracker data dir (default: runtime/c2g/outcomes under cwd)",
    )
    ap.add_argument("--horizon", type=int, default=3)
    ap.add_argument(
        "--omo-dir",
        type=Path,
        default=None,
        help="OMO dir for optional task materialization",
    )
    ap.add_argument(
        "--apply-tasks",
        action="store_true",
        help="Create planned tasks via OMO broker (default: dry-run proposals only)",
    )
    ap.add_argument(
        "--show-apply-plan",
        action="store_true",
        help="Include dry-run task plan in JSON without creating",
    )
    args = ap.parse_args(argv)

    data_dir = args.data_dir or Path("runtime/c2g/outcomes")
    tracker = OutcomeTracker(data_dir)
    report = build_proposals(tracker._outcomes, horizon=args.horizon)

    omo_dir = args.omo_dir
    if omo_dir is None:
        # best-effort workspace .omo
        cand = Path(".omo")
        omo_dir = cand if cand.is_dir() else Path(".omo")

    if args.apply_tasks or args.show_apply_plan:
        report["task_actions"] = apply_proposals_as_tasks(
            report,
            omo_dir,
            dry_run=not args.apply_tasks,
        )

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
