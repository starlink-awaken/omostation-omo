"""Tests for the debt-dashboard mirror write in omo_debt.write_dashboard.

The .omo/_control/debt-dashboard/current.yaml file is a tracked mirror that
the gac state-freshness-check inspects. Without a mirror write, the
canonical .omo/debt/dashboard/current.yaml stays fresh but the tracked
mirror ages out — which is what produced the 25-day staleness in P79.

These tests use a temp workspace with both the canonical debt dir and
the _control/debt-dashboard dir pre-created.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

# Ensure omo package import works under test runner (PYTHONPATH already
# set by uv run --project, but make it explicit for module-level clarity).
import omo.omo_debt as debt_module  # noqa: E402
import omo.omo_debt_metrics as debt_metrics_mod  # noqa: E402

WORKSPACE = Path(__file__).resolve().parents[1]


@pytest.fixture
def synth_workspace(tmp_path: Path) -> Path:
    """Create a synthetic omo tree with the legacy _control/debt-dashboard dir.

    Layout:
      tmp_path/.omo/debt/{items,review-queue,action-packet,owner-routing,dispatch,reviews}
      tmp_path/.omo/_control/debt-dashboard/  (empty, mirror target)
    """
    omo = tmp_path / ".omo"
    (omo / "debt").mkdir(parents=True)
    (omo / "_control" / "debt-dashboard").mkdir(parents=True)
    return omo


def test_write_dashboard_mirrors_to_tracked_path(synth_workspace: Path):
    """write_dashboard must also write the tracked mirror when it exists."""
    metrics = debt_metrics_mod.DebtMetrics(
        debt_health=80.0,
        classification_entropy=0.1,
        state_entropy=0.2,
        pointer_entropy=0.3,
        time_entropy=0.4,
        backlog_pressure=0.5,
        coupling_load=0.6,
        debt_watchlist_count=1,
        debt_gate_count=0,
        watchlist_item_ids=(),
        gate_item_ids=(),
        closed_item_ids=(),
    )
    review_queue = {"due_now": [], "upcoming": []}
    now = "2026-08-22T14:00:00Z"

    debt_module.write_dashboard(synth_workspace, metrics, review_queue, now)

    canonical = synth_workspace / "debt" / "dashboard" / "current.yaml"
    mirror = synth_workspace / "_control" / "debt-dashboard" / "current.yaml"

    assert canonical.exists(), "canonical dashboard should be written"
    assert mirror.exists(), "tracked mirror must be written (P79 fix)"

    canonical_data = yaml.safe_load(canonical.read_text(encoding="utf-8"))
    mirror_data = yaml.safe_load(mirror.read_text(encoding="utf-8"))
    assert mirror_data["generated_at"] == canonical_data["generated_at"]
    assert mirror_data["debt_metrics"] == canonical_data["debt_metrics"]


def test_write_dashboard_skips_mirror_when_dir_absent(tmp_path: Path):
    """When the _control/debt-dashboard dir is absent, mirror is a no-op.

    The mirror path is git-tracked; if a fresh clone strips the dir, we
    should still write the canonical file without crashing.
    """
    omo = tmp_path / ".omo"
    (omo / "debt").mkdir(parents=True)
    # Note: no _control/debt-dashboard created

    metrics = debt_metrics_mod.DebtMetrics(
        debt_health=80.0,
        classification_entropy=0.0,
        state_entropy=0.0,
        pointer_entropy=0.0,
        time_entropy=0.0,
        backlog_pressure=0.0,
        coupling_load=0.0,
        debt_watchlist_count=0,
        debt_gate_count=0,
        watchlist_item_ids=(),
        gate_item_ids=(),
        closed_item_ids=(),
    )
    review_queue = {"due_now": [], "upcoming": []}

    debt_module.write_dashboard(omo, metrics, review_queue, "2026-08-22T14:00:00Z")

    assert (omo / "debt" / "dashboard" / "current.yaml").exists()
    assert not (omo / "_control" / "debt-dashboard" / "current.yaml").exists()
