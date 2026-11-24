from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest
import yaml

from omo.omo_debt_action_packet import build_action_packet
from omo.omo_debt_cli import main
from omo.omo_debt_lifecycle import schedule_item
from omo.omo_debt_registry import DebtItem
from omo.omo_debt_review_queue import build_review_queue


def _seed_empty_omo(tmp_path: Path) -> Path:
    omo_dir = tmp_path / ".omo"
    (omo_dir / "debt" / "items").mkdir(parents=True, exist_ok=True)
    truth_reg = omo_dir / "_truth" / "registry"
    truth_reg.mkdir(parents=True, exist_ok=True)
    (truth_reg / "debt.yaml").write_text("version: 3\nseed_items: []\n", encoding="utf-8")
    return omo_dir


@pytest.mark.parametrize(
    "argv_suffix",
    [
        ["schedule", "--id", "GHOST-ID", "--next-review-at", "2026-12-08"],
        ["reopen", "--id", "GHOST-ID"],
        ["escalate", "--id", "GHOST-ID", "--gate-level", "watchlist"],
        [
            "reclassify",
            "--id",
            "GHOST-ID",
            "--dimension",
            "governance",
            "--subdimension",
            "consistency",
        ],
    ],
)
def test_write_verbs_refuse_unknown_debt_id_without_minting_orphan(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    argv_suffix: list[str],
) -> None:
    omo_dir = _seed_empty_omo(tmp_path)
    rc = main([argv_suffix[0], "--omo-dir", str(omo_dir), *argv_suffix[1:]])
    captured = capsys.readouterr()

    assert rc == 1
    assert "GHOST-ID" in captured.out
    assert list((omo_dir / "debt" / "items").glob("*.yaml")) == []


def test_schedule_item_raises_file_not_found_for_unknown_id(tmp_path: Path) -> None:
    omo_dir = _seed_empty_omo(tmp_path)
    with pytest.raises(FileNotFoundError, match="NO-SUCH-DEBT"):
        schedule_item(
            omo_dir,
            "NO-SUCH-DEBT",
            "2026-12-08",
            lambda p: yaml.safe_load(p.read_text(encoding="utf-8")) if p.exists() else {},
            lambda p, d: p.write_text(yaml.safe_dump(d), encoding="utf-8"),
            "2026-11-24T00:00:00+00:00",
        )


def test_register_populates_default_and_explicit_review_dates(tmp_path: Path) -> None:
    omo_dir = _seed_empty_omo(tmp_path)

    rc_default = main(
        [
            "register",
            "--omo-dir",
            str(omo_dir),
            "--id",
            "DEFAULT-DATES",
            "--title",
            "Default dates",
            "--dimension",
            "governance",
            "--subdimension",
            "consistency",
            "--severity",
            "medium",
            "--owner",
            "governance-team",
        ]
    )
    assert rc_default == 0
    default_payload = yaml.safe_load((omo_dir / "debt" / "items" / "DEFAULT-DATES.yaml").read_text(encoding="utf-8"))
    opened_dt = datetime.fromisoformat(default_payload["opened_at"])
    assert default_payload["last_reviewed_at"] == default_payload["opened_at"][:10]
    assert default_payload["next_review_at"] == (opened_dt + timedelta(days=14)).date().isoformat()

    rc_explicit = main(
        [
            "register",
            "--omo-dir",
            str(omo_dir),
            "--id",
            "EXPLICIT-DATES",
            "--title",
            "Explicit dates",
            "--dimension",
            "governance",
            "--subdimension",
            "consistency",
            "--severity",
            "medium",
            "--owner",
            "governance-team",
            "--last-reviewed-at",
            "2026-11-20",
            "--next-review-at",
            "2026-12-01",
        ]
    )
    assert rc_explicit == 0
    explicit_payload = yaml.safe_load((omo_dir / "debt" / "items" / "EXPLICIT-DATES.yaml").read_text(encoding="utf-8"))
    assert explicit_payload["last_reviewed_at"] == "2026-11-20"
    assert explicit_payload["next_review_at"] == "2026-12-01"


def test_review_queue_and_action_packet_surface_out_of_window_watchlist_and_gate(
    tmp_path: Path,
) -> None:
    (tmp_path / "ev.md").write_text("ok\n", encoding="utf-8")

    def make_item(item_id: str, gate_level: str) -> DebtItem:
        return DebtItem(
            id=item_id,
            title=item_id,
            dimension="governance",
            subdimension="consistency",
            domain="workspace",
            scope="governance_kernel",
            severity="medium",
            weight=0.1,
            entropy_class="low",
            lifecycle_state="identified",
            owner="governance-team",
            affected_roots=(".omo",),
            evidence_refs=("ev.md",),
            mitigation_refs=(),
            opened_at="2026-11-10",
            last_reviewed_at="2026-11-24",
            next_review_at="2026-12-08",
            gate_level=gate_level,
            history=(),
        )

    queue = build_review_queue(
        (
            make_item("WATCH-LATER", "watchlist"),
            make_item("GATE-LATER", "gate"),
            make_item("PLAIN-LATER", "none"),
        ),
        now="2026-11-24T00:00:00Z",
        repo_root=tmp_path,
    )

    assert [e["id"] for e in queue["scheduled_later"]] == [
        "GATE-LATER",
        "WATCH-LATER",
        "PLAIN-LATER",
    ]
    assert queue["summary"]["scheduled_later_count"] == 3
    assert queue["summary"]["open_count"] == 3

    packet = build_action_packet(queue, now="2026-11-24T00:00:00Z")
    watch_entries = packet["lanes"]["watch_only"]
    assert [e["id"] for e in watch_entries] == ["GATE-LATER", "WATCH-LATER"]
    assert all(e["reason"] == "watchlist_scheduled_later" for e in watch_entries)
