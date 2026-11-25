"""Tests for omo_debt_cli evidence_refs/mitigation_refs, comment-safe registry append, and broker receipts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml

from omo.omo_debt_cli import main


def _seed_item(omo_dir: Path, debt_id: str = "TEST-DEBT-REFS") -> Path:
    item_path = omo_dir / "debt" / "items" / f"{debt_id}.yaml"
    item_path.parent.mkdir(parents=True, exist_ok=True)
    item_path.write_text(
        yaml.safe_dump(
            {
                "id": debt_id,
                "title": "Test debt refs",
                "lifecycle_state": "identified",
                "evidence_refs": ["docs/existing.md"],
                "mitigation_refs": [],
                "history": [],
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return item_path


def _read_receipts(log_path: Path) -> list[dict]:
    return [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_refs_subcommand_updates_refs_and_writes_sha256_receipt(tmp_path: Path, monkeypatch) -> None:
    omo_dir = tmp_path / ".omo"
    item_path = _seed_item(omo_dir)
    receipt_log = tmp_path / "custom-receipts.jsonl"
    monkeypatch.setenv("OMO_DEBT_RECEIPT_LOG", str(receipt_log))
    monkeypatch.setenv("AGENT_WORKFLOW_RUN_ID", "20261125T000000Z-governance-state-mutation-deadbeef")

    assert (
        main(
            [
                "refs",
                "--omo-dir",
                str(omo_dir),
                "--id",
                "TEST-DEBT-REFS",
                "--add-evidence",
                "bin/a.py",
                "--remove-evidence",
                "docs/existing.md",
                "--add-mitigation",
                "docs/plan.md",
                "--dry-run",
            ]
        )
        == 0
    )
    assert not receipt_log.exists()

    rc = main(
        [
            "refs",
            "--omo-dir",
            str(omo_dir),
            "--id",
            "TEST-DEBT-REFS",
            "--add-evidence",
            "bin/a.py",
            "--add-evidence",
            "bin/a.py",
            "--remove-evidence",
            "docs/existing.md",
            "--add-mitigation",
            "docs/plan.md",
            "--actor",
            "governance-agent",
        ]
    )
    assert rc == 0
    payload = yaml.safe_load(item_path.read_text(encoding="utf-8"))
    assert payload["evidence_refs"] == ["bin/a.py"]
    assert payload["mitigation_refs"] == ["docs/plan.md"]
    assert payload["history"][-1]["action"] == "update_refs"

    receipts = _read_receipts(receipt_log)
    assert len(receipts) == 1
    assert receipts[0]["path"] == ".omo/debt/items/TEST-DEBT-REFS.yaml"
    assert receipts[0]["sha256"] == hashlib.sha256(item_path.read_bytes()).hexdigest()
    assert receipts[0]["command"] == "refs"
    assert receipts[0]["run_id"] == "20261125T000000Z-governance-state-mutation-deadbeef"


def test_register_preserves_debt_yaml_comments_and_writes_both_receipts(tmp_path: Path, monkeypatch) -> None:
    omo_dir = tmp_path / ".omo"
    registry = omo_dir / "_truth" / "registry" / "debt.yaml"
    registry.parent.mkdir(parents=True, exist_ok=True)
    registry.write_text(
        "# top-level comment\n"
        "status: active\n"
        "version: 3\n"
        "# keep v3 rationale comment\n"
        "seed_items:\n"
        "- .omo/debt/items/EXISTING.yaml\n"
        "# trailing list comment\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("OMO_DEBT_RECEIPT_LOG", raising=False)

    rc = main(
        [
            "register",
            "--omo-dir",
            str(omo_dir),
            "--id",
            "NEW-COMMENT-SAFE",
            "--title",
            "Preserve registry comments",
            "--dimension",
            "governance",
            "--subdimension",
            "gate",
            "--severity",
            "medium",
            "--owner",
            "governance-team",
            "--actor",
            "governance-agent",
            "--evidence-ref",
            "bin/ssot/write-owner-audit.py",
            "--mitigation-ref",
            "docs/fix.md",
        ]
    )
    assert rc == 0

    item_path = omo_dir / "debt" / "items" / "NEW-COMMENT-SAFE.yaml"
    payload = yaml.safe_load(item_path.read_text(encoding="utf-8"))
    assert payload["evidence_refs"] == ["bin/ssot/write-owner-audit.py"]
    assert payload["mitigation_refs"] == ["docs/fix.md"]

    reg_text = registry.read_text(encoding="utf-8")
    assert "# keep v3 rationale comment" in reg_text
    assert "# trailing list comment" in reg_text
    reg_data = yaml.safe_load(reg_text)
    assert reg_data["seed_items"] == [
        ".omo/debt/items/EXISTING.yaml",
        ".omo/debt/items/NEW-COMMENT-SAFE.yaml",
    ]

    default_log = omo_dir / "_delivery" / "omo-debt-writes.jsonl"
    receipts = _read_receipts(default_log)
    by_path = {entry["path"]: entry for entry in receipts}
    assert (
        by_path[".omo/debt/items/NEW-COMMENT-SAFE.yaml"]["sha256"] == hashlib.sha256(item_path.read_bytes()).hexdigest()
    )
    assert by_path[".omo/_truth/registry/debt.yaml"]["sha256"] == hashlib.sha256(registry.read_bytes()).hexdigest()


def test_close_writes_evidence_refs_and_matching_receipt(tmp_path: Path, monkeypatch) -> None:
    omo_dir = tmp_path / ".omo"
    item_path = _seed_item(omo_dir, "CLOSE-REFS-TEST")
    monkeypatch.setenv("AGENT_WORKFLOW_RUN_ID", "20261125T080000Z-governance-state-mutation-12345678")

    rc = main(
        [
            "close",
            "--omo-dir",
            str(omo_dir),
            "--id",
            "CLOSE-REFS-TEST",
            "--actor",
            "governance-agent",
            "--close-reason",
            "fixed and verified",
            "--resolution-evidence",
            "verified end to end with unit tests and pre-push check",
            "--evidence-ref",
            "bin/ssot/write-owner-audit.py",
            "--mitigation-ref",
            "docs/runbook.md",
        ]
    )
    assert rc == 0

    payload = yaml.safe_load(item_path.read_text(encoding="utf-8"))
    assert payload["lifecycle_state"] == "closed"
    assert payload["evidence_refs"] == ["docs/existing.md", "bin/ssot/write-owner-audit.py"]
    assert payload["mitigation_refs"] == ["docs/runbook.md"]

    default_log = omo_dir / "_delivery" / "omo-debt-writes.jsonl"
    receipts = _read_receipts(default_log)
    assert len(receipts) == 1
    assert receipts[0]["command"] == "close"
    assert receipts[0]["sha256"] == hashlib.sha256(item_path.read_bytes()).hexdigest()
    assert receipts[0]["run_id"] == "20261125T080000Z-governance-state-mutation-12345678"
