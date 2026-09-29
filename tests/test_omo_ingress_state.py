from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from omo.omo_ingress_state import sync_state_projection
from omo.omo_paths import STATE_ROOT_ENV

LEGACY_WRITE_PATHS = [
    ".omo/state/runtime/health.yaml",
    ".omo/state/system.yaml",
    ".omo/state/runtime/brief.md",
    ".omo/state/runtime/governance-data.json",
]


def _payloads(stamp: str) -> dict:
    return {
        "health_content": f'# generated_at: {stamp}\ngenerated_at: "{stamp}"\nhealth_score: 91\n',
        "system_updates": {"health_score": 91, "health_score_generated_at": stamp},
        "brief_content": f"# BRIEF.md\n\n> **Generated**: `{stamp}`\n",
        "governance_data": {
            "version": "1.0",
            "generated_at": stamp,
            "governance": {"health_score": 91},
            "debt": {"total_count": 0},
            "categories": {},
            "trend": [],
            "projects": {},
        },
    }


def _fake_checkout(root: Path) -> Path:
    """Build a FAKE checkout (``.omo/state/system.yaml`` only) and return its root."""
    state_dir = root / ".omo" / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "system.yaml").write_text(
        yaml.safe_dump({"current_phase": 42}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return root


def test_sync_state_projection_skips_timestamp_only_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)
    omo_dir = tmp_path / ".omo"
    state_dir = _fake_checkout(tmp_path) / ".omo" / "state"

    payloads = _payloads("2026-07-03T00:00:00Z")
    governance_data = payloads["governance_data"]
    report = sync_state_projection(
        tmp_path,
        health_content=payloads["health_content"],
        system_updates=payloads["system_updates"],
        brief_content=payloads["brief_content"],
        governance_data=governance_data,
    )

    assert report["changed_count"] == 4
    assert report["artifact_ref"]
    runtime_dir = state_dir / "runtime"
    assert (runtime_dir / "health.yaml").exists()
    assert (runtime_dir / "brief.md").exists()
    assert (runtime_dir / "governance-data.json").exists()
    # ADR-0129 Phase 2: the legacy mirrors are no longer written.
    assert not (state_dir / "health.yaml").exists()
    assert not (tmp_path / "BRIEF.md").exists()
    assert not (omo_dir / "_control").exists()

    governance_data["generated_at"] = "2026-07-03T00:01:00Z"
    payloads = _payloads("2026-07-03T00:01:00Z")
    second = sync_state_projection(
        tmp_path,
        health_content=payloads["health_content"],
        system_updates=payloads["system_updates"],
        brief_content=payloads["brief_content"],
        governance_data=governance_data,
    )

    assert second["changed_count"] == 0
    assert second["artifact_ref"] == ""
    written_governance = json.loads((runtime_dir / "governance-data.json").read_text(encoding="utf-8"))
    assert written_governance["generated_at"] == "2026-07-03T00:00:00Z"

    payloads = _payloads("2026-07-03T00:02:00Z")
    semantic_change = sync_state_projection(
        tmp_path,
        health_content="# generated_at: 2026-07-03T00:02:00Z\nhealth_score: 92\n",
        system_updates={
            "health_score": 92,
            "health_score_generated_at": "2026-07-03T00:02:00Z",
        },
        brief_content=payloads["brief_content"],
        governance_data=governance_data,
    )

    assert semantic_change["changed_count"] == 2
    assert semantic_change["artifact_ref"]


def test_undeclared_profile_writes_the_byte_identical_legacy_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADR-0456 C3 — with no profile declared every write path string matches pre-F1 exactly."""
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)
    checkout = _fake_checkout(tmp_path)

    report = sync_state_projection(checkout, **_payloads("2026-09-29T00:00:00Z"))

    assert [item["path"] for item in report["writes"]] == LEGACY_WRITE_PATHS
    # The two roots coincide, and the mirror root stays under the same checkout.
    assert report["state_root"] == report["code_root"] == str(tmp_path.resolve())
    assert (tmp_path / "runtime" / "omo" / "_delivery" / "ingress").is_dir()


def test_declared_profile_redirects_every_write_to_state_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADR-0456 C1/C2 — the checkout keeps only reads; projections and mirror root move."""
    checkout = _fake_checkout(tmp_path / "checkout")
    system_bytes = (checkout / ".omo" / "state" / "system.yaml").read_bytes()

    state_root = tmp_path / "state"
    monkeypatch.setenv(STATE_ROOT_ENV, str(state_root))

    report = sync_state_projection(checkout, **_payloads("2026-09-29T00:00:00Z"))

    assert report["changed_count"] == 4
    assert report["state_root"] == str(state_root)
    dev_state = state_root / ".omo" / "state"
    assert (dev_state / "runtime" / "health.yaml").is_file()
    assert (dev_state / "runtime" / "brief.md").is_file()
    assert (dev_state / "runtime" / "governance-data.json").is_file()
    assert (dev_state / "system.yaml").is_file()
    # Nothing touched the development checkout — neither its state plane nor a mirror root.
    assert (checkout / ".omo" / "state" / "system.yaml").read_bytes() == system_bytes
    assert not (checkout / ".omo" / "state" / "runtime").exists()
    assert not (checkout / "runtime").exists()
    # delivery / audit / trail / lock / mutation follow the same root (one audit chain).
    mirror = state_root / "runtime" / "omo" / "_delivery" / "ingress"
    assert list((mirror / "state").glob("state-sync-*.yaml"))
    assert (mirror / "ingress-audit.jsonl").is_file()
    assert (mirror / "ingress-trail.jsonl").is_file()
    assert (mirror / "ingress.lock").is_file()
    assert (state_root / "runtime" / "omo" / "change-log" / "mutations.jsonl").is_file()


def test_explicit_state_root_argument_wins_over_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkout = _fake_checkout(tmp_path / "checkout")
    monkeypatch.setenv(STATE_ROOT_ENV, str(tmp_path / "env-state"))
    explicit = tmp_path / "explicit-state"

    report = sync_state_projection(checkout, state_root=explicit, **_payloads("2026-09-29T00:00:00Z"))

    assert report["state_root"] == str(explicit)
    assert (explicit / ".omo" / "state" / "runtime" / "health.yaml").is_file()
    assert not (tmp_path / "env-state").exists()


def test_state_root_writes_stay_absolute_in_receipt_and_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADR-0456 C4 — writes outside the checkout are reported as absolute paths.

    Relativising them would make the audit reader believe the write landed in the
    working copy, which is the exact confusion this bet removes.
    """
    checkout = _fake_checkout(tmp_path / "checkout")
    state_root = tmp_path / "state"
    monkeypatch.setenv(STATE_ROOT_ENV, str(state_root))

    report = sync_state_projection(checkout, **_payloads("2026-09-29T00:00:00Z"))

    expected = [
        str(state_root / ".omo" / "state" / "runtime" / "health.yaml"),
        str(state_root / ".omo" / "state" / "system.yaml"),
        str(state_root / ".omo" / "state" / "runtime" / "brief.md"),
        str(state_root / ".omo" / "state" / "runtime" / "governance-data.json"),
    ]
    assert [item["path"] for item in report["writes"]] == expected

    artifact = next((state_root / "runtime" / "omo" / "_delivery" / "ingress" / "state").glob("state-sync-*.yaml"))
    payload = yaml.safe_load(artifact.read_text(encoding="utf-8"))
    assert payload["write_count"] == 4
    assert payload["changed_paths"] == expected
    assert all(Path(entry).is_absolute() for entry in payload["changed_paths"])
