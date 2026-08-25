from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import omo.workflow.cli as cli_mod
import omo.workflow.core as core_mod
import omo.workflow.lifecycle as lifecycle_mod
from omo.workflow.core import WorkflowError
from omo.workflow.lifecycle import spawn_run, start_run


@pytest.fixture()
def registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    (tmp_path / "runs").mkdir()
    (tmp_path / "ledger").mkdir()
    monkeypatch.setattr(core_mod, "WORKSPACE", tmp_path)
    monkeypatch.setattr(lifecycle_mod, "WORKSPACE", tmp_path)
    return {
        "runner": {"run_state_dir": "runs", "ledger_path": "ledger/events.jsonl"},
        "workflows": [
            {
                "id": "test-workflow",
                "title": "Test",
                "purpose": "test",
                "agents": {"test-agent": {"actor": "tester"}},
                "allowed_lanes": [],
                "lock_scopes": [],
                "phases": {},
            }
        ],
        "agent_profiles": {
            "test-agent": {
                "id": "test-agent",
                "actor": "tester",
                "allowed_workflows": ["*"],
            }
        },
    }


def _context() -> dict[str, str]:
    return {
        "actor": "tester",
        "profile": "test-agent",
        "project": "",
        "format": "openspec",
        "source_file": "",
        "run_id": "",
    }


def _workflow(registry: dict[str, Any]) -> dict[str, Any]:
    return registry["workflows"][0]


def _prepared_identity() -> dict[str, Any]:
    binding = {
        "spec_ref": "repo://docs/spec.md",
        "spec_version": "1.0.0",
        "content_digest": "sha256:" + "1" * 64,
        "decision_ref": "decision://accepted/BET-BOUND",
    }
    requirements = [
        {"capability_id": "skill:git-discipline", "operation": "load", "effect": "read_only"},
        {"capability_id": "workflow:bet-execution", "operation": "load", "effect": "read_only"},
    ]
    packet = {
        "packet_id": "WP-BET-BOUND",
        "schema_version": "work-packet/v2",
        "bet_id": "BET-BOUND",
        "spec_binding": binding,
        "capability_requirements": requirements,
    }
    canonical = json.dumps(requirements, sort_keys=True, separators=(",", ":"))
    return {
        "spec_binding": binding,
        "work_packet": packet,
        "work_packet_hash": "sha256:" + "2" * 64,
        "capability_requirements_digest": "sha256:" + hashlib.sha256(canonical.encode()).hexdigest(),
    }


def _preflight(run_id: str, identity: dict[str, Any]) -> dict[str, Any]:
    packet = identity["work_packet"]
    return {
        "requirements_digest": identity["capability_requirements_digest"],
        "binding": {
            "correlation_id": run_id,
            "workflow_run_id": run_id,
            "packet_id": packet["packet_id"],
            "packet_hash": identity["work_packet_hash"],
            "assignment_id": f"preflight:{run_id}:assignment",
            "dispatch_id": f"preflight:{run_id}:dispatch",
            "actor_id": "actor:test",
            "delivery_attempt_id": "attempt:test",
        },
        "receipts": [
            {
                "capability_id": requirement["capability_id"],
                "source_digest": "sha256:" + "3" * 64,
                "receipt_digest": "sha256:" + "4" * 64,
            }
            for requirement in packet["capability_requirements"]
        ],
        "invoked": False,
        "value_indicator_policy": False,
    }


def _install_prepared_identity(monkeypatch: pytest.MonkeyPatch, identity: dict[str, Any]) -> None:
    monkeypatch.setattr(lifecycle_mod, "_prepare_bet_execution", lambda _bet_id: identity)


def test_fresh_capability_identity_runs_preflight_before_any_state_write(
    registry: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = _prepared_identity()
    _install_prepared_identity(monkeypatch, identity)
    observations: list[tuple[str, str]] = []

    def provider(run_id: str, prepared: dict[str, Any]) -> dict[str, Any]:
        observations.append((run_id, "provider"))
        assert list((lifecycle_mod.WORKSPACE / "runs").glob("*.yaml")) == []
        assert not (lifecycle_mod.WORKSPACE / "ledger/events.jsonl").exists()
        return _preflight(run_id, prepared)

    record = start_run(
        registry,
        _workflow(registry),
        _context(),
        "capability preflight",
        False,
        False,
        bet_id="BET-BOUND",
        start_preflight=provider,
    )

    assert observations == [(record["run_id"], "provider")]
    assert record["capability_preflight"] == _preflight(record["run_id"], identity)


def test_fresh_capability_identity_without_provider_fails_before_side_effects(
    registry: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_prepared_identity(monkeypatch, _prepared_identity())

    with pytest.raises(WorkflowError, match="CAPABILITY_PREFLIGHT"):
        start_run(
            registry,
            _workflow(registry),
            _context(),
            "must not start",
            False,
            False,
            bet_id="BET-BOUND",
        )

    assert list((lifecycle_mod.WORKSPACE / "runs").glob("*.yaml")) == []
    assert not (lifecycle_mod.WORKSPACE / "ledger/events.jsonl").exists()


def test_preflight_result_is_strictly_redacted_and_causally_bound(
    registry: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = _prepared_identity()
    _install_prepared_identity(monkeypatch, identity)

    def provider(run_id: str, prepared: dict[str, Any]) -> dict[str, Any]:
        result = _preflight(run_id, prepared)
        result["private_receipt"] = "must be rejected"
        return result

    with pytest.raises(WorkflowError, match="CAPABILITY_PREFLIGHT"):
        start_run(
            registry,
            _workflow(registry),
            _context(),
            "must reject private result",
            False,
            False,
            bet_id="BET-BOUND",
            start_preflight=provider,
        )

    assert list((lifecycle_mod.WORKSPACE / "runs").glob("*.yaml")) == []
    assert not (lifecycle_mod.WORKSPACE / "ledger/events.jsonl").exists()


def test_inherited_exact_identity_is_byte_identical_and_does_not_rerun(
    registry: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = _prepared_identity()
    _install_prepared_identity(monkeypatch, identity)
    calls: list[str] = []

    def provider(run_id: str, prepared: dict[str, Any]) -> dict[str, Any]:
        calls.append(run_id)
        return _preflight(run_id, prepared)

    parent = start_run(
        registry,
        _workflow(registry),
        _context(),
        "parent",
        False,
        False,
        bet_id="BET-BOUND",
        start_preflight=provider,
    )

    class ContractError(RuntimeError):
        pass

    monkeypatch.setattr(
        lifecycle_mod,
        "_SPEC_BINDING_CONTRACT",
        SimpleNamespace(
            SpecBindingContractError=ContractError,
            validate_work_packet_run=lambda *args, **kwargs: None,
        ),
    )
    child = spawn_run(
        registry,
        parent["run_id"],
        _workflow(registry),
        _context(),
        "child",
        start_preflight=provider,
    )

    assert calls == [parent["run_id"]]
    for key in ("spec_binding", "work_packet", "work_packet_hash", "capability_requirements_digest", "capability_preflight"):
        assert child[key] == parent[key]


def test_cli_threads_start_preflight_into_start_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_start_run(*args: Any, **kwargs: Any) -> dict[str, Any]:
        captured["start_preflight"] = kwargs["start_preflight"]
        return {"run_id": "run-test"}

    monkeypatch.setattr(cli_mod, "start_run", fake_start_run)
    monkeypatch.setattr(cli_mod, "load_registry", lambda _path: {
        "runner": {},
        "workflows": [{"id": "test-workflow", "lock_scopes": [], "phases": {}}],
        "agent_profiles": {},
    })
    monkeypatch.setattr(cli_mod, "_load_chain_bind", lambda: None)

    provider = lambda run_id, identity: _preflight(run_id, identity)
    assert cli_mod.main(["start", "test-workflow", "--dry-run"], start_preflight=provider) == 0
    assert captured["start_preflight"] is provider
