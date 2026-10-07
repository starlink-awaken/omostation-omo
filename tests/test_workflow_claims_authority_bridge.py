from __future__ import annotations

import ast
import hashlib
import inspect
import io
import json
import os
import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import yaml

import omo.workflow.claims_authority as claims_authority
import omo.workflow.lifecycle as lifecycle
from omo.workflow.claims_authority import (
    AuthorityError,
    ShadowDecision,
    V1Decision,
    _AuthorityStore,
    canonical_digest,
    canonical_json,
    compare_decisions,
    resolve_authority_paths,
)
from omo.workflow.core import WorkflowError


def _digest(seed: str) -> str:
    return f"sha256:{seed * 64}"


def valid_observe_request(*, request_id: str | None = None) -> dict[str, object]:
    return {
        "schema": "claim-mutation-envelope/v2",
        "operation": "observe-claim",
        "request_id": request_id or str(uuid4()),
        "actor_id": "agent-a",
        "delivery_attempt_id": "attempt-a",
        "repository": "starlink-awaken/omostation",
        "clone_root_digest": _digest("1"),
        "branch": "agent/agent-a--attempt-a",
        "clone_identity_digest": _digest("2"),
        "manifest_digest": _digest("3"),
        "readiness_digest": _digest("4"),
        "frozen_base": "a" * 40,
        "head_oid": "b" * 40,
        "bet_id": "BET-Y1Q4-T10-145",
        "work_packet_id": "WP-BET-Y1Q4-T10-145",
        "work_packet_digest": _digest("5"),
        "spec_ref": "repo://docs/superpowers/specs/claims-authority.md",
        "spec_digest": _digest("6"),
        "affected_graph_digest": _digest("7"),
        "requested_paths_digest": _digest("8"),
        "expected_claim_version": 0,
        "run_id": "run-a",
        "claim_ordinal": 0,
        "v1_claim_digest": _digest("b"),
        "v1_run_digest": _digest("c"),
        "v1_lock_set_digest": _digest("d"),
        "v1_decision": {"decision": "deny", "code": "claims_authority_mismatch"},
    }


def valid_activation_request(store: _AuthorityStore, *, request_id: str | None = None) -> dict[str, object]:
    if store.authority_id == "omo-claims-authority-r0":
        return valid_production_activation_request(store, request_id=request_id)
    descriptor: dict[str, object] = {
        "schema": "claims-authority-descriptor/v2",
        "authority_id": store.authority_id,
        "security_level": "cooperative-r0",
        "operating_mode": "shadow",
        "repository": "starlink-awaken/omostation",
        "broker_transport": "stdio",
        "store_identity": "test-store",
        "critical_dependency_closure_digest": _digest("c"),
        "accepted_clone_identity_schemas": ["agent-clone-identity/v2"],
        "v1_compatibility": "legacy-effective-shadow",
    }
    descriptor["digest"] = canonical_digest(descriptor)
    return {
        "schema": "claim-mutation-envelope/v2",
        "operation": "activate-shadow",
        "request_id": request_id or str(uuid4()),
        "authority_id": store.authority_id,
        "expected_authority_epoch": 0,
        "expected_state": "unactivated",
        "descriptor": descriptor,
    }


def valid_production_activation_request(
    store: _AuthorityStore,
    *,
    request_id: str | None = None,
) -> dict[str, object]:
    from omo.workflow import claims_verifiers

    descriptor: dict[str, object] = {
        "schema": "claims-authority-descriptor/v2",
        "authority_id": store.authority_id,
        "security_level": "cooperative-r0",
        "operating_mode": "shadow",
        "repository": "starlink-awaken/omostation",
        "broker_transport": "stdio",
        "store_identity": "production-store",
        "critical_dependency_closure_digest": claims_verifiers.production_verifier_closure_digest(),
        "accepted_clone_identity_schemas": ["agent-clone-identity/v2"],
        "v1_compatibility": "legacy-effective-shadow",
        "operator_authorization_verifier_digest": claims_verifiers.operator_authorization_verifier_digest(),
        "stopped_process_verifier_digest": claims_verifiers.stopped_process_verifier_digest(),
    }
    descriptor["digest"] = canonical_digest(descriptor)
    return {
        "schema": "claim-mutation-envelope/v2",
        "operation": "activate-shadow",
        "request_id": request_id or str(uuid4()),
        "authority_id": store.authority_id,
        "expected_authority_epoch": 0,
        "expected_state": "unactivated",
        "descriptor": descriptor,
    }


@pytest.fixture(autouse=True)
def safe_sqlite_wal_runtime_for_test(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(claims_authority, "wal_allowed_for_current", lambda: True)


@pytest.fixture
def store(tmp_path: Path) -> _AuthorityStore:
    return _AuthorityStore.connect_for_test(
        tmp_path / "authority",
        authority_id=f"test:{uuid4()}",
    )


def test_ac01_canonical_json_and_digest_are_stable() -> None:
    left = {"b": 2, "a": 1, "digest": "ignored"}
    right = {"a": 1, "b": 2}

    assert canonical_json(left) == '{"a":1,"b":2}'
    assert canonical_digest(left) == canonical_digest(right)
    assert canonical_digest(right).startswith("sha256:")
    assert lifecycle._authority_digest(left) == canonical_digest(left)


def test_new_run_is_claims_eligible_before_its_first_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = {
        "runner": {},
        "agent_profiles": {
            "governance-agent": {
                "id": "governance-agent",
                "actor": "agent-a",
                "allowed_workflows": ["project-code-change"],
            }
        },
    }
    workflow = {
        "id": "project-code-change",
        "title": "Project code change",
        "purpose": "test",
        "agents": {},
        "allowed_lanes": ["governance_code"],
        "lock_scopes": [],
        "phases": {},
    }
    record = lifecycle.start_run(
        registry,
        workflow,
        {
            "actor": "agent-a",
            "profile": "governance-agent",
            "project": "",
            "format": "openspec",
            "source_file": "",
            "run_id": "",
        },
        "test initial Claims eligibility",
        True,
        False,
    )

    assert record["claims"] == []
    record.update(
        {
            "bet_id": "BET-Y2Q2-T4-01",
            "spec_binding": {"spec_ref": "repo://spec.md"},
            "work_packet": {"packet_id": "WP-BET-Y2Q2-T4-01"},
            "work_packet_hash": _digest("1"),
        }
    )
    monkeypatch.setattr(lifecycle, "_authority_has_clone_identity", lambda _workspace: True)
    assert lifecycle._authority_run_is_eligible(record) is True


def test_red_frozen_child_public_api_is_complete_and_explicit() -> None:
    assert claims_authority.__all__ == (
        "AuthorityError",
        "AuthorityPaths",
        "V1Decision",
        "ShadowDecision",
        "canonical_json",
        "canonical_digest",
        "resolve_authority_paths",
        "dispatch_request",
        "observe_claim",
        "begin_claim_mutation",
        "settle_claim_mutation",
        "mark_claim_mutation_operator_required",
        "resolve_claim_mutation_unknown",
        "activate_shadow",
        "issue_legacy_fence",
        "enter_legacy_publishing",
        "settle_legacy_publication",
        "mark_legacy_operator_required",
        "resolve_legacy_unknown",
        "authority_status",
        "evaluate_graduation",
        "cli_main",
    )
    assert all(callable(getattr(claims_authority, name)) for name in claims_authority.__all__)


def test_red_dispatch_uses_frozen_verb_and_canonical_response_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = {"schema": "claim-mutation-envelope/v2", "request_id": str(uuid4())}
    calls: list[dict[str, object]] = []

    def fake_observe(payload: dict[str, object]) -> dict[str, object]:
        calls.append(payload)
        return {"sequence": 7, "receipt_digest": _digest("a")}

    monkeypatch.setattr(claims_authority, "observe_claim", fake_observe, raising=False)
    response = claims_authority.dispatch_request("observe-claim", request)

    assert calls == [request]
    assert response == {
        "ok": True,
        "schema": "claims-authority-response/v2",
        "authority_id": "omo-claims-authority-r0",
        "sequence": 7,
        "result": {"sequence": 7, "receipt_digest": _digest("a")},
        "error": None,
    }
    with pytest.raises(AuthorityError, match="REQUEST_SCHEMA_INVALID"):
        claims_authority.dispatch_request("status", request)
    with pytest.raises(AuthorityError, match="REQUEST_SCHEMA_INVALID"):
        claims_authority.dispatch_request("observe-claim", None)
    with pytest.raises(AuthorityError, match="REQUEST_SCHEMA_INVALID"):
        claims_authority.dispatch_request("not-a-verb", request)


def test_green_child_cli_emits_exactly_one_canonical_response_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = {"schema": "claim-mutation-envelope/v2", "request_id": str(uuid4())}
    stdin = io.StringIO(json.dumps(request, sort_keys=True, separators=(",", ":")))
    stdout = io.StringIO()
    monkeypatch.setattr(claims_authority.sys, "stdin", stdin)
    monkeypatch.setattr(claims_authority.sys, "stdout", stdout)
    monkeypatch.setattr(
        claims_authority,
        "dispatch_request",
        lambda verb, body: {
            "ok": True,
            "schema": "claims-authority-response/v2",
            "authority_id": "omo-claims-authority-r0",
            "sequence": 3,
            "result": {"verb": verb, "request_id": body["request_id"]},
            "error": None,
        },
    )

    assert claims_authority.cli_main(["observe-claim", "--request-json", "-"]) == 0
    lines = stdout.getvalue().splitlines()
    assert len(lines) == 1
    assert lines[0] == json.dumps(json.loads(lines[0]), sort_keys=True, separators=(",", ":"))


def test_red_request_id_reuse_with_changed_payload_is_denied(store: _AuthorityStore) -> None:
    first = valid_observe_request(request_id="00000000-0000-4000-8000-000000000001")
    store.observe_claim(first)
    changed = {**first, "head_oid": "c" * 40}

    with pytest.raises(AuthorityError, match="REQUEST_ID_REUSE_MISMATCH"):
        store.observe_claim(changed)


def test_red_unknown_request_fields_are_rejected_before_store_mutation(store: _AuthorityStore) -> None:
    request = {**valid_observe_request(), "unexpected_authority_hint": "attacker"}
    count_before = store.scalar("SELECT COUNT(*) FROM receipts")

    with pytest.raises(AuthorityError, match="REQUEST_SCHEMA_INVALID"):
        store.observe_claim(request)

    assert store.scalar("SELECT COUNT(*) FROM receipts") == count_before


@pytest.mark.parametrize("name", ["HOME", "WORKSPACE_ROOT", "OMO_COORDINATION_DB", "CLAIMS_ROOT"])
def test_red_caller_cannot_redirect_production_store(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    monkeypatch.setenv(name, "/tmp/attacker")

    paths = resolve_authority_paths()

    account_home = Path(claims_authority.pwd.getpwuid(os.getuid()).pw_dir)
    assert paths.integration_root == account_home / "Workspace"
    assert paths.store == account_home / "agents/_shared/runtime/omo-claims-authority-r0/store.sqlite3"


def test_red_authority_root_is_account_resolved(tmp_path: Path) -> None:
    paths = resolve_authority_paths()
    account_home = Path(claims_authority.pwd.getpwuid(os.getuid()).pw_dir)

    assert paths.account_home == account_home
    assert paths.integration_root == account_home / "Workspace"
    with pytest.raises(AuthorityError, match="IDENTITY_MISMATCH"):
        _AuthorityStore._connect_new(
            tmp_path / "external-authority",
            "omo-claims-authority-r0-copy",
            test_only=False,
        )


@pytest.mark.parametrize("scenario", ["mode", "owner"])
def test_red_authority_paths_reject_wrong_owner_or_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
) -> None:
    runtime = tmp_path / "agents/_shared/runtime/omo-claims-authority-r0"
    runtime.mkdir(parents=True)
    runtime.chmod(0o777)
    monkeypatch.setattr(
        claims_authority.pwd,
        "getpwuid",
        lambda _uid: SimpleNamespace(pw_dir=str(tmp_path)),
    )
    if scenario == "owner":
        runtime.chmod(0o700)
        actual_uid = os.getuid()
        monkeypatch.setattr(claims_authority.os, "getuid", lambda: actual_uid + 1)

    with pytest.raises(AuthorityError, match="AUTHORITY_STORE_UNSAFE"):
        resolve_authority_paths()


def test_red_authority_paths_reject_every_symlink_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_parent = tmp_path / "agents/_shared/runtime"
    runtime_parent.mkdir(parents=True)
    target = tmp_path / "redirected"
    target.mkdir()
    (runtime_parent / "omo-claims-authority-r0").symlink_to(target, target_is_directory=True)
    monkeypatch.setattr(claims_authority.pwd, "getpwuid", lambda _uid: SimpleNamespace(pw_dir=str(tmp_path)))

    with pytest.raises(AuthorityError, match="AUTHORITY_STORE_UNSAFE"):
        resolve_authority_paths()


def test_ac02_store_pragmas_and_atomic_sequence(store: _AuthorityStore) -> None:
    assert store.scalar("PRAGMA journal_mode").lower() == "wal"
    assert store.scalar("PRAGMA synchronous") == 2
    assert store.scalar("PRAGMA foreign_keys") == 1
    assert store.scalar("PRAGMA busy_timeout") == 5000

    r1 = store.observe_claim(valid_observe_request())
    r2 = store.observe_claim(valid_observe_request())

    assert r1["sequence"] == 1
    assert r2["sequence"] == 2
    assert r2["previous_receipt_digest"] == r1["receipt_digest"]


def test_red_sqlite_user_version_one_matches_frozen_wave_a_schema(store: _AuthorityStore) -> None:
    expected = {
        "authority_meta": {
            "authority_id",
            "epoch",
            "operating_mode",
            "descriptor_digest",
            "last_sequence",
            "last_receipt_digest",
            "last_broker_time",
        },
        "requests": {
            "authority_id",
            "request_id",
            "request_digest",
            "response_json",
            "created_at",
        },
        "receipts": {
            "authority_id",
            "sequence",
            "receipt_id",
            "previous_receipt_digest",
            "receipt_json",
            "receipt_digest",
            "recorded_at",
        },
        "claims": {
            "claim_id",
            "run_id",
            "authority_claim_version",
            "authority_lease_epoch",
            "state",
            "intent_or_fence_id",
            "expires_at",
            "identity_digest",
            "v1_snapshot_digest",
        },
        "claim_mutation_batches": {
            "mutation_id",
            "run_id",
            "operation",
            "settlement_request_id",
            "expected_v1_run_digest",
            "expected_v1_lockset_digest",
            "state",
            "result_v1_run_digest",
            "result_v1_lockset_digest",
            "outcome",
            "authorization_digest",
            "operator_required",
            "operator_required_at",
            "mutation_process_proof_digest",
            "observed_v1_state_digest",
            "created_at",
            "settled_at",
        },
        "claim_mutation_members": {
            "mutation_id",
            "claim_id",
            "expected_authority_claim_version",
            "expected_authority_lease_epoch",
        },
        "legacy_fences": {
            "fence_id",
            "epoch",
            "claim_id",
            "request_digest",
            "settlement_request_id",
            "state",
            "expected_remote_oid",
            "remote_ref_digest",
            "expires_at",
            "outcome",
            "observed_remote_oid",
            "settlement_digest",
            "operator_required",
            "operator_required_at",
            "operator_authorization_digest",
            "effect_process_proof_digest",
        },
        "activation": {
            "authority_id",
            "operating_mode",
            "activation_state",
            "descriptor_digest",
            "activated_at",
            "activation_receipt_digest",
        },
    }
    actual_tables = {
        str(row[0])
        for row in store._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }

    assert store.scalar("PRAGMA user_version") == 1
    assert actual_tables == set(expected)
    for table, columns in expected.items():
        actual_columns = {str(row[1]) for row in store._connection.execute(f"PRAGMA table_info({table})")}
        assert actual_columns == columns


def test_red_unsafe_sqlite_wal_admission_has_zero_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority_dir = tmp_path / "authority"
    monkeypatch.setattr(claims_authority, "wal_allowed_for_current", lambda: False, raising=False)

    with pytest.raises(AuthorityError, match="AUTHORITY_STORE_UNSAFE"):
        _AuthorityStore.connect_for_test(authority_dir, authority_id=f"test:{uuid4()}")

    assert not authority_dir.exists()

    monkeypatch.setattr(claims_authority, "wal_allowed_for_current", lambda: True, raising=False)
    existing_dir = tmp_path / "existing-authority"
    existing_dir.mkdir(mode=0o700)
    existing_store = existing_dir / "store.sqlite3"
    connection = sqlite3.connect(existing_store)
    connection.execute("PRAGMA user_version=1")
    connection.execute("CREATE TABLE sentinel(value TEXT NOT NULL)")
    connection.execute("INSERT INTO sentinel(value) VALUES('preserve')")
    connection.commit()
    connection.close()
    existing_store.chmod(0o600)
    paths = claims_authority.AuthorityPaths(
        account_home=tmp_path,
        integration_root=tmp_path / "Workspace",
        authority_dir=existing_dir,
        store=existing_store,
        high_water=existing_dir / "high-water.json",
        backups=existing_dir / "backups",
        activation_witness=existing_dir / "activation-witness.json",
    )
    before = existing_store.read_bytes()

    with pytest.raises(AuthorityError, match="AUTHORITY_STORE_UNSAFE"):
        _AuthorityStore._connect_existing(paths, "omo-claims-authority-r0")

    assert existing_store.read_bytes() == before
    assert sorted(path.name for path in existing_dir.iterdir()) == ["store.sqlite3"]


def _write_witness(paths: claims_authority.AuthorityPaths, *, state: str, sequence: int = 0) -> None:
    paths.authority_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    witness: dict[str, object] = {
        "schema": "claims-activation-witness/v1",
        "authority_id": "omo-claims-authority-r0",
        "state": state,
        "sequence": sequence,
        "descriptor_digest": None if state == "unactivated" else _digest("9"),
        "activation_receipt_digest": None if state == "unactivated" else _digest("a"),
    }
    witness["digest"] = canonical_digest(witness)
    paths.activation_witness.write_text(
        json.dumps(witness, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    paths.activation_witness.chmod(0o600)


def test_green_broker_unavailable_pristine_or_unactivated_preserves_v1_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(claims_authority.pwd, "getpwuid", lambda _uid: SimpleNamespace(pw_dir=str(tmp_path)))
    paths = resolve_authority_paths()

    pristine = claims_authority._broker_unavailable_policy(paths)
    assert pristine == {"allow_v1": True, "code": "not_activated", "witness_state": "pristine"}
    assert not paths.authority_dir.exists()

    _write_witness(paths, state="unactivated")
    unactivated = claims_authority._broker_unavailable_policy(paths)
    assert unactivated == {"allow_v1": True, "code": "not_activated", "witness_state": "unactivated"}


@pytest.mark.parametrize(
    ("case", "state"),
    [
        ("prepared", "prepared"),
        ("active", "shadow-active"),
        ("invalid_state", "invalid"),
        ("rollback", "unactivated"),
        ("highwater_digest", "unactivated"),
        ("missing_after_init", None),
        ("malformed", "malformed"),
    ],
)
def test_red_broker_unavailable_prepared_active_invalid_rollback_blocks_v1_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    state: str | None,
) -> None:
    monkeypatch.setattr(claims_authority.pwd, "getpwuid", lambda _uid: SimpleNamespace(pw_dir=str(tmp_path)))
    paths = resolve_authority_paths()
    paths.authority_dir.mkdir(parents=True, mode=0o700)

    if case == "missing_after_init":
        paths.store.touch(mode=0o600)
    elif case == "malformed":
        paths.activation_witness.write_text("{", encoding="utf-8")
        paths.activation_witness.chmod(0o600)
    else:
        assert state is not None
        _write_witness(paths, state=state)
        if case == "rollback":
            paths.high_water.write_text(json.dumps({"sequence": 1}), encoding="utf-8")
            paths.high_water.chmod(0o600)
        elif case == "highwater_digest":
            paths.high_water.write_text(
                json.dumps(
                    {
                        "schema": "claims-authority-high-water/v1",
                        "authority_id": "omo-claims-authority-r0",
                        "sequence": 0,
                        "receipt_digest": None,
                        "descriptor_digest": None,
                        "digest": _digest("0"),
                    }
                ),
                encoding="utf-8",
            )
            paths.high_water.chmod(0o600)

    with pytest.raises(AuthorityError, match="AUTHORITY_ACTIVATION_WITNESS_INVALID"):
        claims_authority._broker_unavailable_policy(paths)


def test_ac14_store_initialization_writes_unactivated_witness(store: _AuthorityStore) -> None:
    witness = json.loads(store.test_paths.activation_witness.read_text(encoding="utf-8"))
    high_water = json.loads(store.test_paths.high_water.read_text(encoding="utf-8"))

    assert witness["state"] == "unactivated"
    assert witness["sequence"] == 0
    assert witness["digest"] == canonical_digest(witness)
    assert high_water["sequence"] == 0


@pytest.mark.parametrize("checkpoint", ["prepared", "db_commit", "high_water", "active"])
def test_red_activation_crash_boundaries_require_same_receipt_reconciliation(
    store: _AuthorityStore,
    monkeypatch: pytest.MonkeyPatch,
    checkpoint: str,
) -> None:
    class SimulatedCrashError(RuntimeError):
        pass

    def crash_at(stage: str) -> None:
        if stage == checkpoint:
            raise SimulatedCrashError(stage)

    monkeypatch.setattr(claims_authority, "_activation_checkpoint", crash_at, raising=False)
    request = valid_activation_request(store)

    with pytest.raises(SimulatedCrashError, match=checkpoint):
        store.activate_shadow(request)

    with pytest.raises(AuthorityError, match="AUTHORITY_ACTIVATION_WITNESS_INVALID"):
        claims_authority._broker_unavailable_policy(store.test_paths)

    if checkpoint == "prepared":
        with pytest.raises(AuthorityError, match="AUTHORITY_ACTIVATION_WITNESS_INVALID"):
            store.reconcile_activation_witness()
    else:
        reconciled = store.reconcile_activation_witness()
        assert reconciled["state"] == "shadow-active"
        assert reconciled["descriptor_digest"] == request["descriptor"]["digest"]


def test_green_broker_reopen_reconciles_exact_committed_activation_tail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    production = _AuthorityStore._connect_new(
        tmp_path / "authority",
        "omo-claims-authority-r0",
        test_only=False,
    )
    request = valid_activation_request(production)

    def crash_after_commit(stage: str) -> None:
        if stage == "db_commit":
            raise RuntimeError("crash:db_commit")

    monkeypatch.setattr(claims_authority, "_activation_checkpoint", crash_after_commit)
    with pytest.raises(RuntimeError, match="crash:db_commit"):
        production.activate_shadow(request)
    production._connection.close()

    reopened = _AuthorityStore._connect_existing(
        production.test_paths,
        "omo-claims-authority-r0",
    )
    try:
        assert reopened.authority_status()["activation_state"] == "shadow-active"
        witness = json.loads(reopened.test_paths.activation_witness.read_text(encoding="utf-8"))
        assert witness["state"] == "shadow-active"
        assert reopened.verify_high_water()["database_sequence"] == 1
    finally:
        reopened._connection.close()


def test_red_production_store_reopens_after_post_activation_receipt(tmp_path: Path) -> None:
    production = _AuthorityStore._connect_new(
        tmp_path / "authority",
        "omo-claims-authority-r0",
        test_only=False,
    )
    production.activate_shadow(valid_activation_request(production))
    production.observe_claim(valid_observe_request())
    paths = production.test_paths
    production._connection.close()

    reopened = _AuthorityStore._connect_existing(paths, "omo-claims-authority-r0")
    try:
        status = reopened.authority_status()
        assert status["activation_state"] == "shadow-active"
        assert status["sequence"] == 2
    finally:
        reopened._connection.close()


def test_red_production_activation_requires_bound_verifier_digests(tmp_path: Path) -> None:
    production = _AuthorityStore._connect_new(
        tmp_path / "authority",
        "omo-claims-authority-r0",
        test_only=False,
    )
    missing = valid_activation_request(production)
    descriptor = dict(missing["descriptor"])
    descriptor.pop("operator_authorization_verifier_digest")
    descriptor.pop("stopped_process_verifier_digest")
    descriptor["critical_dependency_closure_digest"] = _digest("c")
    descriptor["digest"] = canonical_digest(descriptor)
    missing["descriptor"] = descriptor
    with pytest.raises(AuthorityError, match="AUTHORITY_DESCRIPTOR_MISMATCH"):
        production.activate_shadow(missing)

    unbound = valid_production_activation_request(production)
    unbound_descriptor = dict(unbound["descriptor"])
    unbound_descriptor["operator_authorization_verifier_digest"] = _digest("e")
    unbound_descriptor["digest"] = canonical_digest(unbound_descriptor)
    unbound["descriptor"] = unbound_descriptor
    with pytest.raises(AuthorityError, match="AUTHORITY_DESCRIPTOR_MISMATCH"):
        production.activate_shadow(unbound)

    witness = json.loads(production.test_paths.activation_witness.read_text(encoding="utf-8"))
    assert witness["state"] == "unactivated"
    assert production.authority_status()["activation_state"] == "unactivated"
    assert production.authority_status()["sequence"] == 0


def test_green_repeated_activation_request_returns_same_receipt(store: _AuthorityStore) -> None:
    request = valid_activation_request(store)

    first = store.activate_shadow(request)
    second = store.activate_shadow(request)

    assert second == first
    assert store.scalar("SELECT COUNT(*) FROM activation") == 1
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM receipts WHERE authority_id=?",
            (store.authority_id,),
        ).fetchone()[0]
        == 1
    )


@pytest.mark.parametrize("case", ["highwater_ahead", "highwater_digest"])
def test_red_store_chain_or_highwater_drift_fails_closed(
    store: _AuthorityStore,
    case: str,
) -> None:
    receipt = store.observe_claim(valid_observe_request())
    if case == "highwater_ahead":
        payload = {"sequence": receipt["sequence"] + 2}
    else:
        payload = json.loads(store.test_paths.high_water.read_text(encoding="utf-8"))
        payload["digest"] = _digest("0")
    store.test_paths.high_water.write_text(json.dumps(payload), encoding="utf-8")
    store.test_paths.high_water.chmod(0o600)

    with pytest.raises(AuthorityError, match="AUTHORITY_HIGHWATER_ROLLBACK"):
        store.verify_high_water()


def test_red_receipt_chain_tamper_fails_before_any_new_mutation(store: _AuthorityStore) -> None:
    store.observe_claim(valid_observe_request())
    store.observe_claim({**valid_observe_request(), "request_id": str(uuid4()), "run_id": "run-b"})
    receipt = store._connection.execute(
        "SELECT receipt_json FROM receipts WHERE authority_id=? AND sequence=1",
        (store.authority_id,),
    ).fetchone()
    assert receipt is not None
    tampered = json.loads(receipt["receipt_json"])
    tampered["run_id"] = "tampered"
    store._connection.execute(
        "UPDATE receipts SET receipt_json=? WHERE authority_id=? AND sequence=1",
        (
            json.dumps(tampered, sort_keys=True, separators=(",", ":")),
            store.authority_id,
        ),
    )
    count_before = store.scalar("SELECT COUNT(*) FROM receipts")

    with pytest.raises(AuthorityError, match="AUTHORITY_STORE_CORRUPT"):
        store.verify_high_water()
    with pytest.raises(AuthorityError, match="AUTHORITY_STORE_CORRUPT"):
        store.observe_claim({**valid_observe_request(), "request_id": str(uuid4()), "run_id": "run-c"})

    assert store.scalar("SELECT COUNT(*) FROM receipts") == count_before


def _append_test_receipts_with_raw_sqlite(store: _AuthorityStore, *, count: int) -> None:
    sequence, previous_digest = store._database_tip()
    for offset in range(1, count + 1):
        receipt: dict[str, object] = {
            "schema": "claims-authority-receipt/v2",
            "authority_id": store.authority_id,
            "security_level": "R0_COOPERATIVE",
            "publishable": False,
            "operation": "test-crash-tail",
            "sequence": sequence + offset,
            "previous_receipt_digest": previous_digest,
            "issued_at": f"2026-09-10T00:00:{offset:02d}Z",
        }
        receipt["receipt_id"] = canonical_digest(
            {
                "authority_id": store.authority_id,
                "sequence": receipt["sequence"],
                "operation": receipt["operation"],
            }
        )
        receipt["receipt_digest"] = canonical_digest(receipt)
        store._connection.execute(
            "INSERT INTO receipts(authority_id, sequence, receipt_id, previous_receipt_digest, receipt_json, receipt_digest, recorded_at) VALUES(?,?,?,?,?,?,?)",
            (
                store.authority_id,
                receipt["sequence"],
                receipt["receipt_id"],
                previous_digest,
                json.dumps(receipt, sort_keys=True, separators=(",", ":")),
                receipt["receipt_digest"],
                receipt["issued_at"],
            ),
        )
        store._connection.execute(
            "UPDATE authority_meta SET last_sequence=?, last_receipt_digest=? WHERE authority_id=?",
            (receipt["sequence"], receipt["receipt_digest"], store.authority_id),
        )
        previous_digest = str(receipt["receipt_digest"])


def test_ac02_exact_one_tail_reconciles_but_two_tails_do_not(store: _AuthorityStore) -> None:
    store.observe_claim(valid_observe_request())
    _append_test_receipts_with_raw_sqlite(store, count=1)

    assert store.reconcile_crash_tail()["reconciled"] is True

    _append_test_receipts_with_raw_sqlite(store, count=2)
    with pytest.raises(AuthorityError, match="AUTHORITY_HIGHWATER_GAP"):
        store.reconcile_crash_tail()


def test_red_backup_manifest_binds_descriptor_digest(store: _AuthorityStore) -> None:
    activation = store.activate_shadow(valid_activation_request(store))

    pairs = [store.backup_now(timestamp=f"2026-09-{day:02d}T00:00:00Z") for day in range(20, 24)]
    manifests = sorted(store.test_paths.backups.glob("*.manifest.json"))
    databases = sorted(store.test_paths.backups.glob("*.sqlite3"))

    assert len(pairs) == 4
    assert len(manifests) == 3
    assert len(databases) == 3
    for manifest_path in manifests:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest["descriptor_digest"] == activation["descriptor_digest"]
        assert manifest["store_digest"].startswith("sha256:")


def test_green_lazy_backup_rotation_keeps_three_valid_days_and_preserves_invalid_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = {"value": datetime(2026, 9, 1, 12, 0, tzinfo=UTC)}
    monkeypatch.setattr(claims_authority, "_clock_now", lambda: current["value"])
    daily = _AuthorityStore.connect_for_test(
        tmp_path / "authority",
        authority_id=f"test:{uuid4()}",
    )
    for day in range(1, 5):
        current["value"] = datetime(2026, 9, day, 12, 0, tzinfo=UTC)
        daily.observe_claim(
            {
                **valid_observe_request(),
                "request_id": str(uuid4()),
                "run_id": f"run-{day}",
            }
        )
        if day == 3:
            daily.test_paths.backups.mkdir(mode=0o700, parents=True, exist_ok=True)
            invalid = daily.test_paths.backups / "backup-19990101T000000Z.manifest.json"
            invalid.write_text('{"invalid":true}', encoding="utf-8")
            invalid.chmod(0o600)

    valid_manifests = []
    for path in daily.test_paths.backups.glob("backup-*.manifest.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("digest") == canonical_digest(payload):
            valid_manifests.append(path)
    assert len(valid_manifests) == 3
    assert (daily.test_paths.backups / "backup-19990101T000000Z.manifest.json").exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("actor_id", "agent-b"),
        ("delivery_attempt_id", "attempt-b"),
        ("repository", "attacker/repository"),
        ("branch", "agent/other--branch"),
        ("frozen_base", "A" * 40),
        ("head_oid", "not-an-oid"),
    ],
)
def test_red_identity_tuple_mismatch_is_denied(
    store: _AuthorityStore,
    field: str,
    value: str,
) -> None:
    request = valid_observe_request()
    request[field] = value

    with pytest.raises(AuthorityError, match="IDENTITY_MISMATCH"):
        store.observe_claim(request)


@pytest.mark.parametrize(
    "field",
    ["clone_root_digest", "clone_identity_digest", "manifest_digest", "readiness_digest"],
)
def test_red_identity_receipt_digest_drift_is_denied(store: _AuthorityStore, field: str) -> None:
    request = valid_observe_request()
    request[field] = "sha256:INVALID"

    with pytest.raises(AuthorityError, match="IDENTITY_MISMATCH"):
        store.observe_claim(request)


def test_red_existing_claim_identity_tuple_cannot_drift_between_observations(
    store: _AuthorityStore,
) -> None:
    store.observe_claim(valid_observe_request())
    changed = {
        **valid_observe_request(),
        "request_id": str(uuid4()),
        "clone_identity_digest": _digest("9"),
    }

    with pytest.raises(AuthorityError, match="IDENTITY_MISMATCH"):
        store.observe_claim(changed)


def _production_observe_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[claims_authority.AuthorityPaths, dict[str, object], Path]:
    account_home = tmp_path / "account"
    clone_root = account_home / "agents/agent-a/attempts/attempt-a/ws"
    registry, seeded_run_path, seeded_lock_path = _seed_authority_lifecycle_run(clone_root, monkeypatch)
    del registry
    run_path = clone_root / ".omo/_delivery/agent-workflows/runs/run-authority.yaml"
    run_path.parent.mkdir(parents=True)
    run_path.write_bytes(seeded_run_path.read_bytes())
    integration_root = account_home / "Workspace"
    spec_path = integration_root / "docs/spec.md"
    spec_path.parent.mkdir(parents=True)
    spec_path.write_text("---\nstatus: accepted\n---\n", encoding="utf-8")
    spec_digest = f"sha256:{hashlib.sha256(spec_path.read_bytes()).hexdigest()}"
    affected_body = {
        "schema": "affected-graph-receipt/v1",
        "affected_projects": ["omo"],
        "changed_projects": ["omo"],
        "layer_contract_digest": "7" * 64,
    }
    affected_hash = hashlib.sha256(
        json.dumps(affected_body, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    affected_path = clone_root / ".omo/evidence/affected.json"
    affected_path.parent.mkdir(parents=True)
    affected_path.write_text(
        json.dumps({**affected_body, "receipt_hash": affected_hash}, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    run = yaml.safe_load(run_path.read_text(encoding="utf-8"))
    canonical_lock_path = clone_root / ".omo/_delivery/agent-workflows/locks/path_existing.py.lock.yaml"
    canonical_lock_path.parent.mkdir(parents=True)
    canonical_lock_path.write_bytes(seeded_lock_path.read_bytes())
    run["locks"] = [str(canonical_lock_path)]
    run["claims"][0]["locks"] = [str(canonical_lock_path)]
    run["spec_binding"] = {
        "spec_ref": "repo://docs/spec.md",
        "content_digest": spec_digest,
    }
    run["work_packet"] = {
        "packet_id": "WP-BET-Y1Q4-T10-145",
        "bet_id": "BET-Y1Q4-T10-145",
        "scope": {"write_surfaces": ["existing.py"]},
    }
    run["work_packet_hash"] = canonical_digest(run["work_packet"])
    run["claims"][0]["affected_graph"] = {
        **affected_body,
        "receipt_hash": affected_hash,
        "receipt_ref": ".omo/evidence/affected.json",
    }
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    identity = json.loads((clone_root / ".git/agent-clone-identity.json").read_text(encoding="utf-8"))
    provenance = json.loads((clone_root / ".git/agent-clone-provenance.json").read_text(encoding="utf-8"))
    readiness = json.loads((clone_root / ".git/agent-clone-readiness.json").read_text(encoding="utf-8"))
    claim = run["claims"][0]
    snapshot = lifecycle._authority_snapshot(
        {
            "runner": {
                "workspace_root": str(clone_root),
                "run_state_dir": ".omo/_delivery/agent-workflows/runs",
                "lock_state_dir": ".omo/_delivery/agent-workflows/locks",
            }
        },
        "run-authority",
    )
    request: dict[str, object] = {
        "schema": "claim-mutation-envelope/v2",
        "operation": "observe-claim",
        "request_id": str(uuid4()),
        "authority_id": "omo-claims-authority-r0",
        "actor_id": "agent-a",
        "delivery_attempt_id": "attempt-a",
        "repository": "starlink-awaken/omostation",
        "clone_root_digest": canonical_digest({"clone_root": str(clone_root.resolve())}),
        "branch": provenance["working_branch"],
        "clone_identity_digest": canonical_digest(identity),
        "manifest_digest": canonical_digest(identity["transport"]),
        "readiness_digest": f"sha256:{readiness['receipt_digest']}",
        "frozen_base": identity["frozen_root_sha"],
        "head_oid": "a" * 40,
        "bet_id": run["bet_id"],
        "work_packet_id": run["work_packet"]["packet_id"],
        "work_packet_digest": run["work_packet_hash"],
        "spec_ref": run["spec_binding"]["spec_ref"],
        "spec_digest": run["spec_binding"]["content_digest"],
        "affected_graph_digest": f"sha256:{affected_hash}",
        "requested_paths_digest": canonical_digest({"paths": claim["paths"], "surfaces": claim["surfaces"]}),
        "expected_claim_version": 0,
        "run_id": run["run_id"],
        "claim_ordinal": 0,
        "v1_claim_digest": canonical_digest(claim),
        "v1_run_digest": snapshot["run_digest"],
        "v1_lock_set_digest": snapshot["lock_set_digest"],
        "v1_decision": {"decision": "deny", "code": "claims_authority_mismatch"},
        "authority_mode": "shadow",
        "clone_identity_schema": identity["schema"],
    }
    monkeypatch.setattr(claims_authority.pwd, "getpwuid", lambda _uid: SimpleNamespace(pw_dir=str(account_home)))
    return resolve_authority_paths(), request, spec_path


def test_red_production_broker_independently_rereads_identity_run_packet_spec_and_graph(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, request, spec_path = _production_observe_fixture(tmp_path, monkeypatch)

    verified = claims_authority._verify_production_observe_request(request, paths)
    assert verified["run_id"] == "run-authority"
    assert verified["claim_ordinal"] == 0

    spec_path.write_text("tampered\n", encoding="utf-8")
    with pytest.raises(AuthorityError, match="WORK_PACKET_UNBOUND"):
        claims_authority._verify_production_observe_request(request, paths)


def test_red_production_mutation_broker_recomputes_run_and_lock_snapshots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, observe_request, _spec_path = _production_observe_fixture(tmp_path, monkeypatch)
    clone_root = paths.account_home / "agents/agent-a/attempts/attempt-a/ws"
    registry = {
        "runner": {
            "workspace_root": str(clone_root),
            "run_state_dir": ".omo/_delivery/agent-workflows/runs",
            "lock_state_dir": ".omo/_delivery/agent-workflows/locks",
        }
    }
    snapshot = lifecycle._authority_snapshot(registry, "run-authority")
    del observe_request
    envelope_identity = lifecycle._authority_envelope_identity(registry, snapshot)
    identity_fields = {key: envelope_identity[key] for key in claims_authority._ENVELOPE_IDENTITY_FIELDS}
    request = {
        "schema": "claim-mutation-envelope/v2",
        **identity_fields,
        "request_id": str(uuid4()),
        "authority_id": "omo-claims-authority-r0",
        "operation": "heartbeat",
        "run_digest": snapshot["run_digest"],
        "lock_set_digest": snapshot["lock_set_digest"],
        "members": [],
    }

    verified = claims_authority._verify_production_mutation_request(request, paths, phase="before")
    assert verified["run_digest"] == snapshot["run_digest"]
    lock_path = clone_root / ".omo/_delivery/agent-workflows/locks/path_existing.py.lock.yaml"
    lock_path.write_text(lock_path.read_text(encoding="utf-8") + "tampered: true\n", encoding="utf-8")

    with pytest.raises(AuthorityError, match="AFFECTED_GRAPH_MISMATCH"):
        claims_authority._verify_production_mutation_request(request, paths, phase="before")


def _broker992_rebind_observe(
    paths: claims_authority.AuthorityPaths,
    request: dict[str, object],
    *,
    claimed_path: str,
    write_surfaces: object,
    surfaces: list[str] | None = None,
) -> Path:
    clone = paths.account_home / "agents/agent-a/attempts/attempt-a/ws"
    run_path = clone / ".omo/_delivery/agent-workflows/runs/run-authority.yaml"
    run = yaml.safe_load(run_path.read_text(encoding="utf-8"))
    claim = run["claims"][0]
    claim["paths"] = [claimed_path]
    claim["surfaces"] = surfaces or []
    run["work_packet"]["scope"]["write_surfaces"] = write_surfaces
    run["work_packet_hash"] = canonical_digest(run["work_packet"])
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    request.update(
        work_packet_digest=run["work_packet_hash"],
        v1_claim_digest=canonical_digest(claim),
        v1_run_digest=f"sha256:{hashlib.sha256(run_path.read_bytes()).hexdigest()}",
        requested_paths_digest=canonical_digest({"paths": claim["paths"], "surfaces": claim["surfaces"]}),
    )
    return clone


@pytest.mark.parametrize(
    "surface",
    [
        "bin/panorama/assets/host",
        "bin/panorama/assets/host/",
        "bin/panorama/assets/host/live_server.py.asset",
        "bin/panorama/assets/host/*.asset",
    ],
)
def test_broker992_observe_accepts_native_exact_directory_and_glob_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
) -> None:
    paths, request, _ = _production_observe_fixture(tmp_path, monkeypatch)
    clone = _broker992_rebind_observe(
        paths,
        request,
        claimed_path="bin/panorama/assets/host/live_server.py.asset",
        write_surfaces=[surface],
    )
    run_path = clone / ".omo/_delivery/agent-workflows/runs/run-authority.yaml"
    before = run_path.read_bytes()
    assert claims_authority._verify_production_observe_request(request, paths)["run_id"] == "run-authority"
    assert run_path.read_bytes() == before


@pytest.mark.parametrize(
    ("claimed_path", "write_surfaces", "surfaces"),
    [
        ("bin/panorama/assets/host-other/file.asset", ["bin/panorama/assets/host"], []),
        ("dir/file.py/child", ["dir/file.py"], []),
        ("existing.py", ["other.py"], []),
        ("existing.py", [""], []),
        ("existing.py", [7], []),
        ("existing.py", None, []),
        ("../outside.py", ["../outside.py"], []),
        ("/tmp/outside.py", ["/tmp/outside.py"], []),
        ("existing.py", ["existing.py"], ["governance_state"]),
    ],
)
def test_broker992_observe_rejects_prefix_escape_invalid_scope_and_surface_labels(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    claimed_path: str,
    write_surfaces: object,
    surfaces: list[str],
) -> None:
    paths, request, _ = _production_observe_fixture(tmp_path, monkeypatch)
    _broker992_rebind_observe(
        paths,
        request,
        claimed_path=claimed_path,
        write_surfaces=write_surfaces,
        surfaces=surfaces,
    )
    with pytest.raises(AuthorityError, match="CLAIM_SCOPE_VIOLATION"):
        claims_authority._verify_production_observe_request(request, paths)


@pytest.mark.parametrize("outside", [False, True])
def test_broker992_observe_rejects_symlink_alias_and_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outside: bool,
) -> None:
    paths, request, _ = _production_observe_fixture(tmp_path, monkeypatch)
    clone = _broker992_rebind_observe(
        paths,
        request,
        claimed_path="dir/link/file.py",
        write_surfaces=["dir" + "/link"],
    )
    target = tmp_path / "outside" if outside else clone / "dir/real"
    target.mkdir(parents=True)
    link = clone / "dir/link"
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(AuthorityError, match="CLAIM_SCOPE_VIOLATION"):
        claims_authority._verify_production_observe_request(request, paths)


def _broker992_mutation_request(
    paths: claims_authority.AuthorityPaths,
    *,
    operation: str = "heartbeat",
) -> dict[str, object]:
    clone = paths.account_home / "agents/agent-a/attempts/attempt-a/ws"
    registry = {
        "runner": {
            "workspace_root": str(clone),
            "run_state_dir": ".omo/_delivery/agent-workflows/runs",
            "lock_state_dir": ".omo/_delivery/agent-workflows/locks",
        }
    }
    snapshot = lifecycle._authority_snapshot(registry, "run-authority")
    identity = lifecycle._authority_envelope_identity(registry, snapshot)
    return {
        "schema": "claim-mutation-envelope/v2",
        **{key: identity[key] for key in claims_authority._ENVELOPE_IDENTITY_FIELDS},
        "request_id": str(uuid4()),
        "authority_id": "omo-claims-authority-r0",
        "operation": operation,
        "run_digest": snapshot["run_digest"],
        "lock_set_digest": snapshot["lock_set_digest"],
        "resulting_run_digest": snapshot["run_digest"],
        "resulting_lock_set_digest": snapshot["lock_set_digest"],
        "members": [],
        "mutation_process_identity_digest": _digest("9"),
    }


@pytest.mark.parametrize("phase", ["before", "after"])
def test_broker992_mutation_deduplicates_relative_and_absolute_lock_aliases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    paths, _, _ = _production_observe_fixture(tmp_path, monkeypatch)
    clone = paths.account_home / "agents/agent-a/attempts/attempt-a/ws"
    run_path = clone / ".omo/_delivery/agent-workflows/runs/run-authority.yaml"
    run = yaml.safe_load(run_path.read_text(encoding="utf-8"))
    run["locks"].append(".omo/_delivery/agent-workflows/locks/path_existing.py.lock.yaml")
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    request = _broker992_mutation_request(paths)
    verified = claims_authority._verify_production_mutation_request(request, paths, phase=phase)
    assert verified["lock_set_digest"] == request["lock_set_digest"]
    assert verified["claims_empty"] is False
    lock_path = clone / run["locks"][-1]
    lock_path.write_text(lock_path.read_text(encoding="utf-8") + "tampered: true\n", encoding="utf-8")
    with pytest.raises(AuthorityError, match="AFFECTED_GRAPH_MISMATCH"):
        claims_authority._verify_production_mutation_request(request, paths, phase=phase)


def _broker992_empty_run(paths: claims_authority.AuthorityPaths) -> None:
    clone = paths.account_home / "agents/agent-a/attempts/attempt-a/ws"
    run_path = clone / ".omo/_delivery/agent-workflows/runs/run-authority.yaml"
    run = yaml.safe_load(run_path.read_text(encoding="utf-8"))
    for raw in run["locks"]:
        Path(raw).unlink()
    run["claims"] = []
    run["locks"] = []
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")


@pytest.mark.parametrize("operation", ["heartbeat", "close"])
def test_broker992_real_empty_native_run_can_reserve_idempotently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    paths, _, _ = _production_observe_fixture(tmp_path, monkeypatch)
    _broker992_empty_run(paths)
    local_store = _AuthorityStore.connect_for_test(paths.account_home / "test-authority", f"test:{uuid4()}")
    request = _broker992_mutation_request(paths, operation=operation)
    request.pop("resulting_run_digest")
    request.pop("resulting_lock_set_digest")
    request["authority_id"] = local_store.authority_id
    receipt = local_store.begin_claim_mutation(request)
    assert receipt["state"] == "reserved"
    assert receipt["members"] == []
    assert local_store.begin_claim_mutation(request) == receipt
    changed = {**request, "run_digest": _digest("f")}
    with pytest.raises(AuthorityError, match="REQUEST_ID_REUSE_MISMATCH"):
        local_store.begin_claim_mutation(changed)


def test_broker992_empty_members_do_not_hide_real_native_claims(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _, _ = _production_observe_fixture(tmp_path, monkeypatch)
    local_store = _AuthorityStore.connect_for_test(paths.account_home / "test-authority", f"test:{uuid4()}")
    request = _broker992_mutation_request(paths)
    request.pop("resulting_run_digest")
    request.pop("resulting_lock_set_digest")
    request["authority_id"] = local_store.authority_id
    with pytest.raises(AuthorityError, match="CLAIM_SCOPE_VIOLATION"):
        local_store.begin_claim_mutation(request)
    assert local_store.scalar("SELECT COUNT(*) FROM claim_mutation_batches") == 0


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("head_oid", "b" * 40, "IDENTITY_MISMATCH"),
        ("work_packet_digest", _digest("f"), "WORK_PACKET_UNBOUND"),
        ("run_digest", _digest("f"), "AFFECTED_GRAPH_MISMATCH"),
    ],
)
def test_broker992_empty_run_still_rechecks_identity_binding_and_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: str,
    error: str,
) -> None:
    paths, _, _ = _production_observe_fixture(tmp_path, monkeypatch)
    _broker992_empty_run(paths)
    local_store = _AuthorityStore.connect_for_test(paths.account_home / "test-authority", f"test:{uuid4()}")
    request = _broker992_mutation_request(paths)
    request.pop("resulting_run_digest")
    request.pop("resulting_lock_set_digest")
    request.update(authority_id=local_store.authority_id)
    request[field] = value
    with pytest.raises(AuthorityError, match=error):
        local_store.begin_claim_mutation(request)
    assert local_store.scalar("SELECT COUNT(*) FROM claim_mutation_batches") == 0


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("bet_id", "", "WORK_PACKET_UNBOUND"),
        ("work_packet_id", "WP-OTHER", "WORK_PACKET_UNBOUND"),
        ("work_packet_digest", "sha256:INVALID", "WORK_PACKET_UNBOUND"),
        ("spec_ref", "https://attacker/spec", "WORK_PACKET_UNBOUND"),
        ("spec_digest", "sha256:INVALID", "WORK_PACKET_UNBOUND"),
        ("requested_paths_digest", "sha256:INVALID", "CLAIM_SCOPE_VIOLATION"),
    ],
)
def test_red_work_packet_binding_and_scope_are_exact(
    store: _AuthorityStore,
    field: str,
    value: str,
    code: str,
) -> None:
    request = valid_observe_request()
    request[field] = value

    with pytest.raises(AuthorityError, match=code):
        store.observe_claim(request)


def test_red_affected_graph_path_mismatch_is_denied(store: _AuthorityStore) -> None:
    request = valid_observe_request()
    request["affected_graph_digest"] = "sha256:INVALID"

    with pytest.raises(AuthorityError, match="AFFECTED_GRAPH_MISMATCH"):
        store.observe_claim(request)


@pytest.mark.parametrize("field", ["run_id"])
def test_red_unverifiable_run_or_receipt_is_rejected(store: _AuthorityStore, field: str) -> None:
    request = valid_observe_request()
    request[field] = ""

    with pytest.raises(AuthorityError, match="IDENTITY_MISMATCH"):
        store.observe_claim(request)


def test_red_test_authority_receipt_is_never_publishable(store: _AuthorityStore) -> None:
    receipt = store.observe_claim(valid_observe_request())

    assert receipt["authority_id"].startswith("test:")
    assert receipt["publishable"] is False
    assert receipt["security_level"] == "R0_COOPERATIVE"


def test_red_r0_receipt_cannot_claim_adversarial_security(store: _AuthorityStore) -> None:
    request = valid_activation_request(store)
    descriptor = dict(request["descriptor"])
    descriptor["security_level"] = "adversarial-r1"
    descriptor["digest"] = canonical_digest(descriptor)
    request["descriptor"] = descriptor

    with pytest.raises(AuthorityError, match="AUTHORITY_DESCRIPTOR_MISMATCH"):
        store.activate_shadow(request)


def test_red_v1_managed_clone_allow_is_forbidden(store: _AuthorityStore) -> None:
    request = valid_observe_request()
    request["clone_identity_schema"] = "agent-clone-identity/v2"
    request["v1_decision"] = {"decision": "allow", "code": "legacy_allow"}

    with pytest.raises(AuthorityError, match="V1_AUTHORITY_FORBIDDEN"):
        store.observe_claim(request)


def _publication_scope(paths: list[str]) -> dict[str, object]:
    return {
        "schema": "claims-publication-scope/v1",
        "kind": "legacy-publication",
        "effect_ceiling": "one-legacy-fence",
        "changed_paths": paths,
        "paths_digest": canonical_digest(paths),
    }


def test_red_v2_general_allow_still_forbidden_without_scope(store: _AuthorityStore) -> None:
    request = valid_observe_request()
    request["clone_identity_schema"] = "agent-clone-identity/v2"
    request["v1_decision"] = {"decision": "allow", "code": "legacy_allow"}

    with pytest.raises(AuthorityError, match="V1_AUTHORITY_FORBIDDEN"):
        store.observe_claim(request)


def test_red_v2_publication_scope_wrong_ceiling_rejected(store: _AuthorityStore) -> None:
    request = valid_observe_request()
    request["clone_identity_schema"] = "agent-clone-identity/v2"
    request["v1_decision"] = {"decision": "allow", "code": "legacy_allow"}
    scope = _publication_scope(["docs/reports/example.md"])
    scope["effect_ceiling"] = "unlimited"
    request["publication_scope"] = scope
    request["requested_paths_digest"] = scope["paths_digest"]

    with pytest.raises(AuthorityError, match="V1_AUTHORITY_FORBIDDEN"):
        store.observe_claim(request)


def test_red_v2_publication_scope_unbound_paths_rejected(store: _AuthorityStore) -> None:
    request = valid_observe_request()
    request["clone_identity_schema"] = "agent-clone-identity/v2"
    request["v1_decision"] = {"decision": "allow", "code": "legacy_allow"}
    scope = _publication_scope(["docs/reports/example.md"])
    request["publication_scope"] = scope
    request["requested_paths_digest"] = _digest("other")

    with pytest.raises(AuthorityError, match="CLAIM_SCOPE_VIOLATION"):
        store.observe_claim(request)


def test_green_v2_publication_scoped_allow_observe_and_fence(store: _AuthorityStore) -> None:
    activation = store.activate_shadow(valid_activation_request(store))
    descriptor_digest = str(activation["descriptor_digest"])

    paths = ["docs/reports/2026-09-20-claims-authority-lifecycle-regression-runbook.md"]
    scope = _publication_scope(paths)
    request = valid_observe_request()
    request.update(
        {
            "request_id": str(uuid4()),
            "run_id": "run-pub-scope",
            "claim_ordinal": 0,
            "clone_identity_schema": "agent-clone-identity/v2",
            "v1_claim_digest": canonical_digest({"run_id": "run-pub-scope", "ordinal": 0}),
            "v1_decision": {"decision": "allow", "code": "legacy_allow"},
            "publication_scope": scope,
            "requested_paths_digest": scope["paths_digest"],
        }
    )
    receipt = store.observe_claim(request)
    assert receipt["comparison"]["effective_v1"]["decision"] == "allow"
    assert receipt["publication_scope"]["effect_ceiling"] == "one-legacy-fence"

    fence_req = _issue_fence_request(store, receipt, descriptor_digest)
    fence_req["path_digest"] = scope["paths_digest"]
    fence = store.issue_legacy_fence(fence_req)
    assert fence["operation"] == "issue-legacy-fence"


def test_red_future_cutover_rejects_v1_publication_fixture(store: _AuthorityStore) -> None:
    request = valid_observe_request()
    request["authority_mode"] = "cutover"
    request["v1_decision"] = {"decision": "allow", "code": "legacy_allow"}

    with pytest.raises(AuthorityError, match="V1_AUTHORITY_FORBIDDEN"):
        store.observe_claim(request)


def test_red_v1_record_cannot_be_promoted_to_v2(store: _AuthorityStore) -> None:
    request = valid_observe_request()
    request["promoted_v1_receipt"] = {"source": "copied"}

    with pytest.raises(AuthorityError, match="REQUEST_SCHEMA_INVALID"):
        store.observe_claim(request)


def test_red_shadow_result_never_changes_effective_v1() -> None:
    result = compare_decisions(
        V1Decision("allow", "legacy_allow"),
        ShadowDecision("would_deny", "scope_mismatch"),
    )

    assert result["classification"] == "unexplained"
    assert result["effective_v1"] == {"decision": "allow", "code": "legacy_allow"}
    assert result["effective_claim_authority"] == "v1"
    assert result["instruction_capable"] is False


def test_green_managed_clone_is_expected_difference_without_publication(store: _AuthorityStore) -> None:
    result = compare_decisions(
        V1Decision("deny", "claims_authority_mismatch"),
        ShadowDecision("would_allow", "valid_managed_clone"),
    )

    assert result["classification"] == "expected_managed_clone_difference"
    assert result["effective_claim_authority"] == "v1"
    assert result["instruction_capable"] is False
    assert result["publication_effect_fence"] == "legacy-v2-required-after-v1-allow"

    receipt = store.observe_claim(valid_observe_request())
    assert receipt["comparison"] == result
    assert receipt["publishable"] is False


def test_ac07_status_is_redacted_and_stale_after_120_seconds(store: _AuthorityStore) -> None:
    store.observe_claim(valid_observe_request())
    now = datetime.now(UTC)

    current = store.authority_status(now=now)
    stale = store.authority_status(now=now + timedelta(seconds=121))

    assert current["fresh"] is True
    assert stale["fresh"] is False
    assert current["instruction_capable"] is False
    serialized = json.dumps(current)
    assert str(Path.home()) not in serialized
    assert "github.com" not in serialized
    assert claims_authority.pwd.getpwuid(os.getuid()).pw_name not in serialized


def test_red_shadow_active_is_not_an_operating_mode(store: _AuthorityStore) -> None:
    request = valid_activation_request(store)
    descriptor = dict(request["descriptor"])
    descriptor["operating_mode"] = "shadow-active"
    descriptor["digest"] = canonical_digest(descriptor)
    request["descriptor"] = descriptor

    with pytest.raises(AuthorityError, match="AUTHORITY_DESCRIPTOR_MISMATCH"):
        store.activate_shadow(request)


@pytest.mark.parametrize("case", ["missing_closure", "wrong_repository", "unknown_field"])
def test_red_activation_descriptor_requires_exact_dependency_closure(
    store: _AuthorityStore,
    case: str,
) -> None:
    request = valid_activation_request(store)
    descriptor = dict(request["descriptor"])
    if case == "missing_closure":
        descriptor.pop("critical_dependency_closure_digest")
    elif case == "wrong_repository":
        descriptor["repository"] = "attacker/repository"
    else:
        descriptor["caller_store_path"] = "/tmp/attacker"
    descriptor["digest"] = canonical_digest(descriptor)
    request["descriptor"] = descriptor

    with pytest.raises(AuthorityError, match="AUTHORITY_DESCRIPTOR_MISMATCH"):
        store.activate_shadow(request)


def _observe_member(
    store: _AuthorityStore,
    *,
    run_id: str,
    ordinal: int,
    v1_decision: str = "deny",
) -> dict[str, object]:
    request = valid_observe_request()
    request.update(
        {
            "request_id": str(uuid4()),
            "run_id": run_id,
            "claim_ordinal": ordinal,
            "v1_claim_digest": canonical_digest({"run_id": run_id, "ordinal": ordinal}),
            "v1_decision": {
                "decision": v1_decision,
                "code": "legacy_allow" if v1_decision == "allow" else "claims_authority_mismatch",
            },
        }
    )
    return store.observe_claim(request)


def _member(receipt: dict[str, object]) -> dict[str, object]:
    return {
        "claim_id": receipt["claim_id"],
        "claim_version": receipt["claim_version"],
        "lease_epoch": receipt["lease_epoch"],
    }


def _begin_mutation_request(
    store: _AuthorityStore,
    receipts: list[dict[str, object]],
    *,
    operation: str = "heartbeat",
    run_id: str = "run-batch",
) -> dict[str, object]:
    return {
        "schema": "claim-mutation-envelope/v2",
        "request_id": str(uuid4()),
        "authority_id": store.authority_id,
        "operation": operation,
        "run_id": run_id,
        "run_digest": _digest("d"),
        "lock_set_digest": _digest("e"),
        "members": [_member(receipt) for receipt in receipts],
        "mutation_process_identity_digest": _digest("9"),
    }


def _active_claim_and_descriptor(store: _AuthorityStore, *, run_id: str = "run-fence") -> tuple[dict[str, object], str]:
    activation = store.activate_shadow(valid_activation_request(store))
    claim = _observe_member(store, run_id=run_id, ordinal=0, v1_decision="allow")
    return claim, str(activation["descriptor_digest"])


def _remote_observation_pair(
    *,
    remote_ref: str,
    observed_remote_oid: str,
    descriptor_digest: str,
    effect_process_identity_digest: str = _digest("9"),
) -> tuple[dict[str, object], dict[str, object]]:
    observed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    base = {
        "schema": "claims-remote-observation/v2",
        "repository_identity_digest": _digest("a"),
        "remote_ref_digest": canonical_digest({"remote_ref": remote_ref}),
        "observed_remote_oid": observed_remote_oid,
        "broker_observed_at": observed_at,
        "git_executable_digest": _digest("b"),
        "effect_process_identity_digest": effect_process_identity_digest,
        "command_digest": _digest("c"),
        "descriptor_digest": descriptor_digest,
    }
    return ({**base, "monotonic_ns": 1}, {**base, "monotonic_ns": 2})


def _issue_fence_request(
    store: _AuthorityStore,
    claim: dict[str, object],
    descriptor_digest: str,
) -> dict[str, object]:
    remote_ref = "refs/heads/agent/agent-a--attempt-a"
    first, second = _remote_observation_pair(
        remote_ref=remote_ref,
        observed_remote_oid="a" * 40,
        descriptor_digest=descriptor_digest,
    )
    return {
        "schema": "claim-mutation-envelope/v2",
        "request_id": str(uuid4()),
        "authority_id": store.authority_id,
        "operation": "issue-legacy-fence",
        "claim_id": claim["claim_id"],
        "claim_version": claim["claim_version"],
        "lease_epoch": claim["lease_epoch"],
        "v1_allow_receipt_digest": claim["receipt_digest"],
        "v1_snapshot_digest": claim["v1_run_digest"],
        "changeset_digest": _digest("3"),
        "path_digest": _digest("4"),
        "head_oid": "b" * 40,
        "descriptor_digest": descriptor_digest,
        "remote_ref": remote_ref,
        "expected_remote_oid": "a" * 40,
        "first_remote_observation": first,
        "second_remote_observation": second,
    }


def _enter_fence_request(store: _AuthorityStore, fence: dict[str, object]) -> dict[str, object]:
    first, second = _remote_observation_pair(
        remote_ref=str(fence["remote_ref"]),
        observed_remote_oid=str(fence["expected_remote_oid"]),
        descriptor_digest=str(fence["descriptor_digest"]),
    )
    return {
        "schema": "claim-mutation-envelope/v2",
        "request_id": str(uuid4()),
        "authority_id": store.authority_id,
        "operation": "enter-legacy-publishing",
        "fence_id": fence["fence_id"],
        "v1_snapshot_digest": fence["v1_snapshot_digest"],
        "claim_version": fence["claim_version"],
        "lease_epoch": fence["lease_epoch"],
        "remote_ref": fence["remote_ref"],
        "expected_remote_oid": fence["expected_remote_oid"],
        "first_remote_observation": first,
        "second_remote_observation": second,
    }


def _settle_fence_request(
    store: _AuthorityStore,
    fence: dict[str, object],
    *,
    outcome: str = "success",
    request_id: str | None = None,
) -> dict[str, object]:
    first, second = _remote_observation_pair(
        remote_ref=str(fence["remote_ref"]),
        observed_remote_oid="b" * 40,
        descriptor_digest=str(fence["descriptor_digest"]),
    )
    if request_id is None:
        row = store._connection.execute(
            "SELECT settlement_request_id FROM legacy_fences WHERE fence_id=?",
            (fence["fence_id"],),
        ).fetchone()
        assert row is not None and row["settlement_request_id"]
        request_id = str(row["settlement_request_id"])
    return {
        "schema": "claim-mutation-envelope/v2",
        "request_id": request_id,
        "authority_id": store.authority_id,
        "operation": "settle-legacy-publication",
        "fence_id": fence["fence_id"],
        "outcome": outcome,
        "remote_ref": fence["remote_ref"],
        "observed_remote_oid": "b" * 40,
        "effect_process_identity_digest": _digest("9"),
        "first_remote_observation": first,
        "second_remote_observation": second,
    }


def _settle_mutation_request(
    store: _AuthorityStore,
    begin: dict[str, object],
    *,
    outcome: str = "applied",
    resulting_run_digest: str = _digest("d"),
    resulting_lock_set_digest: str = _digest("f"),
    request_id: str | None = None,
) -> dict[str, object]:
    return {
        "schema": "claim-mutation-envelope/v2",
        "request_id": request_id or str(begin["settlement_request_id"]),
        "authority_id": store.authority_id,
        "operation": "settle-claim-mutation",
        "mutation_batch_id": begin["mutation_batch_id"],
        "outcome": outcome,
        "resulting_run_digest": resulting_run_digest,
        "resulting_lock_set_digest": resulting_lock_set_digest,
        "members": begin["members"],
        "mutation_process_identity_digest": _digest("9"),
    }


def test_red_run_mutation_batch_requires_every_claim_member(store: _AuthorityStore) -> None:
    first = _observe_member(store, run_id="run-batch", ordinal=0)
    _observe_member(store, run_id="run-batch", ordinal=1)
    request = _begin_mutation_request(store, [first])

    with pytest.raises(AuthorityError, match="CLAIM_SCOPE_VIOLATION"):
        store.begin_claim_mutation(request)


def test_red_claim_cas_lease_and_takeover_races_are_denied(store: _AuthorityStore) -> None:
    claim = _observe_member(store, run_id="run-batch", ordinal=0)
    stale = _begin_mutation_request(store, [claim])
    stale["members"] = [{**_member(claim), "claim_version": int(claim["claim_version"]) + 1}]

    with pytest.raises(AuthorityError, match="CLAIM_VERSION_STALE"):
        store.begin_claim_mutation(stale)

    store._connection.execute(
        "UPDATE claims SET expires_at='2000-01-01T00:00:00Z' WHERE claim_id=?",
        (claim["claim_id"],),
    )
    expired = _begin_mutation_request(store, [claim])
    with pytest.raises(AuthorityError, match="CLAIM_LEASE_EXPIRED"):
        store.begin_claim_mutation(expired)


def test_green_expire_batch_accepts_the_complete_expired_member_set(store: _AuthorityStore) -> None:
    claim = _observe_member(store, run_id="run-expire", ordinal=0)
    store._connection.execute(
        "UPDATE claims SET expires_at='2000-01-01T00:00:00Z' WHERE claim_id=?",
        (claim["claim_id"],),
    )

    receipt = store.begin_claim_mutation(
        _begin_mutation_request(
            store,
            [claim],
            operation="expire",
            run_id="run-expire",
        )
    )

    assert receipt["state"] == "reserved"
    assert receipt["mutation_operation"] == "expire"


def test_red_heartbeat_settlement_binds_complete_lockset_digest(store: _AuthorityStore) -> None:
    claim = _observe_member(store, run_id="run-batch", ordinal=0)
    begin = store.begin_claim_mutation(_begin_mutation_request(store, [claim]))
    unchanged = _settle_mutation_request(
        store,
        begin,
        outcome="applied",
        resulting_run_digest=_digest("d"),
        resulting_lock_set_digest=_digest("e"),
    )

    with pytest.raises(AuthorityError, match="AFFECTED_GRAPH_MISMATCH"):
        store.settle_claim_mutation(unchanged)

    changed = {
        **unchanged,
        "resulting_lock_set_digest": _digest("f"),
    }
    settled = store.settle_claim_mutation(changed)
    assert settled["outcome"] == "applied"
    assert settled["members"][0]["lease_epoch"] == int(claim["lease_epoch"]) + 1


@pytest.mark.parametrize("outcome", ["applied", "rejected"])
def test_red_mutation_settlement_cannot_misclassify_unchanged_or_changed_v1_state(
    store: _AuthorityStore,
    outcome: str,
) -> None:
    claim = _observe_member(store, run_id="run-close", ordinal=0)
    begin = store.begin_claim_mutation(_begin_mutation_request(store, [claim], operation="close", run_id="run-close"))
    if outcome == "applied":
        resulting_run_digest = _digest("d")
        resulting_lock_digest = _digest("e")
    else:
        resulting_run_digest = _digest("f")
        resulting_lock_digest = _digest("e")
    request = _settle_mutation_request(
        store,
        begin,
        outcome=outcome,
        resulting_run_digest=resulting_run_digest,
        resulting_lock_set_digest=resulting_lock_digest,
    )

    with pytest.raises(AuthorityError, match="AFFECTED_GRAPH_MISMATCH"):
        store.settle_claim_mutation(request)


def test_red_claim_mutation_and_fence_entry_are_mutually_exclusive(store: _AuthorityStore) -> None:
    claim, descriptor_digest = _active_claim_and_descriptor(store, run_id="run-batch")
    request = _begin_mutation_request(store, [claim])
    store.begin_claim_mutation(request)

    second = {**request, "request_id": str(uuid4())}
    with pytest.raises(AuthorityError, match="CLAIM_VERSION_STALE"):
        store.begin_claim_mutation(second)
    with pytest.raises(AuthorityError, match="LEGACY_DRAIN_INCOMPLETE"):
        store.issue_legacy_fence(_issue_fence_request(store, claim, descriptor_digest))


def test_red_fence_cannot_be_issued_from_deny_or_unbound_v1_receipt(store: _AuthorityStore) -> None:
    activation = store.activate_shadow(valid_activation_request(store))
    denied = _observe_member(store, run_id="run-denied", ordinal=0)
    request = _issue_fence_request(store, denied, str(activation["descriptor_digest"]))
    request["v1_allow_receipt_digest"] = denied["receipt_digest"]
    request["v1_snapshot_digest"] = denied["v1_snapshot_digest"]

    with pytest.raises(AuthorityError, match="V1_AUTHORITY_FORBIDDEN"):
        store.issue_legacy_fence(request)

    request["request_id"] = str(uuid4())
    request["v1_allow_receipt_digest"] = _digest("0")
    with pytest.raises(AuthorityError, match="V1_AUTHORITY_FORBIDDEN"):
        store.issue_legacy_fence(request)


def test_red_unknown_mutation_resolution_requires_all_operator_proofs(store: _AuthorityStore) -> None:
    claim = _observe_member(store, run_id="run-batch", ordinal=0)
    begin = store.begin_claim_mutation(_begin_mutation_request(store, [claim]))
    unknown = store.settle_claim_mutation(
        _settle_mutation_request(
            store,
            begin,
            outcome="unknown",
            resulting_run_digest=_digest("d"),
            resulting_lock_set_digest=_digest("e"),
        )
    )

    with pytest.raises(AuthorityError, match="REQUEST_SCHEMA_INVALID"):
        store.resolve_claim_mutation_unknown(
            {
                "schema": "claim-mutation-envelope/v2",
                "request_id": str(uuid4()),
                "authority_id": store.authority_id,
                "operation": "resolve-claim-mutation-unknown",
                "mutation_batch_id": unknown["mutation_batch_id"],
            }
        )

    with pytest.raises(AuthorityError, match="OPERATOR_AUTHORIZATION_REQUIRED"):
        store.resolve_claim_mutation_unknown(_resolve_mutation_request(store, unknown, begin["members"]))


def _mark_mutation_request(
    store: _AuthorityStore,
    mutation: dict[str, object],
    *,
    request_id: str | None = None,
) -> dict[str, object]:
    return {
        "schema": "claim-mutation-envelope/v2",
        "request_id": request_id or str(uuid4()),
        "authority_id": store.authority_id,
        "operation": "mark-claim-mutation-operator-required",
        "mutation_batch_id": mutation["mutation_batch_id"],
        "reason_code": "MUTATION_PROCESS_RESULT_UNKNOWN",
        "mutation_process_identity_digest": _digest("9"),
    }


def _resolve_mutation_request(
    store: _AuthorityStore,
    mutation: dict[str, object],
    members: object,
    *,
    outcome: str = "applied",
    run_digest: str = _digest("d"),
    lock_set_digest: str = _digest("f"),
    request_id: str | None = None,
) -> dict[str, object]:
    complete_read = {
        "run_digest": run_digest,
        "lock_set_digest": lock_set_digest,
        "members": members,
    }
    return {
        "schema": "claim-mutation-envelope/v2",
        "request_id": request_id or str(uuid4()),
        "authority_id": store.authority_id,
        "operation": "resolve-claim-mutation-unknown",
        "mutation_batch_id": mutation["mutation_batch_id"],
        "authorization_digest": _digest("7"),
        "stopped_process_digest": _digest("8"),
        "first_complete_read": complete_read,
        "second_complete_read": dict(complete_read),
        "outcome": outcome,
    }


def _write_operator_resolution_evidence(
    store: _AuthorityStore,
    *,
    target_kind: str,
    target_field: str,
    target_id: str,
    unknown_operation: str,
    resolver_operation: str,
    outcome: str,
    process_identity_digest: str,
) -> tuple[str, str]:
    unknown_receipt_digest = claims_authority._unknown_receipt_digest(
        store,
        operation=unknown_operation,
        target_field=target_field,
        target_id=target_id,
    )
    now = datetime.now(UTC)
    authorization: dict[str, object] = {
        "schema": "claims-operator-authorization/v1",
        "authority_id": store.authority_id,
        "security_level": "R0_COOPERATIVE",
        "principal_id": "principal-test",
        "principal_authority_ref": "decision://accepted/test-principal",
        "principal_receipt_digest": _digest("a"),
        "decision_ref": "decision://accepted/test-operator",
        "target_kind": target_kind,
        "target_id": target_id,
        "unknown_receipt_digest": unknown_receipt_digest,
        "resolver_operation": resolver_operation,
        "authorized_outcome": outcome,
        "process_identity_digest": process_identity_digest,
        "issued_at": now.isoformat().replace("+00:00", "Z"),
        "expires_at": (now + timedelta(seconds=300)).isoformat().replace("+00:00", "Z"),
    }
    authorization["digest"] = canonical_digest(authorization)
    authorization_digest = str(authorization["digest"])

    stopped_proof: dict[str, object] = {
        "schema": "claims-stopped-process-proof/v1",
        "authority_id": store.authority_id,
        "security_level": "R0_COOPERATIVE",
        "observer_kind": "independent-process-observer",
        "observer_receipt_digest": _digest("b"),
        "target_kind": target_kind,
        "target_id": target_id,
        "unknown_receipt_digest": unknown_receipt_digest,
        "authorization_digest": authorization_digest,
        "process_identity_digest": process_identity_digest,
        "status": "stopped",
        "observed_at": now.isoformat().replace("+00:00", "Z"),
    }
    stopped_proof["digest"] = canonical_digest(stopped_proof)
    stopped_process_digest = str(stopped_proof["digest"])

    for directory_name, digest, payload in (
        ("operator-authorizations", authorization_digest, authorization),
        ("stopped-process-proofs", stopped_process_digest, stopped_proof),
    ):
        directory = store.test_paths.authority_dir / directory_name
        directory.mkdir(mode=0o700, exist_ok=True)
        path = directory / f"{digest.removeprefix('sha256:')}.json"
        path.write_text(
            json.dumps(
                payload,
                ensure_ascii=True,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        path.chmod(0o600)
    return authorization_digest, stopped_process_digest


def test_green_authorized_unknown_mutation_resolution_settles_same_batch_idempotently(
    store: _AuthorityStore,
) -> None:
    claim = _observe_member(store, run_id="run-batch", ordinal=0)
    begin = store.begin_claim_mutation(_begin_mutation_request(store, [claim]))
    unknown = store.settle_claim_mutation(
        _settle_mutation_request(
            store,
            begin,
            outcome="unknown",
            resulting_run_digest=_digest("d"),
            resulting_lock_set_digest=_digest("f"),
        )
    )
    marker_id = str(uuid4())
    marker = store.mark_claim_mutation_operator_required(_mark_mutation_request(store, unknown, request_id=marker_id))
    repeated_marker = store.mark_claim_mutation_operator_required(
        _mark_mutation_request(store, unknown, request_id=marker_id)
    )
    assert marker == repeated_marker
    assert marker["operator_required"] is True

    resolution_id = str(uuid4())
    request = _resolve_mutation_request(
        store,
        unknown,
        begin["members"],
        request_id=resolution_id,
    )
    resolved = store.resolve_claim_mutation_unknown(request)
    repeated = store.resolve_claim_mutation_unknown(request)

    assert resolved == repeated
    assert resolved["mutation_batch_id"] == unknown["mutation_batch_id"]
    assert resolved["state"] == "settled"
    assert resolved["outcome"] == "applied"
    assert resolved["members"][0]["lease_epoch"] == int(claim["lease_epoch"]) + 1


def test_red_production_unknown_mutation_resolution_rejects_self_attested_proof_digests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    production = _AuthorityStore._connect_new(
        tmp_path / "authority",
        "omo-claims-authority-r0",
        test_only=False,
    )
    production.activate_shadow(valid_activation_request(production))
    claim = _observe_member(production, run_id="run-production-batch", ordinal=0)
    begin = production.begin_claim_mutation(_begin_mutation_request(production, [claim], run_id="run-production-batch"))
    unknown = production.settle_claim_mutation(
        _settle_mutation_request(
            production,
            begin,
            outcome="unknown",
            resulting_run_digest=_digest("d"),
            resulting_lock_set_digest=_digest("f"),
        )
    )
    production.mark_claim_mutation_operator_required(_mark_mutation_request(production, unknown))
    request = _resolve_mutation_request(production, unknown, begin["members"])
    paths = production.test_paths
    production._connection.close()
    monkeypatch.setattr(claims_authority, "resolve_authority_paths", lambda: paths)

    with pytest.raises(AuthorityError, match="OPERATOR_AUTHORIZATION_REQUIRED"):
        claims_authority.resolve_claim_mutation_unknown(request)


def test_green_production_unknown_mutation_resolution_uses_canonical_operator_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    production = _AuthorityStore._connect_new(
        tmp_path / "authority",
        "omo-claims-authority-r0",
        test_only=False,
    )
    production.activate_shadow(valid_activation_request(production))
    claim = _observe_member(production, run_id="run-production-batch", ordinal=0)
    begin = production.begin_claim_mutation(_begin_mutation_request(production, [claim], run_id="run-production-batch"))
    unknown = production.settle_claim_mutation(
        _settle_mutation_request(
            production,
            begin,
            outcome="unknown",
            resulting_run_digest=_digest("d"),
            resulting_lock_set_digest=_digest("f"),
        )
    )
    production.mark_claim_mutation_operator_required(_mark_mutation_request(production, unknown))
    request = _resolve_mutation_request(production, unknown, begin["members"])
    authorization_digest, stopped_process_digest = _write_operator_resolution_evidence(
        production,
        target_kind="claim_mutation",
        target_field="mutation_batch_id",
        target_id=str(unknown["mutation_batch_id"]),
        unknown_operation="settle-claim-mutation",
        resolver_operation="resolve-claim-mutation-unknown",
        outcome="applied",
        process_identity_digest=_digest("9"),
    )
    request.update(
        {
            "authorization_digest": authorization_digest,
            "stopped_process_digest": stopped_process_digest,
        }
    )
    paths = production.test_paths
    production._connection.close()
    monkeypatch.setattr(claims_authority, "resolve_authority_paths", lambda: paths)

    resolved = claims_authority.resolve_claim_mutation_unknown(request)
    repeated = claims_authority.resolve_claim_mutation_unknown(request)

    assert resolved == repeated
    assert resolved["state"] == "settled"
    assert resolved["outcome"] == "applied"


def test_red_unknown_mutation_operator_marker_cannot_switch_process_identity(
    store: _AuthorityStore,
) -> None:
    claim = _observe_member(store, run_id="run-batch", ordinal=0)
    begin = store.begin_claim_mutation(_begin_mutation_request(store, [claim]))
    unknown = store.settle_claim_mutation(
        _settle_mutation_request(
            store,
            begin,
            outcome="unknown",
            resulting_run_digest=_digest("d"),
            resulting_lock_set_digest=_digest("f"),
        )
    )
    marker = _mark_mutation_request(store, unknown)
    marker["mutation_process_identity_digest"] = _digest("8")

    with pytest.raises(AuthorityError, match="IDENTITY_MISMATCH"):
        store.mark_claim_mutation_operator_required(marker)


def test_red_fence_replay_and_expiry_issue_no_replacement(store: _AuthorityStore) -> None:
    claim, descriptor_digest = _active_claim_and_descriptor(store)
    request = _issue_fence_request(store, claim, descriptor_digest)
    fence = store.issue_legacy_fence(request)

    with pytest.raises(AuthorityError, match="LEGACY_FENCE_REPLAY"):
        store.issue_legacy_fence({**request, "request_id": str(uuid4())})

    store._connection.execute(
        "UPDATE legacy_fences SET expires_at='2000-01-01T00:00:00Z' WHERE fence_id=?",
        (fence["fence_id"],),
    )
    with pytest.raises(AuthorityError, match="PUBLISH_INTENT_EXPIRED"):
        store.enter_legacy_publishing(_enter_fence_request(store, fence))


def test_red_child_broker_rejects_unbound_or_disagreeing_remote_observation_pair(
    store: _AuthorityStore,
) -> None:
    claim, descriptor_digest = _active_claim_and_descriptor(store)
    request = _issue_fence_request(store, claim, descriptor_digest)
    second = dict(request["second_remote_observation"])
    second["observed_remote_oid"] = "c" * 40
    request["second_remote_observation"] = second

    with pytest.raises(AuthorityError, match="REMOTE_OID_DRIFT"):
        store.issue_legacy_fence(request)

    assert store.scalar("SELECT COUNT(*) FROM legacy_fences") == 0


def test_red_publishing_claim_is_frozen_until_settlement(store: _AuthorityStore) -> None:
    claim, descriptor_digest = _active_claim_and_descriptor(store)
    fence = store.issue_legacy_fence(_issue_fence_request(store, claim, descriptor_digest))
    store.enter_legacy_publishing(_enter_fence_request(store, fence))

    for operation in ("claim", "heartbeat", "close", "takeover", "expire"):
        with pytest.raises(AuthorityError, match="LEGACY_DRAIN_INCOMPLETE"):
            store.begin_claim_mutation(_begin_mutation_request(store, [claim], operation=operation, run_id="run-fence"))


def test_green_repeated_settlement_returns_same_receipt(store: _AuthorityStore) -> None:
    claim, descriptor_digest = _active_claim_and_descriptor(store)
    fence = store.issue_legacy_fence(_issue_fence_request(store, claim, descriptor_digest))
    entered = store.enter_legacy_publishing(_enter_fence_request(store, fence))
    request = _settle_fence_request(store, fence, request_id=str(entered["settlement_request_id"]))

    first = store.settle_legacy_publication(request)
    second = store.settle_legacy_publication(request)

    assert first == second
    assert first["state"] == "settled"
    assert first["outcome"] == "success"
    assert first["settlement_request_id"] == entered["settlement_request_id"]


def test_green_begin_allocates_immutable_settlement_request_id(store: _AuthorityStore) -> None:
    claim = _observe_member(store, run_id="run-batch", ordinal=0)
    begin = store.begin_claim_mutation(_begin_mutation_request(store, [claim]))
    row = store._connection.execute(
        "SELECT settlement_request_id, state FROM claim_mutation_batches WHERE mutation_id=?",
        (begin["mutation_batch_id"],),
    ).fetchone()

    assert row is not None
    assert row["state"] == "reserved"
    assert begin["settlement_request_id"] == row["settlement_request_id"]
    claims_authority._uuid4(begin["settlement_request_id"])


def test_green_mutation_settlement_replay_and_mismatch_use_allocated_id(store: _AuthorityStore) -> None:
    claim = _observe_member(store, run_id="run-batch", ordinal=0)
    begin = store.begin_claim_mutation(_begin_mutation_request(store, [claim]))
    request = _settle_mutation_request(
        store,
        begin,
        outcome="applied",
        resulting_run_digest=_digest("d"),
        resulting_lock_set_digest=_digest("f"),
    )

    with pytest.raises(AuthorityError, match="REQUEST_SCHEMA_INVALID"):
        store.settle_claim_mutation({**request, "request_id": str(uuid4())})

    first = store.settle_claim_mutation(request)
    second = store.settle_claim_mutation(request)
    assert first == second
    assert first["settlement_request_id"] == begin["settlement_request_id"]

    with pytest.raises(AuthorityError, match="REQUEST_ID_REUSE_MISMATCH"):
        store.settle_claim_mutation({**request, "resulting_lock_set_digest": _digest("a")})


def test_green_legacy_enter_allocates_settlement_request_id_and_replay_is_bound(
    store: _AuthorityStore,
) -> None:
    claim, descriptor_digest = _active_claim_and_descriptor(store)
    fence = store.issue_legacy_fence(_issue_fence_request(store, claim, descriptor_digest))
    issued_row = store._connection.execute(
        "SELECT settlement_request_id FROM legacy_fences WHERE fence_id=?",
        (fence["fence_id"],),
    ).fetchone()
    assert issued_row is not None
    assert issued_row["settlement_request_id"] is None

    entered = store.enter_legacy_publishing(_enter_fence_request(store, fence))
    publishing_row = store._connection.execute(
        "SELECT settlement_request_id, state FROM legacy_fences WHERE fence_id=?",
        (fence["fence_id"],),
    ).fetchone()
    assert publishing_row is not None
    assert publishing_row["state"] == "publishing"
    assert entered["settlement_request_id"] == publishing_row["settlement_request_id"]

    request = _settle_fence_request(store, fence)
    with pytest.raises(AuthorityError, match="REQUEST_SCHEMA_INVALID"):
        store.settle_legacy_publication({**request, "request_id": str(uuid4())})

    first = store.settle_legacy_publication(request)
    second = store.settle_legacy_publication(request)
    assert first == second
    assert first["settlement_request_id"] == entered["settlement_request_id"]

    with pytest.raises(AuthorityError, match="REQUEST_ID_REUSE_MISMATCH"):
        store.settle_legacy_publication({**request, "outcome": "rejected"})


@pytest.mark.parametrize(
    ("outcome", "observed_remote_oid"),
    [("success", "a" * 40), ("rejected", "b" * 40)],
)
def test_red_legacy_settlement_outcome_requires_exact_bound_oid(
    store: _AuthorityStore,
    outcome: str,
    observed_remote_oid: str,
) -> None:
    claim, descriptor_digest = _active_claim_and_descriptor(store)
    fence = store.issue_legacy_fence(_issue_fence_request(store, claim, descriptor_digest))
    store.enter_legacy_publishing(_enter_fence_request(store, fence))
    request = _settle_fence_request(store, fence, outcome=outcome)
    request["observed_remote_oid"] = observed_remote_oid

    with pytest.raises(AuthorityError, match="REMOTE_OID_DRIFT"):
        store.settle_legacy_publication(request)


def test_red_unknown_resolution_requires_all_operator_proofs(store: _AuthorityStore) -> None:
    claim, descriptor_digest = _active_claim_and_descriptor(store)
    fence = store.issue_legacy_fence(_issue_fence_request(store, claim, descriptor_digest))
    store.enter_legacy_publishing(_enter_fence_request(store, fence))
    unknown = store.settle_legacy_publication(_settle_fence_request(store, fence, outcome="unknown"))

    with pytest.raises(AuthorityError, match="REQUEST_SCHEMA_INVALID"):
        store.resolve_legacy_unknown(
            {
                "schema": "claim-mutation-envelope/v2",
                "request_id": str(uuid4()),
                "authority_id": store.authority_id,
                "operation": "resolve-legacy-unknown",
                "fence_id": unknown["fence_id"],
            }
        )

    with pytest.raises(AuthorityError, match="OPERATOR_AUTHORIZATION_REQUIRED"):
        store.resolve_legacy_unknown(_resolve_fence_request(store, unknown))


def _mark_fence_request(
    store: _AuthorityStore,
    fence: dict[str, object],
    *,
    request_id: str | None = None,
) -> dict[str, object]:
    return {
        "schema": "claim-mutation-envelope/v2",
        "request_id": request_id or str(uuid4()),
        "authority_id": store.authority_id,
        "operation": "mark-legacy-operator-required",
        "fence_id": fence["fence_id"],
        "reason_code": "PUBLISH_PROCESS_RESULT_UNKNOWN",
        "effect_process_identity_digest": _digest("9"),
    }


def _resolve_fence_request(
    store: _AuthorityStore,
    fence: dict[str, object],
    *,
    outcome: str = "success",
    observed_remote_oid: str = "b" * 40,
    request_id: str | None = None,
) -> dict[str, object]:
    first, second = _remote_observation_pair(
        remote_ref=str(fence["remote_ref"]),
        observed_remote_oid=observed_remote_oid,
        descriptor_digest=str(fence["descriptor_digest"]),
    )
    return {
        "schema": "claim-mutation-envelope/v2",
        "request_id": request_id or str(uuid4()),
        "authority_id": store.authority_id,
        "operation": "resolve-legacy-unknown",
        "fence_id": fence["fence_id"],
        "authorization_digest": _digest("d"),
        "stopped_process_digest": _digest("e"),
        "remote_ref": fence["remote_ref"],
        "first_remote_observation": first,
        "second_remote_observation": second,
        "outcome": outcome,
    }


def test_green_authorized_unknown_fence_resolution_settles_same_fence_idempotently(
    store: _AuthorityStore,
) -> None:
    claim, descriptor_digest = _active_claim_and_descriptor(store)
    fence = store.issue_legacy_fence(_issue_fence_request(store, claim, descriptor_digest))
    store.enter_legacy_publishing(_enter_fence_request(store, fence))
    unknown = store.settle_legacy_publication(_settle_fence_request(store, fence, outcome="unknown"))
    marker_id = str(uuid4())
    marker = store.mark_legacy_operator_required(_mark_fence_request(store, unknown, request_id=marker_id))
    repeated_marker = store.mark_legacy_operator_required(_mark_fence_request(store, unknown, request_id=marker_id))
    assert marker == repeated_marker
    assert marker["operator_required"] is True

    resolution_id = str(uuid4())
    request = _resolve_fence_request(store, unknown, request_id=resolution_id)
    resolved = store.resolve_legacy_unknown(request)
    repeated = store.resolve_legacy_unknown(request)

    assert resolved == repeated
    assert resolved["fence_id"] == unknown["fence_id"]
    assert resolved["state"] == "settled"
    assert resolved["outcome"] == "success"


def test_red_production_unknown_fence_resolution_rejects_self_attested_proof_digests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    production = _AuthorityStore._connect_new(
        tmp_path / "authority",
        "omo-claims-authority-r0",
        test_only=False,
    )
    claim, descriptor_digest = _active_claim_and_descriptor(production)
    fence = production.issue_legacy_fence(_issue_fence_request(production, claim, descriptor_digest))
    production.enter_legacy_publishing(_enter_fence_request(production, fence))
    unknown = production.settle_legacy_publication(_settle_fence_request(production, fence, outcome="unknown"))
    production.mark_legacy_operator_required(_mark_fence_request(production, unknown))
    request = _resolve_fence_request(production, unknown)
    paths = production.test_paths
    production._connection.close()
    monkeypatch.setattr(claims_authority, "resolve_authority_paths", lambda: paths)

    with pytest.raises(AuthorityError, match="OPERATOR_AUTHORIZATION_REQUIRED"):
        claims_authority.resolve_legacy_unknown(request)


def test_green_production_unknown_fence_resolution_uses_canonical_operator_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    production = _AuthorityStore._connect_new(
        tmp_path / "authority",
        "omo-claims-authority-r0",
        test_only=False,
    )
    claim, descriptor_digest = _active_claim_and_descriptor(production)
    fence = production.issue_legacy_fence(_issue_fence_request(production, claim, descriptor_digest))
    production.enter_legacy_publishing(_enter_fence_request(production, fence))
    unknown = production.settle_legacy_publication(_settle_fence_request(production, fence, outcome="unknown"))
    production.mark_legacy_operator_required(_mark_fence_request(production, unknown))
    request = _resolve_fence_request(production, unknown)
    authorization_digest, stopped_process_digest = _write_operator_resolution_evidence(
        production,
        target_kind="legacy_fence",
        target_field="fence_id",
        target_id=str(unknown["fence_id"]),
        unknown_operation="settle-legacy-publication",
        resolver_operation="resolve-legacy-unknown",
        outcome="success",
        process_identity_digest=_digest("9"),
    )
    request.update(
        {
            "authorization_digest": authorization_digest,
            "stopped_process_digest": stopped_process_digest,
        }
    )
    paths = production.test_paths
    production._connection.close()
    monkeypatch.setattr(claims_authority, "resolve_authority_paths", lambda: paths)

    resolved = claims_authority.resolve_legacy_unknown(request)
    repeated = claims_authority.resolve_legacy_unknown(request)

    assert resolved == repeated
    assert resolved["state"] == "settled"
    assert resolved["outcome"] == "success"


def test_red_unknown_fence_operator_marker_cannot_switch_effect_process_identity(
    store: _AuthorityStore,
) -> None:
    claim, descriptor_digest = _active_claim_and_descriptor(store)
    fence = store.issue_legacy_fence(_issue_fence_request(store, claim, descriptor_digest))
    store.enter_legacy_publishing(_enter_fence_request(store, fence))
    unknown = store.settle_legacy_publication(_settle_fence_request(store, fence, outcome="unknown"))
    marker = _mark_fence_request(store, unknown)
    marker["effect_process_identity_digest"] = _digest("8")

    with pytest.raises(AuthorityError, match="IDENTITY_MISMATCH"):
        store.mark_legacy_operator_required(marker)

    row = store._connection.execute(
        "SELECT operator_required FROM legacy_fences WHERE fence_id=?",
        (unknown["fence_id"],),
    ).fetchone()
    assert row[0] == 0


@pytest.mark.parametrize("state", ["issued", "publishing", "unknown"])
def test_red_graduation_rejects_every_unresolved_fence_state(store: _AuthorityStore, state: str) -> None:
    claim, descriptor_digest = _active_claim_and_descriptor(store)
    fence = store.issue_legacy_fence(_issue_fence_request(store, claim, descriptor_digest))
    if state in {"publishing", "unknown"}:
        store.enter_legacy_publishing(_enter_fence_request(store, fence))
    if state == "unknown":
        store.settle_legacy_publication(_settle_fence_request(store, fence, outcome="unknown"))

    with pytest.raises(AuthorityError, match="LEGACY_DRAIN_INCOMPLETE"):
        store.evaluate_graduation()


def test_red_broker_clock_rollback_issues_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    current = {"value": initial}
    monkeypatch.setattr(claims_authority, "_clock_now", lambda: current["value"])
    clocked = _AuthorityStore.connect_for_test(
        tmp_path / "authority",
        authority_id=f"test:{uuid4()}",
    )
    current["value"] = initial + timedelta(seconds=60)
    clocked.observe_claim(valid_observe_request())
    sequence_before = clocked.scalar("SELECT COUNT(*) FROM receipts")

    current["value"] = initial
    with pytest.raises(AuthorityError, match="AUTHORITY_CLOCK_ROLLBACK"):
        clocked.observe_claim({**valid_observe_request(), "request_id": str(uuid4())})

    assert clocked.scalar("SELECT COUNT(*) FROM receipts") == sequence_before


def test_red_lifecycle_calls_only_integration_root_broker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account_home = tmp_path / "account"
    integration_root = account_home / "Workspace"
    integration_root.mkdir(parents=True)
    monkeypatch.setattr(
        lifecycle,
        "pwd",
        SimpleNamespace(getpwuid=lambda _uid: SimpleNamespace(pw_dir=str(account_home))),
        raising=False,
    )
    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout='{"activation_state":"unactivated"}\n', stderr="")

    monkeypatch.setattr(lifecycle.subprocess, "run", fake_run)

    status = lifecycle._call_claims_authority("status", None)

    assert status["activation_state"] == "unactivated"
    assert calls == [
        (
            [
                str(integration_root / "bin/gac/managed-python"),
                "run",
                "--profile",
                "stdlib",
                "--",
                str(integration_root / "bin/agent-workflow.py"),
                "claims-authority",
                "status",
                "--json",
            ],
            {
                "cwd": integration_root,
                "input": None,
                "text": True,
                "capture_output": True,
                "check": False,
                "timeout": 5.0,
            },
        )
    ]
    tree = ast.parse(inspect.getsource(lifecycle))
    forbidden_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and str(node.module or "").endswith("claims_authority")
    ]
    assert forbidden_imports == []


def _seed_authority_lifecycle_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    run_id: str = "run-authority",
) -> tuple[dict[str, object], Path, Path]:
    import omo.workflow.core as core

    monkeypatch.setattr(lifecycle, "WORKSPACE", tmp_path)
    monkeypatch.setattr(core, "WORKSPACE", tmp_path)
    registry: dict[str, object] = {
        "runner": {
            "workspace_root": str(tmp_path),
            "run_state_dir": "runs",
            "lock_state_dir": "locks",
            "ledger_path": "events.jsonl",
        }
    }
    git_dir = tmp_path / ".git"
    git_dir.mkdir(parents=True)
    identity = {
        "schema": "agent-clone-identity/v2",
        "actor_id": "agent-a",
        "delivery_attempt_id": "attempt-a",
        "canonical_root": str(tmp_path),
        "frozen_root_sha": "a" * 40,
        "transport": {},
    }
    provenance = {
        "schema": "clone-provenance/v2",
        "working_branch": "agent/agent-a--attempt-a",
        "repository": {"canonical_repository": "github.com/starlink-awaken/omostation"},
    }
    readiness = {"schema": "agent-clone-readiness/v1", "receipt_digest": "4" * 64}
    for name, payload in (
        ("agent-clone-identity.json", identity),
        ("agent-clone-provenance.json", provenance),
        ("agent-clone-readiness.json", readiness),
    ):
        (git_dir / name).write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
    (git_dir / "HEAD").write_text("a" * 40 + "\n", encoding="utf-8")
    lock_path = tmp_path / "locks/path_existing.py.lock.yaml"
    lock_path.parent.mkdir(parents=True)
    now = "2026-09-10T12:00:00Z"
    lock_path.write_text(
        yaml.safe_dump(
            {
                "run_id": run_id,
                "actor": "agent-a",
                "scope": "path:existing.py",
                "created_at": now,
                "last_heartbeat": now,
                "expires_at": "2026-09-11T12:00:00Z",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    run_path = tmp_path / f"runs/{run_id}.yaml"
    run_path.parent.mkdir(parents=True)
    run_path.write_text(
        yaml.safe_dump(
            {
                "run_id": run_id,
                "workflow_id": "bet-execution",
                "status": "active",
                "actor": "agent-a",
                "bet_id": "BET-Y1Q4-T10-145",
                "spec_binding": {
                    "spec_ref": "repo://docs/superpowers/specs/claims-authority.md",
                    "content_digest": _digest("1"),
                },
                "work_packet": {"packet_id": "WP-BET-Y1Q4-T10-145"},
                "work_packet_hash": _digest("2"),
                "claims": [
                    {
                        "paths": ["existing.py"],
                        "surfaces": [],
                        "locks": [str(lock_path)],
                        "affected_graph": {"receipt_hash": "3" * 64},
                    }
                ],
                "locks": [str(lock_path)],
                "created_at": now,
                "updated_at": now,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return registry, run_path, lock_path


@pytest.mark.parametrize("operation", ["heartbeat", "close"])
def test_green_lifecycle_heartbeat_and_close_use_begin_write_settle_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    registry, run_path, _lock_path = _seed_authority_lifecycle_run(tmp_path, monkeypatch)
    order: list[str] = []
    monkeypatch.setattr(lifecycle, "_authority_mode", lambda _payload: "shadow-active")

    def fake_begin(
        _registry: dict[str, object],
        _run_id: str,
        actual_operation: str,
        _snapshot: dict[str, object],
    ) -> dict[str, object]:
        assert actual_operation == operation
        order.append("begin")
        return {
            "mutation_batch_id": _digest("4"),
            "members": [{"claim_id": _digest("5"), "claim_version": 1, "lease_epoch": 1}],
        }

    def fake_settle(
        _registry: dict[str, object],
        _run_id: str,
        actual_operation: str,
        _begin: dict[str, object],
        _snapshot: dict[str, object],
        *,
        outcome: str,
    ) -> dict[str, object]:
        assert actual_operation == operation
        assert outcome == "applied"
        order.append("settle")
        return {"state": "settled"}

    monkeypatch.setattr(lifecycle, "_authority_begin_mutation", fake_begin)
    monkeypatch.setattr(lifecycle, "_authority_settle_mutation", fake_settle)
    if operation == "heartbeat":
        original_write = lifecycle.write_yaml_atomic

        def write_spy(path: Path, payload: dict[str, object]) -> None:
            order.append("write")
            original_write(path, payload)

        monkeypatch.setattr(lifecycle, "write_yaml_atomic", write_spy)
        lifecycle.heartbeat_run(registry, "run-authority")
    else:
        original_write_text = Path.write_text

        def run_write_spy(path: Path, data: str, *args: object, **kwargs: object) -> int:
            if path == run_path:
                order.append("write")
            return original_write_text(path, data, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", run_write_spy)
        lifecycle.close_run(
            registry,
            "run-authority",
            "blocked",
            ["test"],
            release=False,
            emit_mesh=False,
        )

    assert order == ["begin", "write", "settle"]
    serialized = run_path.read_text(encoding="utf-8")
    assert "claim_version" not in serialized
    assert "lease_epoch" not in serialized


def test_red_post_activation_force_preserves_live_lock_and_v1_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, run_path, lock_path = _seed_authority_lifecycle_run(tmp_path, monkeypatch)
    before_run = run_path.read_bytes()
    before_lock = lock_path.read_bytes()
    heartbeat_calls: list[str] = []
    monkeypatch.setattr(
        lifecycle,
        "heartbeat_run",
        lambda _registry, run_id: heartbeat_calls.append(run_id),
    )
    monkeypatch.setattr(lifecycle, "_authority_mode", lambda _payload: "shadow-active")
    monkeypatch.setattr(lifecycle, "_validate_work_packet_claim", lambda *_args, **_kwargs: None)

    with pytest.raises(WorkflowError, match="force is forbidden"):
        lifecycle.claim_run(
            registry,
            "run-authority",
            "agent-a",
            paths=["new.py"],
            surfaces=[],
            force_lock=True,
            affected_receipt="unused.json",
        )

    assert run_path.read_bytes() == before_run
    assert lock_path.read_bytes() == before_lock
    assert heartbeat_calls == []


def test_red_broker_versions_never_mutate_v1_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, run_path, _lock_path = _seed_authority_lifecycle_run(tmp_path, monkeypatch)
    order: list[str] = []
    members = [{"claim_id": _digest("5"), "claim_version": 1, "lease_epoch": 1}]
    monkeypatch.setattr(lifecycle, "heartbeat_run", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(lifecycle, "_validate_work_packet_claim", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        lifecycle,
        "validate_affected_graph_receipt",
        lambda *_args, **_kwargs: {"receipt_hash": "6" * 64},
    )
    monkeypatch.setattr(lifecycle, "_authority_mode", lambda _payload: "shadow-active")

    def begin(*_args: object, **_kwargs: object) -> dict[str, object]:
        order.append("begin")
        return {"mutation_batch_id": _digest("4"), "members": members}

    def settle(*_args: object, **kwargs: object) -> dict[str, object]:
        assert kwargs["outcome"] == "applied"
        order.append("settle")
        return {"state": "settled"}

    original_write = lifecycle.write_run

    def write_spy(path: Path, payload: dict[str, object]) -> None:
        order.append("write")
        original_write(path, payload)

    monkeypatch.setattr(lifecycle, "_authority_begin_mutation", begin)
    monkeypatch.setattr(lifecycle, "_authority_settle_mutation", settle)
    monkeypatch.setattr(lifecycle, "write_run", write_spy)

    result = lifecycle.claim_run(
        registry,
        "run-authority",
        "agent-a",
        paths=["new.py"],
        surfaces=[],
        force_lock=False,
        affected_receipt="unused.json",
    )

    assert result["paths"] == ["new.py"]
    assert "claim_version" not in result
    assert "lease_epoch" not in result
    assert order == ["begin", "write", "settle"]
    serialized = run_path.read_text(encoding="utf-8")
    assert "claim_version" not in serialized
    assert "lease_epoch" not in serialized
    ledger = (tmp_path / "events.jsonl").read_text(encoding="utf-8")
    assert "claim_version" not in ledger
    assert "lease_epoch" not in ledger


def test_green_lifecycle_pristine_witness_preserves_bootstrap_after_canonical_stdio_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, run_path, lock_path = _seed_authority_lifecycle_run(tmp_path / "workspace", monkeypatch)
    account_home = tmp_path / "account"
    account_home.mkdir()
    monkeypatch.setattr(
        lifecycle,
        "pwd",
        SimpleNamespace(getpwuid=lambda _uid: SimpleNamespace(pw_dir=str(account_home))),
    )
    stdio_calls: list[str] = []

    def unavailable_stdio(verb: str, _request: object) -> dict[str, object]:
        stdio_calls.append(verb)
        raise WorkflowError("AUTHORITY_UNAVAILABLE")

    monkeypatch.setattr(lifecycle, "_call_claims_authority", unavailable_stdio)
    before_run = run_path.read_bytes()
    before_lock = lock_path.read_bytes()

    result = lifecycle.heartbeat_run(registry, "run-authority")

    assert result["count"] == 1
    assert run_path.read_bytes() == before_run
    assert lock_path.read_bytes() != before_lock
    event = json.loads((tmp_path / "workspace/events.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert event["event"] == "shadow_unprovable"
    assert event["code"] == "not_activated"
    assert stdio_calls == ["status"]


def test_red_changed_v1_failure_is_settled_unknown_marked_and_preserves_original_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, run_path, _lock_path = _seed_authority_lifecycle_run(tmp_path, monkeypatch)
    begin = {
        "mutation_batch_id": _digest("4"),
        "members": [{"claim_id": _digest("5"), "claim_version": 1, "lease_epoch": 1}],
        "mutation_process_identity_digest": _digest("9"),
    }
    settlements: list[str] = []
    markers: list[str] = []
    monkeypatch.setattr(lifecycle, "_authority_mode", lambda _payload: "shadow-active")
    monkeypatch.setattr(lifecycle, "_authority_begin_mutation", lambda *_args, **_kwargs: begin)

    def settle(*_args: object, **kwargs: object) -> dict[str, object]:
        settlements.append(str(kwargs["outcome"]))
        return {
            "sequence": 2,
            "receipt_digest": _digest("6"),
            "mutation_batch_id": begin["mutation_batch_id"],
            "state": "unknown",
        }

    monkeypatch.setattr(lifecycle, "_authority_settle_mutation", settle)
    monkeypatch.setattr(
        lifecycle,
        "_authority_mark_mutation_operator_required",
        lambda *_args, **_kwargs: markers.append("marked") or {"operator_required": True},
        raising=False,
    )

    with pytest.raises(RuntimeError, match="original-v1-error"):
        with lifecycle._authority_mutation_locked(registry, "run-authority", "heartbeat"):
            run_path.write_text(run_path.read_text(encoding="utf-8") + "changed: true\n", encoding="utf-8")
            raise RuntimeError("original-v1-error")

    assert settlements == ["unknown"]
    assert markers == ["marked"]


def test_red_successful_v1_write_with_unknown_settlement_marks_same_batch_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, run_path, _lock_path = _seed_authority_lifecycle_run(tmp_path, monkeypatch)
    begin = {
        "mutation_batch_id": _digest("4"),
        "members": [{"claim_id": _digest("5"), "claim_version": 1, "lease_epoch": 1}],
        "mutation_process_identity_digest": _digest("9"),
    }
    settlements = 0
    markers: list[tuple[str, str]] = []
    monkeypatch.setattr(lifecycle, "_authority_mode", lambda _payload: "shadow-active")
    monkeypatch.setattr(lifecycle, "_authority_begin_mutation", lambda *_args, **_kwargs: begin)

    def unavailable_settlement(*_args: object, **_kwargs: object) -> dict[str, object]:
        nonlocal settlements
        settlements += 1
        raise WorkflowError("AUTHORITY_UNAVAILABLE")

    monkeypatch.setattr(lifecycle, "_authority_settle_mutation", unavailable_settlement)
    monkeypatch.setattr(
        lifecycle,
        "_authority_mark_mutation_operator_required",
        lambda actual_begin, *, reason_code: (
            markers.append((str(actual_begin["mutation_batch_id"]), reason_code)) or {"operator_required": True}
        ),
    )

    with pytest.raises(WorkflowError, match="AUTHORITY_UNAVAILABLE"):
        with lifecycle._authority_mutation_locked(registry, "run-authority", "heartbeat"):
            run_path.write_text(run_path.read_text(encoding="utf-8") + "changed: true\n", encoding="utf-8")

    assert settlements == 1
    assert markers == [(_digest("4"), "SETTLEMENT_RESULT_UNKNOWN")]
    event = json.loads((tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert event["event"] == "shadow_unprovable"
    assert event["code"] == "settlement_result_unknown"


def test_red_changed_v1_failure_with_unknown_settlement_preserves_original_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, run_path, _lock_path = _seed_authority_lifecycle_run(tmp_path, monkeypatch)
    begin = {
        "mutation_batch_id": _digest("4"),
        "members": [{"claim_id": _digest("5"), "claim_version": 1, "lease_epoch": 1}],
        "mutation_process_identity_digest": _digest("9"),
    }
    settlements = 0
    markers: list[str] = []
    monkeypatch.setattr(lifecycle, "_authority_mode", lambda _payload: "shadow-active")
    monkeypatch.setattr(lifecycle, "_authority_begin_mutation", lambda *_args, **_kwargs: begin)

    def unavailable_settlement(*_args: object, **_kwargs: object) -> dict[str, object]:
        nonlocal settlements
        settlements += 1
        raise WorkflowError("AUTHORITY_UNAVAILABLE")

    monkeypatch.setattr(lifecycle, "_authority_settle_mutation", unavailable_settlement)
    monkeypatch.setattr(
        lifecycle,
        "_authority_mark_mutation_operator_required",
        lambda *_args, **_kwargs: markers.append("marked") or {"operator_required": True},
    )

    with pytest.raises(RuntimeError, match="original-v1-error"):
        with lifecycle._authority_mutation_locked(registry, "run-authority", "heartbeat"):
            run_path.write_text(run_path.read_text(encoding="utf-8") + "changed: true\n", encoding="utf-8")
            raise RuntimeError("original-v1-error")

    assert settlements == 1
    assert markers == ["marked"]
    event = json.loads((tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert event["event"] == "shadow_unprovable"
    assert event["code"] == "settlement_result_unknown"


@pytest.mark.parametrize("witness_state", ["prepared", "shadow-active"])
def test_red_lifecycle_activated_or_prepared_witness_blocks_before_v1_write_when_stdio_is_down(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    witness_state: str,
) -> None:
    registry, run_path, lock_path = _seed_authority_lifecycle_run(tmp_path / "workspace", monkeypatch)
    account_home = tmp_path / "account"
    authority_dir = account_home / "agents/_shared/runtime/omo-claims-authority-r0"
    authority_dir.mkdir(parents=True, mode=0o700)
    witness = {
        "schema": "claims-activation-witness/v1",
        "authority_id": "omo-claims-authority-r0",
        "state": witness_state,
        "sequence": 1,
        "descriptor_digest": _digest("a"),
        "activation_receipt_digest": None if witness_state == "prepared" else _digest("b"),
        "request_digest": _digest("c"),
    }
    witness["digest"] = lifecycle._authority_digest(witness)
    witness_path = authority_dir / "activation-witness.json"
    witness_path.write_text(json.dumps(witness, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    witness_path.chmod(0o600)
    monkeypatch.setattr(
        lifecycle,
        "pwd",
        SimpleNamespace(getpwuid=lambda _uid: SimpleNamespace(pw_dir=str(account_home))),
    )
    monkeypatch.setattr(
        lifecycle,
        "_call_claims_authority",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(WorkflowError("AUTHORITY_UNAVAILABLE")),
    )
    before_run = run_path.read_bytes()
    before_lock = lock_path.read_bytes()

    with pytest.raises(WorkflowError, match="AUTHORITY_UNAVAILABLE"):
        lifecycle.heartbeat_run(registry, "run-authority")

    assert run_path.read_bytes() == before_run
    assert lock_path.read_bytes() == before_lock


def test_green_store_backed_lifecycle_heartbeat_and_claim_settle_complete_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, run_path, _lock_path = _seed_authority_lifecycle_run(tmp_path / "workspace", monkeypatch)
    broker_store = _AuthorityStore.connect_for_test(
        tmp_path / "authority",
        authority_id=f"test:{uuid4()}",
    )
    broker_store.activate_shadow(valid_activation_request(broker_store))
    monkeypatch.setattr(
        lifecycle,
        "_authority_witness_state",
        lambda: {"activation_state": "shadow-active"},
    )

    methods = {
        "observe-claim": broker_store.observe_claim,
        "begin-claim-mutation": broker_store.begin_claim_mutation,
        "settle-claim-mutation": broker_store.settle_claim_mutation,
    }

    def broker(verb: str, request: dict[str, object] | None) -> dict[str, object]:
        if verb == "status":
            return broker_store.authority_status()
        assert request is not None
        routed = {**request, "authority_id": broker_store.authority_id}
        return methods[verb](routed)

    monkeypatch.setattr(lifecycle, "_call_claims_authority", broker)

    lifecycle.heartbeat_run(registry, "run-authority")
    assert (
        broker_store.scalar(
            "SELECT COUNT(*) FROM claim_mutation_batches WHERE operation='heartbeat' AND state='settled'"
        )
        == 1
    )

    monkeypatch.setattr(lifecycle, "heartbeat_run", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(lifecycle, "_validate_work_packet_claim", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        lifecycle,
        "validate_affected_graph_receipt",
        lambda *_args, **_kwargs: {"receipt_hash": "6" * 64},
    )
    lifecycle.claim_run(
        registry,
        "run-authority",
        "agent-a",
        paths=["new.py"],
        surfaces=[],
        force_lock=False,
        affected_receipt="unused.json",
    )

    assert (
        broker_store.scalar("SELECT COUNT(*) FROM claim_mutation_batches WHERE operation='claim' AND state='settled'")
        == 1
    )
    assert broker_store.scalar("SELECT COUNT(*) FROM claims WHERE run_id='run-authority'") == 2
    versions = [
        int(row[0])
        for row in broker_store._connection.execute(
            "SELECT authority_claim_version FROM claims WHERE run_id='run-authority' ORDER BY authority_claim_version"
        )
    ]
    assert versions == [1, 2]
    serialized = run_path.read_text(encoding="utf-8")
    assert "claim_version" not in serialized
    assert "lease_epoch" not in serialized


def test_red_local_lock_timeout_overlap_performs_zero_second_v1_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    registry, run_path, lock_path = _seed_authority_lifecycle_run(workspace, monkeypatch)
    broker_store = _AuthorityStore.connect_for_test(
        tmp_path / "authority",
        authority_id=f"test:{uuid4()}",
    )
    broker_store.activate_shadow(valid_activation_request(broker_store))
    monkeypatch.setattr(
        lifecycle,
        "_authority_witness_state",
        lambda: {"activation_state": "shadow-active"},
    )
    methods = {
        "observe-claim": broker_store.observe_claim,
        "begin-claim-mutation": broker_store.begin_claim_mutation,
        "settle-claim-mutation": broker_store.settle_claim_mutation,
    }

    def broker(verb: str, request: dict[str, object] | None) -> dict[str, object]:
        if verb == "status":
            return broker_store.authority_status()
        assert request is not None
        return methods[verb]({**request, "authority_id": broker_store.authority_id})

    monkeypatch.setattr(lifecycle, "_call_claims_authority", broker)
    before_bytes = run_path.read_bytes()
    real_run_update_lock = lifecycle.run_update_lock
    active_local = 0
    max_active_local = 0

    @lifecycle.contextmanager
    def tracked_run_update_lock(actual_registry: dict[str, object], run_id: str):
        nonlocal active_local, max_active_local
        with real_run_update_lock(actual_registry, run_id):
            active_local += 1
            max_active_local = max(max_active_local, active_local)
            try:
                yield
            finally:
                active_local -= 1

    monkeypatch.setattr(lifecycle, "run_update_lock", tracked_run_update_lock)
    real_write = lifecycle.write_yaml_atomic
    writes: list[Path] = []
    nested_error: AuthorityError | None = None
    overlap_triggered = False

    def overlap_on_first_v1_write(path: Path, payload: dict[str, object]) -> None:
        nonlocal nested_error, overlap_triggered
        if not overlap_triggered:
            overlap_triggered = True
            update_locks = list((workspace / "locks").glob("run_*.update.lock"))
            assert len(update_locks) == 1
            stale_time = datetime.now(UTC).timestamp() - 31
            os.utime(update_locks[0], (stale_time, stale_time))
            original_nonce = lifecycle._CLAIMS_AUTHORITY_PROCESS_NONCE
            monkeypatch.setattr(lifecycle, "_CLAIMS_AUTHORITY_PROCESS_NONCE", "second-process")
            try:
                lifecycle.heartbeat_run(registry, "run-authority")
            except AuthorityError as exc:
                nested_error = exc
            finally:
                monkeypatch.setattr(lifecycle, "_CLAIMS_AUTHORITY_PROCESS_NONCE", original_nonce)
        writes.append(path)
        real_write(path, payload)

    monkeypatch.setattr(lifecycle, "write_yaml_atomic", overlap_on_first_v1_write)
    receipt = lifecycle.heartbeat_run(registry, "run-authority")

    assert overlap_triggered is True
    assert max_active_local == 2
    assert nested_error is not None
    assert nested_error.code == "CLAIM_VERSION_STALE"
    assert nested_error.detail == "batch_unresolved"
    assert receipt["count"] == 1
    assert writes == [lock_path]
    assert run_path.read_bytes() == before_bytes
    batches = broker_store._connection.execute(
        "SELECT state, outcome FROM claim_mutation_batches WHERE run_id='run-authority' AND operation='heartbeat'"
    ).fetchall()
    assert [(row["state"], row["outcome"]) for row in batches] == [("settled", "applied")]
    assert list((workspace / "locks").glob("run_*.update.lock")) == []
    events = [json.loads(line) for line in (workspace / "events.jsonl").read_text(encoding="utf-8").splitlines()]
    observed = [event for event in events if event.get("event") == "shadow_observed"]
    assert len(observed) == 1
    assert observed[0]["code"] == "v1_applied"


def test_red_prune_deletes_only_frozen_revalidated_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, run_path, changed = _seed_authority_lifecycle_run(tmp_path, monkeypatch)
    stale_at = "2000-01-01T00:00:00Z"
    changed_payload = yaml.safe_load(changed.read_text(encoding="utf-8"))
    changed_payload.update({"last_heartbeat": stale_at, "expires_at": stale_at})
    changed.write_text(yaml.safe_dump(changed_payload, sort_keys=False), encoding="utf-8")
    unchanged = tmp_path / "locks/path_unchanged.py.lock.yaml"
    unchanged_payload = {**changed_payload, "scope": "path:unchanged.py"}
    unchanged.write_text(yaml.safe_dump(unchanged_payload, sort_keys=False), encoding="utf-8")
    run_payload = yaml.safe_load(run_path.read_text(encoding="utf-8"))
    run_payload["locks"] = [str(changed), str(unchanged)]
    run_path.write_text(yaml.safe_dump(run_payload, sort_keys=False), encoding="utf-8")

    real_scan = lifecycle.scan_locks
    scan_calls = 0

    def one_scan(actual_registry: dict[str, object]) -> list[dict[str, object]]:
        nonlocal scan_calls
        scan_calls += 1
        return real_scan(actual_registry)

    monkeypatch.setattr(lifecycle, "scan_locks", one_scan)
    monkeypatch.setattr(lifecycle, "_authority_mode", lambda _payload: "unactivated")
    monkeypatch.setattr(
        lifecycle,
        "_legacy_prune_stale_locks",
        lambda *_args, **_kwargs: pytest.fail("legacy discovery prune must not run"),
    )
    real_run_lock = lifecycle.run_update_lock
    mutated = False

    @lifecycle.contextmanager
    def mutate_after_freeze(actual_registry: dict[str, object], run_id: str):
        nonlocal mutated
        with real_run_lock(actual_registry, run_id):
            if not mutated:
                mutated = True
                changed.write_text(changed.read_text(encoding="utf-8") + "changed: true\n", encoding="utf-8")
                new_candidate = tmp_path / "locks/path_new.py.lock.yaml"
                new_candidate.write_text(
                    yaml.safe_dump({**unchanged_payload, "scope": "path:new.py"}, sort_keys=False),
                    encoding="utf-8",
                )
            yield

    monkeypatch.setattr(lifecycle, "run_update_lock", mutate_after_freeze)
    pruned = lifecycle.prune_stale_locks(registry)

    assert scan_calls == 1
    assert [Path(str(item["path"])).name for item in pruned] == [unchanged.name]
    assert changed.exists()
    assert not unchanged.exists()
    assert (tmp_path / "locks/path_new.py.lock.yaml").exists()
    event = json.loads((tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert event["event"] == "shadow_unprovable"
    assert event["operation"] == "expire"


def test_green_store_backed_prune_settles_expire_batch_for_exact_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, _run_path, lock_path = _seed_authority_lifecycle_run(tmp_path / "workspace", monkeypatch)
    lock = yaml.safe_load(lock_path.read_text(encoding="utf-8"))
    lock.update({"last_heartbeat": "2000-01-01T00:00:00Z", "expires_at": "2000-01-01T00:00:00Z"})
    lock_path.write_text(yaml.safe_dump(lock, sort_keys=False), encoding="utf-8")
    broker_store = _AuthorityStore.connect_for_test(
        tmp_path / "authority",
        authority_id=f"test:{uuid4()}",
    )
    broker_store.activate_shadow(valid_activation_request(broker_store))
    monkeypatch.setattr(
        lifecycle,
        "_authority_witness_state",
        lambda: {"activation_state": "shadow-active"},
    )
    methods = {
        "observe-claim": broker_store.observe_claim,
        "begin-claim-mutation": broker_store.begin_claim_mutation,
        "settle-claim-mutation": broker_store.settle_claim_mutation,
    }

    def broker(verb: str, request: dict[str, object] | None) -> dict[str, object]:
        if verb == "status":
            return broker_store.authority_status()
        assert request is not None
        return methods[verb]({**request, "authority_id": broker_store.authority_id})

    monkeypatch.setattr(lifecycle, "_call_claims_authority", broker)

    pruned = lifecycle.prune_stale_locks(registry)

    assert [Path(str(item["path"])).name for item in pruned] == [lock_path.name]
    assert not lock_path.exists()
    batch = broker_store._connection.execute(
        "SELECT state, outcome FROM claim_mutation_batches WHERE run_id='run-authority' AND operation='expire'"
    ).fetchone()
    assert dict(batch) == {"state": "settled", "outcome": "applied"}
    assert broker_store.scalar("SELECT COUNT(*) FROM claims WHERE run_id='run-authority' AND state='expired'") == 1


def test_red_broker_has_no_remote_transport() -> None:
    tree = ast.parse(inspect.getsource(claims_authority))
    imported = {
        alias.name for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom)) for alias in node.names
    }
    calls = {
        node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }

    assert "subprocess" not in imported
    assert "git" not in imported
    assert "requests" not in imported
    assert "httpx" not in imported
    assert "run" not in calls


# ---------------------------------------------------------------------------
# describe-claim: 只读导出 fence 绑定 (ADR-0461)
# ---------------------------------------------------------------------------


def _describe_request(store: _AuthorityStore, claim_id: str, **extra: object) -> dict[str, object]:
    return {
        "schema": "claim-read-envelope/v1",
        "authority_id": store.authority_id,
        "claim_id": claim_id,
        **extra,
    }


def _publishable_claim(store: _AuthorityStore) -> tuple[dict, dict, str]:
    """建一条 publication-scoped allow 的 claim, 返回 (receipt, scope, descriptor_digest)。"""
    activation = store.activate_shadow(valid_activation_request(store))
    scope = _publication_scope(["docs/reports/2026-10-02-claims-fence-binding.md"])
    request = valid_observe_request()
    request.update(
        {
            "request_id": str(uuid4()),
            "run_id": "run-describe",
            "clone_identity_schema": "agent-clone-identity/v2",
            "v1_claim_digest": canonical_digest({"run_id": "run-describe", "ordinal": 0}),
            "v1_decision": {"decision": "allow", "code": "legacy_allow"},
            "publication_scope": scope,
            "requested_paths_digest": scope["paths_digest"],
        }
    )
    receipt = store.observe_claim(request)
    return receipt, scope, str(activation["descriptor_digest"])


def test_describe_claim_round_trip_satisfies_issue_legacy_fence(
    store: _AuthorityStore,
) -> None:
    """GREEN: describe-claim 的输出必须恰好够拼出被接受的 issue-legacy-fence 请求。

    这是 ADR-0461 的核心契约 —— 此前 fence 要求的绑定全仓无生产方,
    integrate --apply 在 shadow-active 态下 100% 阻塞。
    """
    receipt, scope, descriptor_digest = _publishable_claim(store)
    binding = store.describe_claim(_describe_request(store, str(receipt["claim_id"])))

    assert binding["publishable"] is True
    # 拼装请求时只允许用 describe-claim 给的字段
    fence_req = _issue_fence_request(
        store,
        {
            "claim_id": binding["claim_id"],
            "claim_version": binding["claim_version"],
            "lease_epoch": binding["lease_epoch"],
            "receipt_digest": binding["v1_allow_receipt_digest"],
            "v1_run_digest": binding["v1_snapshot_digest"],
        },
        descriptor_digest,
    )
    fence_req["path_digest"] = binding["paths_digest"]
    fence = store.issue_legacy_fence(fence_req)
    assert fence["operation"] == "issue-legacy-fence"


def test_describe_claim_is_read_only(store: _AuthorityStore) -> None:
    """只读: 连续两次 describe 的 sequence 相同, 且不新增 receipt。"""
    receipt, _scope, _d = _publishable_claim(store)
    before_seq, _ = store._database_tip()
    before_receipts = store._connection.execute(
        "SELECT COUNT(*) FROM receipts WHERE authority_id=?", (store.authority_id,)
    ).fetchone()[0]

    first = store.describe_claim(_describe_request(store, str(receipt["claim_id"])))
    second = store.describe_claim(_describe_request(store, str(receipt["claim_id"])))

    after_seq, _ = store._database_tip()
    after_receipts = store._connection.execute(
        "SELECT COUNT(*) FROM receipts WHERE authority_id=?", (store.authority_id,)
    ).fetchone()[0]
    assert first["sequence"] == second["sequence"] == after_seq
    assert before_seq == after_seq
    assert before_receipts == after_receipts, "只读动词不得写 receipt"
    assert first == second


def test_red_describe_claim_unknown_claim_uses_existing_error_code(
    store: _AuthorityStore,
) -> None:
    """未知 claim 必须复用 issue-legacy-fence 已有的 IDENTITY_MISMATCH, 不新增信息类别。"""
    with pytest.raises(AuthorityError, match="IDENTITY_MISMATCH"):
        store.describe_claim(_describe_request(store, "claim-does-not-exist"))


def test_red_describe_claim_rejects_wrong_authority(store: _AuthorityStore) -> None:
    with pytest.raises(AuthorityError, match="IDENTITY_MISMATCH"):
        store.describe_claim(
            {
                "schema": "claim-read-envelope/v1",
                "authority_id": "some-other-authority",
                "claim_id": "c" * 16,
            }
        )


def test_red_describe_claim_rejects_unknown_fields(store: _AuthorityStore) -> None:
    with pytest.raises(AuthorityError, match="REQUEST_SCHEMA_INVALID"):
        store.describe_claim(_describe_request(store, "c" * 16, allow_publish=True))


def test_red_describe_claim_rejects_wrong_schema(store: _AuthorityStore) -> None:
    with pytest.raises(AuthorityError, match="REQUEST_SCHEMA_INVALID"):
        store.describe_claim(
            {
                "schema": "claim-mutation-envelope/v2",
                "authority_id": store.authority_id,
                "claim_id": "c" * 16,
            }
        )


def test_describe_claim_without_allow_receipt_is_not_publishable(
    store: _AuthorityStore,
) -> None:
    """无 allow 回执的 claim 正常返回并标 publishable=false, 而不是抛错。

    调用方在 issue-legacy-fence 处会看到同样的结果, 故不构成新增信息泄漏。
    """
    store.activate_shadow(valid_activation_request(store))
    request = valid_observe_request()
    request.update(
        {
            "request_id": str(uuid4()),
            "run_id": "run-noallow",
            "clone_identity_schema": "agent-clone-identity/v2",
            "v1_claim_digest": canonical_digest({"run_id": "run-noallow", "ordinal": 0}),
        }
    )
    receipt = store.observe_claim(request)
    binding = store.describe_claim(_describe_request(store, str(receipt["claim_id"])))
    assert binding["publishable"] is False
    assert binding["v1_allow_receipt_digest"] is None
    assert binding["paths_digest"] is None
    assert binding["claim_id"] == receipt["claim_id"]


# ---------------------------------------------------------------------------
# ADR-0461 第 1 步: observe 结果写进 ledger, run 记录与 run_digest 不得变动
# ---------------------------------------------------------------------------


def _registry() -> dict[str, object]:
    return {
        "runner": {},
        "agent_profiles": {
            "governance-agent": {
                "id": "governance-agent",
                "actor": "agent-a",
                "allowed_workflows": ["project-code-change"],
            }
        },
    }


def _members() -> list[dict[str, object]]:
    return [
        {
            "claim_id": "claim-1",
            "claim_version": 1,
            "lease_epoch": 2,
            "v1_allow_receipt_digest": "sha256:" + "a" * 64,
            "paths_digest": "sha256:" + "b" * 64,
        }
    ]


def test_green_claim_binding_lands_in_ledger(monkeypatch: pytest.MonkeyPatch) -> None:
    """observe 结果此前被直接丢弃; 现在必须进 ledger。"""
    from omo.workflow import lifecycle_ledger

    captured: list[dict[str, object]] = []
    monkeypatch.setattr(lifecycle_ledger, "append_ledger_event", lambda _r, event: captured.append(event))
    monkeypatch.setattr(lifecycle, "append_ledger_event", lambda _r, e: captured.append(e))

    lifecycle._record_authority_claim_binding(_registry(), "run-1", "claim", _members())

    assert len(captured) == 1
    event = captured[0]
    assert event["event"] == "claim_binding_observed"
    assert event["run_id"] == "run-1"
    member = event["members"][0]
    assert member["claim_id"] == "claim-1"
    assert member["claim_version"] == 1
    assert member["lease_epoch"] == 2
    assert member["v1_allow_receipt_digest"] == "sha256:" + "a" * 64
    assert member["paths_digest"] == "sha256:" + "b" * 64


def test_red_non_claim_operation_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict[str, object]] = []
    monkeypatch.setattr(lifecycle, "append_ledger_event", lambda _r, e: captured.append(e))
    lifecycle._record_authority_claim_binding(_registry(), "run-1", "close", _members())
    assert captured == []


def test_red_empty_members_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict[str, object]] = []
    monkeypatch.setattr(lifecycle, "append_ledger_event", lambda _r, e: captured.append(e))
    lifecycle._record_authority_claim_binding(_registry(), "run-1", "claim", [])
    assert captured == []


def test_hard_run_digest_unchanged_by_binding_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """**硬判据**: 记录绑定后 run 记录与 `run_digest` 必须逐字节不变。

    settle 之后若改动 run 文件, authority 刚认证过的 `resulting_run_digest`
    会静默失配, 且 `:735` 之后无任何复校 —— 必然失配且不可检出。本测试是
    该不变式的守门人: 摘要一旦变化即说明写错了地方。
    """
    registry = _registry()
    workflow = {
        "id": "project-code-change",
        "title": "Project code change",
        "purpose": "test",
        "agents": {},
        "allowed_lanes": ["governance_code"],
        "lock_scopes": [],
        "phases": {},
    }
    record = lifecycle.start_run(
        registry,
        workflow,
        {"actor": "agent-a", "profile": "governance-agent", "project": "", "format": "openspec"},
        "test objective",
        False,
        False,
    )
    run_id = record["run_id"] if isinstance(record, dict) else record.run_id
    run_path, _payload = lifecycle.read_run(registry, run_id)
    before_bytes = run_path.read_bytes()
    before_digest = lifecycle._authority_file_digest(run_path)

    monkeypatch.setattr(lifecycle, "append_ledger_event", lambda *_a, **_k: None)
    lifecycle._record_authority_claim_binding(registry, run_id, "claim", _members())

    assert run_path.read_bytes() == before_bytes, "记录绑定**不得**改动 run 记录字节"
    assert lifecycle._authority_file_digest(run_path) == before_digest, "run_digest 必须不变"
