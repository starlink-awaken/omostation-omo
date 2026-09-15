"""Tests for ExternalTransactionService (A8 7-stage lifecycle)."""
from __future__ import annotations
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
import pytest
from omo.event_ledger.broker import LedgerBroker, LedgerError
from omo.workflow.external_transaction import (
    AdapterReceipt, ExternalAdapter, ExternalTransactionService,
    TransactionError, TransactionRequest, TransactionStateError,
    TransactionValidationError,
)

@pytest.fixture
def broker() -> LedgerBroker:
    with tempfile.TemporaryDirectory() as tmp:
        b = LedgerBroker.connect(Path(tmp) / "ledger.db")
        yield b
        b.close()

@pytest.fixture
def role_registry() -> MagicMock:
    m = MagicMock()
    m.verify_role.return_value = MagicMock(allowed=True)
    m.get.return_value = MagicMock(role_id="role:test",
        capabilities=frozenset({"bos://test/capability"}),
        admission_state="admitted", version=1, digest="sha256:" + "a" * 64)
    return m

@pytest.fixture
def capsule_store() -> MagicMock:
    m = MagicMock()
    m.get.return_value = MagicMock(capsule_id="capsule:test-123", digest="sha256:" + "e" * 64)
    return m

@pytest.fixture
def service(broker: LedgerBroker, role_registry: MagicMock, capsule_store: MagicMock) -> ExternalTransactionService:
    return ExternalTransactionService(broker, role_registry=role_registry, capsule_store=capsule_store)

@pytest.fixture
def valid_request() -> TransactionRequest:
    return TransactionRequest(
        adapter_id="orca", role_id="role:test", capability="bos://test/capability",
        operation="worker.start", request_digest="a" * 64, principal_id="principal:test",
        lease_deadline_seconds=300, capsule_id="capsule:test-123", verifier_role_id="role:verifier",
    )

@pytest.fixture
def fake_adapter() -> ExternalAdapter:
    m = MagicMock(spec=ExternalAdapter)
    m.invoke.return_value = {"state": "succeeded", "result_digest": "b" * 64,
        "provenance_ref": "fake://provenance", "policy_digest": "sha256:" + "c" * 64}
    return m

class TestReserveIdempotency:
    def test_reserve_twice_returns_same(self, service, valid_request):
        t1 = service.reserve(valid_request)
        t2 = service.reserve(valid_request)
        assert t1.transaction_id == t2.transaction_id
        assert t1.state == "reserved"
    def test_reserve_different_digest_rejected(self, service, valid_request):
        service.reserve(valid_request)
        c = TransactionRequest(adapter_id="orca", role_id="role:test", capability="bos://test/capability",
            operation="worker.start", request_digest="b" * 64, principal_id="principal:test", lease_deadline_seconds=300)
        with pytest.raises(TransactionError) as e:
            service.reserve(c)
        assert e.value.code == "conflicting-reservation"

class TestRoleAdmissionGate:
    def test_bind_without_admission_fails(self, service, valid_request):
        t = service.reserve(valid_request)
        service._role_registry.verify_role.return_value = MagicMock(allowed=False)
        with pytest.raises(TransactionError) as e:
            service.bind(t.transaction_id, "capsule:test-123", "role:verifier")
        assert e.value.code == "role-not-admitted"

class TestCapsuleDigestReadback:
    def test_readback_capsule_digest_mismatch(self, service, valid_request):
        t = service.reserve(valid_request)
        t = service.bind(t.transaction_id, "capsule:test-123", "role:verifier")
        service._capsule_store.get.return_value.digest = "sha256:" + "f" * 64
        with pytest.raises(TransactionError) as e:
            service.readback_verify(t.transaction_id)
        assert e.value.code == "readback-failed"
        f = service.get(t.transaction_id)
        assert f is not None and f.state == "fenced"

class TestStartLedgerFailure:
    def test_start_ledger_failure_zero_adapter_calls(self, service, valid_request):
        t = service.reserve(valid_request)
        t = service.bind(t.transaction_id, "capsule:test-123", "role:verifier")
        t = service.readback_verify(t.transaction_id)
        orig = service.broker.append
        calls = []
        class FA(ExternalAdapter):
            def invoke(self, request: dict[str, Any]) -> dict[str, Any]:
                calls.append(request)
                return {"state": "succeeded"}
        def fail(**kw: Any) -> int:
            if kw.get("event_type") == "ExternalTransaction.Started.v1":
                raise LedgerError("simulated")
            return orig(**kw)
        service.broker.append = fail  # type: ignore[assignment]
        with pytest.raises(LedgerError):
            service.start(t.transaction_id, {})
        assert len(calls) == 0

class TestAdapterIdentityMismatch:
    def test_transaction_id_mismatch(self, service, valid_request):
        t = service.reserve(valid_request)
        t = service.bind(t.transaction_id, "capsule:test-123", "role:verifier")
        t = service.readback_verify(t.transaction_id)
        t = service.start(t.transaction_id, {})
        r = AdapterReceipt(transaction_id="ext-tx:wrong", operation="worker.start", result_state="succeeded", result_digest="b" * 64)
        with pytest.raises(TransactionError) as e:
            service.observe(t.transaction_id, r)
        assert e.value.code == "identity-mismatch"
        f = service.get(t.transaction_id)
        assert f is not None and f.state == "fenced"

class TestAdapterUnknownOutcome:
    def test_unknown_outcome_fences(self, service, valid_request):
        t = service.reserve(valid_request)
        t = service.bind(t.transaction_id, "capsule:test-123", "role:verifier")
        t = service.readback_verify(t.transaction_id)
        t = service.start(t.transaction_id, {})
        r = AdapterReceipt(transaction_id=t.transaction_id, operation="worker.start", result_state="unknown")
        with pytest.raises(TransactionError) as e:
            service.observe(t.transaction_id, r)
        assert e.value.code == "fenced"
        assert service.get(t.transaction_id).state == "fenced"
    def test_failed_outcome_fences(self, service, valid_request):
        t = service.reserve(valid_request)
        t = service.bind(t.transaction_id, "capsule:test-123", "role:verifier")
        t = service.readback_verify(t.transaction_id)
        t = service.start(t.transaction_id, {})
        r = AdapterReceipt(transaction_id=t.transaction_id, operation="worker.start", result_state="failed", error_code="E_TIMEOUT")
        with pytest.raises(TransactionError):
            service.observe(t.transaction_id, r)
        assert service.get(t.transaction_id).state == "fenced"

class TestHappyPath:
    def test_happy_path_full_lifecycle(self, service, valid_request, fake_adapter):
        t = service.execute_happy_path(valid_request, fake_adapter, "capsule:test-123", "role:verifier")
        assert t.state == "retired"
        assert t.receipt["result_state"] == "succeeded"
        assert fake_adapter.invoke.call_count == 1
        states = [e.state for e in service._re(t.transaction_id)]
        for s in ("reserved","bound","readback_verified","started","observed","verified","released","retired"):
            assert s in states

class TestDuplicateReplayAfterRetirement:
    def test_replay_after_retirement_no_second_call(self, service, valid_request, fake_adapter):
        t = service.execute_happy_path(valid_request, fake_adapter, "capsule:test-123", "role:verifier")
        assert t.state == "retired" and fake_adapter.invoke.call_count == 1
        t2 = service.reserve(valid_request)
        assert t2.transaction_id == t.transaction_id and t2.state == "retired"

class TestOperatorResolution:
    def test_operator_resolve_fenced(self, service, valid_request):
        t = service.reserve(valid_request)
        t = service.bind(t.transaction_id, "capsule:test-123", "role:verifier")
        t = service.readback_verify(t.transaction_id)
        t = service.start(t.transaction_id, {})
        r = AdapterReceipt(transaction_id=t.transaction_id, operation="worker.start", result_state="unknown")
        with pytest.raises(TransactionError):
            service.observe(t.transaction_id, r)
        res = service.operator_resolve(t.transaction_id, "manual-retry")
        assert res.state == "operator_resolved" and res.operator_resolution == "manual-retry"

class TestStateTransitions:
    def test_cannot_observe_before_start(self, service, valid_request):
        t = service.reserve(valid_request)
        r = AdapterReceipt(transaction_id=t.transaction_id, operation="worker.start", result_state="succeeded")
        with pytest.raises(TransactionStateError):
            service.observe(t.transaction_id, r)
    def test_cannot_verify_before_observe(self, service, valid_request):
        t = service.reserve(valid_request)
        t = service.bind(t.transaction_id, "capsule:test-123", "role:verifier")
        t = service.readback_verify(t.transaction_id)
        t = service.start(t.transaction_id, {})
        with pytest.raises(TransactionStateError):
            service.verify(t.transaction_id)
    def test_cannot_retire_before_release(self, service, valid_request):
        t = service.reserve(valid_request)
        t = service.bind(t.transaction_id, "capsule:test-123", "role:verifier")
        t = service.readback_verify(t.transaction_id)
        t = service.start(t.transaction_id, {})
        r = AdapterReceipt(transaction_id=t.transaction_id, operation="worker.start", result_state="succeeded", result_digest="b" * 64)
        t = service.observe(t.transaction_id, r)
        t = service.verify(t.transaction_id)
        with pytest.raises(TransactionStateError):
            service.retire(t.transaction_id)

class TestTransactionRequestValidation:
    def test_missing_adapter_id(self):
        r = TransactionRequest(adapter_id="", role_id="role:test", capability="bos://test/capability",
            operation="worker.start", request_digest="a" * 64, principal_id="principal:test")
        with pytest.raises(TransactionValidationError):
            r.validate()
    def test_invalid_role_id(self):
        r = TransactionRequest(adapter_id="orca", role_id="invalid", capability="bos://test/capability",
            operation="worker.start", request_digest="a" * 64, principal_id="principal:test")
        with pytest.raises(TransactionValidationError):
            r.validate()
    def test_invalid_request_digest(self):
        r = TransactionRequest(adapter_id="orca", role_id="role:test", capability="bos://test/capability",
            operation="worker.start", request_digest="invalid", principal_id="principal:test")
        with pytest.raises(TransactionValidationError):
            r.validate()
    def test_missing_principal_id(self):
        r = TransactionRequest(adapter_id="orca", role_id="role:test", capability="bos://test/capability",
            operation="worker.start", request_digest="a" * 64, principal_id="")
        with pytest.raises(TransactionValidationError):
            r.validate()

class TestTerminalStates:
    def test_retired_transaction_cannot_be_modified(self, service, valid_request):
        t = service.reserve(valid_request)
        t = service.bind(t.transaction_id, "capsule:test-123", "role:verifier")
        t = service.readback_verify(t.transaction_id)
        t = service.start(t.transaction_id, {})
        r = AdapterReceipt(transaction_id=t.transaction_id, operation="worker.start", result_state="succeeded", result_digest="b" * 64)
        t = service.observe(t.transaction_id, r)
        t = service.verify(t.transaction_id)
        t = service.release(t.transaction_id)
        t = service.retire(t.transaction_id)
        assert t.state == "retired"
        with pytest.raises(TransactionStateError):
            service.observe(t.transaction_id, r)

class TestQuery:
    def test_get_not_found(self, service):
        assert service.get("ext-tx:nonexistent") is None
    def test_list_empty(self, service):
        assert service.list() == []
    def test_list_with_state_filter(self, service, valid_request):
        service.reserve(valid_request)
        assert len(service.list()) == 1
        assert len(service.list(state="reserved")) == 1
        assert len(service.list(state="retired")) == 0
