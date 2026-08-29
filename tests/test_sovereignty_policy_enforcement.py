"""W2-03 sovereignty — PDP/Ledger single-user enforcement unit tests.

Covers: allow/deny decisions; no-mandate and admission denials map to the
stable ``policy_denied`` reason; durable PolicyDecision append; idempotency
(same action+hash reuses prior state, different hash rejected); PDP and
decision/started/terminal ledger failures are fail-closed with 0 or 1
provider calls and stable reasons (pdp_unavailable, ledger_unavailable,
provider_failed, receipt_unconfirmed); the AgoraPepProvider narrow injection
port (trusted top-level request_hash, ignored _omo_policy request_hash, no
hard agora import, explicit False on terminal ledger failure).
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta, timezone

import pytest
from ecos.ssot.mof.generated.control.mof_control_models import DelegationMandate

from omo.event_ledger.broker import LedgerBroker, LedgerError
from omo.sovereignty import (
    EVT_ACTION_STARTED,
    EVT_ACTION_SUCCEEDED,
    EVT_MANDATE_GRANT,
    EVT_POLICY_DECISION,
    OUTCOME_DENIED,
    OUTCOME_SUCCEEDED,
    PDP_PRODUCER,
    REASON_ALLOWED,
    REASON_LEDGER_UNAVAILABLE,
    REASON_PDP_UNAVAILABLE,
    REASON_POLICY_DENIED,
    REASON_PROVIDER_FAILED,
    REASON_RECEIPT_UNCONFIRMED,
    STABLE_REASONS,
    ActionRequest,
    AgoraPepProvider,
    InvalidActionRequestError,
    MandateManager,
    PolicyEnforcementService,
    SovereigntyService,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path):
    return tmp_path / "enforce.db"


@pytest.fixture()
def broker(db_path):
    b = LedgerBroker.connect(db_path)
    yield b
    b.close()


@pytest.fixture()
def svc(broker):
    yield SovereigntyService(broker)


@pytest.fixture()
def mgr(broker):
    yield MandateManager(broker)


@pytest.fixture()
def pdp(broker):
    yield PolicyEnforcementService(broker)


@pytest.fixture()
def now():
    return datetime.now(UTC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _grant_mandate(svc, mgr, now, *, cap="bos://mail/draft", **overrides):
    assignment = svc.assign(
        "principal:xiamingxing",
        "role:family-steward",
        role_name="Family Steward",
        scope="family",
        responsibilities=["family-commitments"],
    )
    resp = assignment.responsibilities[0]
    kwargs = {
        "mandate_id": "mandate:enforce-001",
        "schema_version": "delegation-mandate/v1",
        "principal_id": "principal:xiamingxing",
        "executor_id": "agent:planner",
        "episode_id": "episode_enforce",
        "role_context_id": "role:family-steward",
        "role_assignment_id": assignment.assignment_id,
        "role_assignment_version": assignment.version,
        "responsibility_id": resp.resp_id,
        "responsibility_version": resp.version,
        "purpose": "Enforcement test mandate",
        "capability_scope": [cap],
        "autonomy_level": "A3",
        "risk_ceiling": "R2",
        "approval_mode": "matrix",
        "disclosure_policy": "disclosure:private",
        "valid_from": now - timedelta(hours=1),
        "expires_at": now + timedelta(days=365),
        "budget_limit": 10.0,
        "budget_unit": "call",
        "revocable": True,
        "trace_id": "enforce001234567890abcdef12",
        "mandate_version": 1,
        "status": "active",
    }
    kwargs.update(overrides)
    return mgr.grant(DelegationMandate(**kwargs))


_VALID_CREDENTIAL_XIAMINGXING = "credential:key:1:sha256:a3bba3adae0ebc76d0c42035e9f2c45172edaf945683ccdb9b4d9e40ccaf47ed"


def _authority_digest_for(principal_id: str) -> str:
    """Compute a fresh, valid principal receipt digest for the default clock."""
    from datetime import datetime

    from omo.sovereignty.principal_authority import (
        DefaultPrincipalAuthority,
        digest_receipt,
    )

    auth = DefaultPrincipalAuthority()
    receipt = auth.verify(
        principal_id,
        _VALID_CREDENTIAL_XIAMINGXING,
        now=datetime.now(UTC).isoformat(),
    )
    return digest_receipt(receipt)


def _make_request(**overrides: object) -> ActionRequest:
    kwargs: dict[str, object] = {
        "action_id": "action:draft-reply",
        "principal_id": "principal:xiamingxing",
        "executor_id": "agent:planner",
        "episode_id": "episode_enforce",
        "mandate_id": "mandate:enforce-001",
        "role_context_id": "role:family-steward",
        "responsibility_id": "responsibility:family-commitments",
        "capability": "bos://mail/draft",
        "server_risk": "R2",
        "requested_budget": 1.0,
        "budget_unit": "call",
        "disclosure_policy": "disclosure:private",
        "request_hash": "req-hash-enforce-001",
        # Principal authority binding (BET-Y1Q3-T4-04): every request carries a
        # verified authority receipt by default; tests override to probe the
        # fail-closed negative matrix.
        "principal_authority_ref": "authority:omo:v1:principal:xiamingxing",
        "principal_receipt_digest": _authority_digest_for("principal:xiamingxing"),
        "credential_ref": _VALID_CREDENTIAL_XIAMINGXING,
    }
    kwargs.update(overrides)
    return ActionRequest(**kwargs)  # type: ignore[arg-type]


def _make_request_dict(*, request_hash="req-hash-enforce-001", **overrides):
    """Full Agora-style request dict: trusted top-level hash + _omo_policy."""
    req = _make_request(**overrides)
    envelope = {
        "action_id": req.action_id,
        "principal_id": req.principal_id,
        "executor_id": req.executor_id,
        "episode_id": req.episode_id,
        "mandate_id": req.mandate_id,
        "role_context_id": req.role_context_id,
        "responsibility_id": req.responsibility_id,
        "capability": req.capability,
        "server_risk": req.server_risk,
        "requested_budget": req.requested_budget,
        "budget_unit": req.budget_unit,
        "disclosure_policy": req.disclosure_policy,
        "request_hash": "caller-controlled-hash-IGNORED",  # must be ignored
        "trace_id": req.trace_id,
        "mandate_version": req.mandate_version,
        "principal_authority_ref": req.principal_authority_ref,
        "principal_receipt_digest": req.principal_receipt_digest,
        "credential_ref": req.credential_ref,
    }
    return {
        "uri": req.capability,
        "tool_name": "mutate_resource",
        "operation": "write",
        "caller_id": req.executor_id,
        "arguments": {"_omo_policy": envelope},
        "capability_descriptor": {"effect_class": "effectful", "risk_level": "medium"},
        "request_hash": request_hash,
    }


class _FailingAppendBroker:
    """Delegates reads to a real broker; append raises from call ``n`` on."""

    def __init__(self, broker, *, fails_from: int = 1):
        self._broker = broker
        self._fails_from = fails_from
        self._calls = 0

    def read(self, *args, **kwargs):
        return self._broker.read(*args, **kwargs)

    def append(self, *args, **kwargs):
        self._calls += 1
        if self._calls >= self._fails_from:
            raise LedgerError("simulated ledger append failure")
        return self._broker.append(*args, **kwargs)


class _FailingReadBroker:
    """A broker whose reads always fail (PDP unavailable)."""

    def __init__(self, broker):
        self._broker = broker

    def read(self, *args, **kwargs):
        raise LedgerError("simulated ledger read failure")

    def append(self, *args, **kwargs):
        return self._broker.append(*args, **kwargs)


class _SecondReadFailBroker:
    """Permits history replay, then fails the later grant-timing read."""

    def __init__(self, broker):
        self._broker = broker
        self._read_calls = 0

    def read(self, *args, **kwargs):
        self._read_calls += 1
        if self._read_calls >= 2:
            raise LedgerError("simulated second ledger read failure")
        return self._broker.read(*args, **kwargs)

    def append(self, *args, **kwargs):
        return self._broker.append(*args, **kwargs)


class _CountingProvider:
    def __init__(self, *, raise_error: Exception | None = None, result=None):
        self.calls = 0
        self._raise_error = raise_error
        self._result = result or {"ok": True}

    def __call__(self, request: ActionRequest) -> dict:
        self.calls += 1
        if self._raise_error is not None:
            raise self._raise_error
        return self._result


# ---------------------------------------------------------------------------
# PDP decisions
# ---------------------------------------------------------------------------


def test_stable_reasons_vocabulary():
    assert STABLE_REASONS == (
        REASON_ALLOWED,
        REASON_POLICY_DENIED,
        REASON_PDP_UNAVAILABLE,
        REASON_LEDGER_UNAVAILABLE,
        REASON_PROVIDER_FAILED,
        REASON_RECEIPT_UNCONFIRMED,
    )


def test_decide_allow_is_durable(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    result = pdp.decide(_make_request())
    assert result.decision.decision == "allow"
    assert result.decision.reason == REASON_ALLOWED
    assert result.persisted is True
    assert result.reused is False
    # Decision is durably replayable.
    replayed = pdp.replay_decisions("action:draft-reply")
    assert len(replayed) == 1
    assert replayed[0].decision_id == result.decision.decision_id
    assert replayed[0].schema_version == "policy-decision/v1"
    assert replayed[0].request_hash == "req-hash-enforce-001"
    assert replayed[0].capability == "bos://mail/draft"
    # Exactly one Decision event (no receipts yet).
    events = pdp.replay_events()
    assert [e["event_type"] for e in events] == [EVT_POLICY_DECISION]
    assert events[0]["producer"] == PDP_PRODUCER


def test_allowed_decision_occurs_after_its_mandate_grant(broker, svc):
    """An allowed decision records its own decision time, never valid_from."""
    grant_time = datetime(2026, 8, 21, 0, 7, 33, 61360, tzinfo=UTC)
    decision_time = datetime(2026, 8, 21, 0, 7, 34, tzinfo=UTC)
    manager = MandateManager(broker, clock=lambda: grant_time.isoformat())
    _grant_mandate(svc, manager, grant_time)
    pdp = PolicyEnforcementService(broker, manager=manager, clock=lambda: decision_time.isoformat())

    result = pdp.decide(_make_request())

    rows = broker.read()
    grant = next(row for row in rows if row["event_type"] == EVT_MANDATE_GRANT)
    decision = next(row for row in rows if row["event_type"] == EVT_POLICY_DECISION)
    assert result.decision.issued_at == decision_time
    assert decision["occurred_at"] == decision_time.isoformat()
    assert datetime.fromisoformat(decision["occurred_at"]) >= datetime.fromisoformat(grant["occurred_at"])


def test_allowed_decision_clamps_a_backward_pdp_clock_to_mandate_grant(broker, svc):
    grant_time = datetime(2026, 8, 21, 0, 7, 33, tzinfo=UTC)
    manager = MandateManager(broker, clock=lambda: grant_time.isoformat())
    _grant_mandate(svc, manager, grant_time)
    pdp = PolicyEnforcementService(
        broker,
        manager=manager,
        clock=lambda: (grant_time - timedelta(seconds=1)).isoformat(),
    )

    result = pdp.decide(_make_request())

    decision = next(row for row in broker.read() if row["event_type"] == EVT_POLICY_DECISION)
    assert result.decision.decision == "allow"
    assert result.decision.issued_at == grant_time
    assert decision["occurred_at"] == grant_time.isoformat()


def test_decision_at_mandate_expiry_denies_before_provider(broker, svc):
    grant_time = datetime(2026, 8, 21, 0, 7, 33, tzinfo=UTC)
    manager = MandateManager(broker, clock=lambda: grant_time.isoformat())
    expires_at = grant_time + timedelta(seconds=1)
    _grant_mandate(svc, manager, grant_time, expires_at=expires_at)
    pdp = PolicyEnforcementService(
        broker,
        manager=manager,
        clock=lambda: (expires_at + timedelta(seconds=1)).isoformat(),
    )
    provider = _CountingProvider()

    outcome = pdp.execute(_make_request(), provider)

    decision = pdp.decision("action:draft-reply")
    assert outcome.status == OUTCOME_DENIED
    assert outcome.provider_calls == 0
    assert provider.calls == 0
    assert decision is not None
    assert decision.reason == REASON_POLICY_DENIED
    assert decision.issued_at == expires_at + timedelta(seconds=1)
    assert decision.expires_at == decision.issued_at


def test_receipts_clamp_backward_clock_to_their_causal_predecessors(broker, svc):
    grant_time = datetime(2026, 8, 21, 0, 7, 33, tzinfo=UTC)
    decision_time = grant_time + timedelta(seconds=2)
    clock_values = iter(
        (
            decision_time,
            decision_time - timedelta(seconds=1),
            decision_time - timedelta(seconds=2),
        )
    )
    manager = MandateManager(broker, clock=lambda: grant_time.isoformat())
    _grant_mandate(svc, manager, grant_time)
    pdp = PolicyEnforcementService(broker, manager=manager, clock=lambda: next(clock_values).isoformat())

    outcome = pdp.execute(_make_request(), _CountingProvider())

    events = pdp.replay_events()
    receipts = pdp.replay_receipts("action:draft-reply")
    assert outcome.status == OUTCOME_SUCCEEDED
    assert events[0]["occurred_at"] == decision_time.isoformat()
    assert events[1]["occurred_at"] == decision_time.isoformat()
    assert events[2]["occurred_at"] == decision_time.isoformat()
    assert receipts[0].started_at == decision_time
    assert receipts[1].started_at == decision_time
    assert receipts[1].completed_at == decision_time


def test_idempotent_decision_replay_freezes_original_time_after_clock_regresses(broker, svc):
    grant_time = datetime(2026, 8, 21, 0, 7, 33, tzinfo=UTC)
    current_time = grant_time + timedelta(seconds=1)
    manager = MandateManager(broker, clock=lambda: grant_time.isoformat())
    _grant_mandate(svc, manager, grant_time)
    pdp = PolicyEnforcementService(broker, manager=manager, clock=lambda: current_time.isoformat())

    first = pdp.decide(_make_request())
    current_time = grant_time - timedelta(days=1)
    replay = pdp.decide(_make_request())

    assert replay.reused is True
    assert replay.decision.issued_at == first.decision.issued_at
    assert len(pdp.replay_events()) == 1


def test_hash_mismatch_decision_clamps_a_backward_clock_to_mandate_grant(broker, svc):
    grant_time = datetime(2026, 8, 21, 0, 7, 33, tzinfo=UTC)
    current_time = grant_time + timedelta(seconds=1)
    manager = MandateManager(broker, clock=lambda: grant_time.isoformat())
    _grant_mandate(svc, manager, grant_time)
    pdp = PolicyEnforcementService(broker, manager=manager, clock=lambda: current_time.isoformat())

    pdp.decide(_make_request())
    current_time = grant_time - timedelta(days=1)
    mismatch = pdp.decide(_make_request(request_hash="req-hash-different-002"))

    assert mismatch.decision.decision == "deny"
    assert mismatch.decision.issued_at == grant_time
    assert pdp.replay_events()[-1]["occurred_at"] == grant_time.isoformat()


def test_decide_no_mandate_is_policy_denied_and_durable(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    result = pdp.decide(_make_request(mandate_id="mandate:missing"))
    assert result.decision.decision == "deny"
    assert result.decision.reason == REASON_POLICY_DENIED
    assert result.persisted is True
    assert pdp.decision("action:draft-reply").decision == "deny"


def test_admission_denial_maps_to_policy_denied(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    result = pdp.decide(_make_request(requested_budget=999.0))
    assert result.decision.decision == "deny"
    # Fine-grained mandate reason goes to description; the stable vocabulary
    # only ever says policy_denied.
    assert result.decision.reason == REASON_POLICY_DENIED
    assert "budget" in (result.decision.description or "")


def test_idempotent_reuse_same_action_same_hash(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    first = pdp.decide(_make_request())
    count_after_first = len(pdp.replay_events())
    second = pdp.decide(_make_request())
    assert second.decision.decision_id == first.decision.decision_id
    assert second.reused is True
    assert second.persisted is False
    assert len(pdp.replay_events()) == count_after_first


def test_request_mismatch_same_action_different_hash_is_denied(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    first = pdp.decide(_make_request())
    second = pdp.decide(_make_request(request_hash="req-hash-different-002"))
    assert first.decision.decision == "allow"
    assert second.decision.decision == "deny"
    assert second.decision.reason == REASON_POLICY_DENIED
    assert "request_hash_mismatch" in (second.decision.description or "")
    # The original hash stays reusable (idempotent), the conflict is durable.
    assert pdp.decide(_make_request()).decision.decision_id == first.decision.decision_id
    assert pdp.decide(_make_request(request_hash="req-hash-different-002")).reused is True


def test_pdp_failure_denies_with_zero_calls(broker):
    failing = _FailingReadBroker(broker)
    pdp = PolicyEnforcementService(failing)  # type: ignore[arg-type]
    result = pdp.decide(_make_request())
    assert result.decision.decision == "deny"
    assert result.decision.reason == REASON_PDP_UNAVAILABLE
    assert result.persisted is False
    outcome = pdp.execute(_make_request(), _CountingProvider())
    assert outcome.status == "denied"
    assert outcome.provider_calls == 0


def test_second_grant_timing_read_failure_denies_without_persisting_or_calling_provider(broker, svc, now):
    manager = MandateManager(broker)
    _grant_mandate(svc, manager, now)
    pdp = PolicyEnforcementService(_SecondReadFailBroker(broker), manager=manager)
    provider = _CountingProvider()

    outcome = pdp.execute(_make_request(), provider)

    assert outcome.status == OUTCOME_DENIED
    assert outcome.reason == REASON_PDP_UNAVAILABLE
    assert outcome.provider_calls == 0
    assert provider.calls == 0
    assert list(broker.read(producer=PDP_PRODUCER)) == []


def test_decision_append_failure_denies_with_zero_calls(broker, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    failing = _FailingAppendBroker(broker, fails_from=1)
    pdp = PolicyEnforcementService(failing)  # type: ignore[arg-type]
    outcome = pdp.execute(_make_request(), _CountingProvider())
    assert outcome.status == "denied"
    assert outcome.reason == REASON_LEDGER_UNAVAILABLE
    assert outcome.provider_calls == 0
    assert outcome.started_receipt_id is None


# ---------------------------------------------------------------------------
# Execution flow
# ---------------------------------------------------------------------------


def test_execute_success_order_and_replay(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    provider = _CountingProvider()
    outcome = pdp.execute(_make_request(), provider)
    assert outcome.status == "succeeded"
    assert outcome.reason == REASON_ALLOWED
    assert outcome.provider_calls == 1
    assert provider.calls == 1
    # Ordered replay: decision -> started -> succeeded.
    events = pdp.replay_events()
    assert [e["event_type"] for e in events] == [
        EVT_POLICY_DECISION,
        EVT_ACTION_STARTED,
        EVT_ACTION_SUCCEEDED,
    ]
    receipts = pdp.replay_receipts("action:draft-reply")
    assert [r.status for r in receipts] == ["started", "succeeded"]
    assert receipts[0].decision_id == outcome.decision_id
    assert receipts[1].decision_id == outcome.decision_id
    assert receipts[1].completed_at is not None
    assert receipts[1].result == {"ok": True}


def test_execute_provider_failure_writes_failed_receipt(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    provider = _CountingProvider(raise_error=RuntimeError("provider boom"))
    outcome = pdp.execute(_make_request(), provider)
    assert outcome.status == "failed"
    assert outcome.reason == REASON_PROVIDER_FAILED
    assert outcome.provider_calls == 1
    receipts = pdp.replay_receipts("action:draft-reply")
    assert [r.status for r in receipts] == ["started", "failed"]
    assert receipts[1].reason == REASON_PROVIDER_FAILED
    assert receipts[1].completed_at is not None


def test_execute_started_append_failure_never_calls_provider(broker, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    failing = _FailingAppendBroker(broker, fails_from=2)
    pdp = PolicyEnforcementService(failing)  # type: ignore[arg-type]
    provider = _CountingProvider()
    outcome = pdp.execute(_make_request(), provider)
    assert outcome.status == "failed"
    assert outcome.reason == REASON_LEDGER_UNAVAILABLE
    assert outcome.provider_calls == 0
    assert provider.calls == 0
    assert outcome.started_receipt_id is None


def test_execute_terminal_append_failure_never_returns_succeeded(broker, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    failing = _FailingAppendBroker(broker, fails_from=3)
    pdp = PolicyEnforcementService(failing)  # type: ignore[arg-type]
    provider = _CountingProvider()
    outcome = pdp.execute(_make_request(), provider)
    assert outcome.status == "unconfirmed"
    assert outcome.reason == REASON_RECEIPT_UNCONFIRMED
    assert outcome.provider_calls == 1
    assert outcome.terminal_receipt_id is None
    # Started receipt is visible in replay; no terminal exists.
    receipts = pdp.replay_receipts("action:draft-reply")
    assert [r.status for r in receipts] == ["started"]
    assert outcome.started_receipt_id == receipts[0].receipt_id


def test_idempotent_retry_returns_prior_state_without_recall(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    provider = _CountingProvider()
    first = pdp.execute(_make_request(), provider)
    assert first.status == "succeeded"
    second = pdp.execute(_make_request(), provider)
    assert second.status == "succeeded"
    assert second.decision_id == first.decision_id
    assert second.provider_calls == 0
    assert provider.calls == 1  # provider never re-invoked
    # Retry after failure returns the prior failed state without recall.
    pdp2 = PolicyEnforcementService(pdp.broker)
    provider2 = _CountingProvider()
    third = pdp2.execute(_make_request(), provider2)
    assert third.status == "succeeded"
    assert third.provider_calls == 0
    assert provider2.calls == 0


# ---------------------------------------------------------------------------
# AgoraPepProvider — narrow injection port
# ---------------------------------------------------------------------------


def test_provider_module_has_no_hard_agora_import():
    # The core OMO module must not hard-import Agora (no dependency cycle).
    assert "agora" not in sys.modules
    import omo.sovereignty.enforcement as en

    assert "agora" not in sys.modules
    assert hasattr(en.AgoraPepProvider, "evaluate")
    assert hasattr(en.AgoraPepProvider, "start_receipt")
    assert hasattr(en.AgoraPepProvider, "confirm_receipt")


def test_adapter_evaluate_uses_trusted_top_level_hash(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    adapter = AgoraPepProvider(service=pdp)
    request_dict = _make_request_dict(request_hash="trusted-hash-0001")
    decision = adapter.evaluate(request_dict)
    assert decision.decision == "allow"
    # The trusted top-level hash wins; the caller-controlled _omo_policy hash
    # ("caller-controlled-hash-IGNORED") is ignored.
    assert decision.request_hash == "trusted-hash-0001"


def test_adapter_missing_trusted_hash_raises(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    adapter = AgoraPepProvider(service=pdp)
    request_dict = _make_request_dict()
    request_dict.pop("request_hash")
    with pytest.raises(InvalidActionRequestError):
        adapter.evaluate(request_dict)
    request_dict["request_hash"] = "short"
    with pytest.raises(InvalidActionRequestError):
        adapter.evaluate(request_dict)


def test_adapter_missing_omo_policy_envelope_raises(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    adapter = AgoraPepProvider(service=pdp)
    request_dict = _make_request_dict()
    request_dict["arguments"] = {}
    with pytest.raises(InvalidActionRequestError):
        adapter.evaluate(request_dict)


def test_adapter_full_flow_persists_decision_started_terminal(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    adapter = AgoraPepProvider(service=pdp)
    decision = adapter.evaluate(_make_request_dict())
    assert decision.decision == "allow"
    started = adapter.start_receipt(decision)
    assert started.status == "started"
    ok = adapter.confirm_receipt(started, "succeeded", result={"ok": True})
    assert ok is True
    events = pdp.replay_events()
    assert [e["event_type"] for e in events] == [
        EVT_POLICY_DECISION,
        EVT_ACTION_STARTED,
        EVT_ACTION_SUCCEEDED,
    ]
    receipts = pdp.replay_receipts("action:draft-reply")
    assert [r.status for r in receipts] == ["started", "succeeded"]


def test_adapter_confirm_terminal_ledger_failure_returns_false(broker, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    failing = _FailingAppendBroker(broker, fails_from=3)
    adapter = AgoraPepProvider(service=PolicyEnforcementService(failing))  # type: ignore[arg-type]
    decision = adapter.evaluate(_make_request_dict())
    started = adapter.start_receipt(decision)
    assert adapter.confirm_receipt(started, "succeeded", result={"ok": True}) is False


def test_adapter_deny_path(pdp, svc, mgr, now):
    _grant_mandate(svc, mgr, now)
    adapter = AgoraPepProvider(service=pdp)
    request_dict = _make_request_dict(mandate_id="mandate:missing")
    decision = adapter.evaluate(request_dict)
    assert decision.decision == "deny"
    assert decision.reason == REASON_POLICY_DENIED


# ---------------------------------------------------------------------------
# BET-Y1Q3-T4-04 Principal authority binding — fail-closed rejection matrix
# ---------------------------------------------------------------------------


def _grant_for_principal(svc, mgr, now, principal_id, mandate_id="mandate:enforce-001"):
    assignment = svc.assign(
        principal_id,
        "role:family-steward",
        role_name="Family Steward",
        scope="family",
        responsibilities=["family-commitments"],
    )
    resp = assignment.responsibilities[0]
    from omo.sovereignty.mandates import DelegationMandate
    kwargs = {
        "mandate_id": mandate_id,
        "schema_version": "delegation-mandate/v1",
        "principal_id": principal_id,
        "executor_id": "agent:planner",
        "episode_id": "episode_enforce",
        "role_context_id": "role:family-steward",
        "role_assignment_id": assignment.assignment_id,
        "role_assignment_version": assignment.version,
        "responsibility_id": resp.resp_id,
        "responsibility_version": resp.version,
        "purpose": "Enforcement test mandate",
        "capability_scope": ["bos://mail/draft"],
        "autonomy_level": "A3",
        "risk_ceiling": "R2",
        "approval_mode": "matrix",
        "disclosure_policy": "disclosure:private",
        "valid_from": now - timedelta(hours=1),
        "expires_at": now + timedelta(days=365),
        "budget_limit": 10.0,
        "budget_unit": "call",
        "revocable": True,
        "trace_id": "enforce001234567890abcdef12",
        "mandate_version": 1,
        "status": "active",
    }
    return mgr.grant(DelegationMandate(**kwargs))


def _assert_denied_zero_effect(outcome, decision, *, reason):
    assert outcome.status == OUTCOME_DENIED
    assert outcome.provider_calls == 0
    assert decision is not None
    assert decision.decision == "deny"
    assert decision.reason == reason


def test_authority_missing_receipt_denied(broker, svc, mgr, now):
    _grant_for_principal(svc, mgr, now, "principal:xiamingxing")
    pdp = PolicyEnforcementService(broker, manager=mgr)
    provider = _CountingProvider()
    req = _make_request(principal_authority_ref=None, principal_receipt_digest=None, credential_ref=None)
    outcome = pdp.execute(req, provider)
    decision = pdp.decision("action:draft-reply")
    from omo.sovereignty.principal_authority import REASON_AUTHORITY_REQUIRED
    _assert_denied_zero_effect(outcome, decision, reason=REASON_AUTHORITY_REQUIRED)


def test_authority_principal_mismatch_denied(broker, svc, mgr, now):
    _grant_for_principal(svc, mgr, now, "principal:xiamingxing")
    from omo.sovereignty.principal_authority import (
        REASON_AUTHORITY_PRINCIPAL_MISMATCH,
        PrincipalAuthorityReceipt,
        digest_receipt,
    )
    # Defensive branch: a buggy authority returns a receipt for a DIFFERENT
    # principal than the request -> must deny before any mandate/admission work.
    class _WrongPrincipalAuthority:
        def verify(self, principal_id, credential_ref, *, now):
            return PrincipalAuthorityReceipt(
                principal_id="principal:someone-else",
                authority_ref="authority:omo:v1:principal:someone-else",
                credential_digest="sha256:" + "a" * 64,
                membership_version=1,
                verified_at=now,
                expires_at="2999-01-01T00:00:00+00:00",
            )

    wrong = _WrongPrincipalAuthority()
    pdp = PolicyEnforcementService(broker, manager=mgr, authority=wrong)
    provider = _CountingProvider()
    req = _make_request(principal_receipt_digest=digest_receipt(wrong.verify("principal:xiamingxing", "", now=now.isoformat())))
    outcome = pdp.execute(req, provider)
    decision = pdp.decision("action:draft-reply")
    _assert_denied_zero_effect(outcome, decision, reason=REASON_AUTHORITY_PRINCIPAL_MISMATCH)


def test_authority_digest_unverified_denied(broker, svc, mgr, now):
    _grant_for_principal(svc, mgr, now, "principal:xiamingxing")
    pdp = PolicyEnforcementService(broker, manager=mgr)
    provider = _CountingProvider()
    forged_digest = "sha256:" + "a" * 64
    req = _make_request(principal_receipt_digest=forged_digest)
    outcome = pdp.execute(req, provider)
    decision = pdp.decision("action:draft-reply")
    from omo.sovereignty.principal_authority import REASON_AUTHORITY_DIGEST_UNVERIFIED
    _assert_denied_zero_effect(outcome, decision, reason=REASON_AUTHORITY_DIGEST_UNVERIFIED)


def test_authority_expired_denied(broker, svc, mgr, now):
    _grant_for_principal(svc, mgr, now, "principal:xiamingxing")
    from omo.sovereignty.principal_authority import (
        REASON_AUTHORITY_EXPIRED,
        PrincipalAuthorityReceipt,
        digest_receipt,
    )
    # Defensive branch: authority returns an already-expired receipt -> deny.
    class _ExpiredAuthority:
        def verify(self, principal_id, credential_ref, *, now):
            return PrincipalAuthorityReceipt(
                principal_id=principal_id,
                authority_ref=f"authority:omo:v1:{principal_id}",
                credential_digest="sha256:" + "a" * 64,
                membership_version=1,
                verified_at="2000-01-01T00:00:00+00:00",
                expires_at="2000-01-02T00:00:00+00:00",
            )

    expired = _ExpiredAuthority()
    pdp = PolicyEnforcementService(broker, manager=mgr, authority=expired)
    provider = _CountingProvider()
    req = _make_request(
        principal_receipt_digest=digest_receipt(expired.verify("principal:xiamingxing", "", now=now.isoformat()))
    )
    outcome = pdp.execute(req, provider)
    decision = pdp.decision("action:draft-reply")
    _assert_denied_zero_effect(outcome, decision, reason=REASON_AUTHORITY_EXPIRED)


def test_authority_unknown_denied(broker, svc, mgr, now):
    _grant_for_principal(svc, mgr, now, "principal:xiamingxing")
    pdp = PolicyEnforcementService(broker, manager=mgr)
    provider = _CountingProvider()
    # unknown principal (not in authority members) with a fabricated ref
    req = _make_request(
        principal_id="principal:nobody",
        principal_authority_ref="authority:omo:v1:principal:nobody",
        principal_receipt_digest="sha256:" + "b" * 64,
        credential_ref="credential:key:1:sha256:" + "c" * 64,
    )
    outcome = pdp.execute(req, provider)
    decision = pdp.decision("action:draft-reply")
    from omo.sovereignty.principal_authority import REASON_AUTHORITY_UNKNOWN
    _assert_denied_zero_effect(outcome, decision, reason=REASON_AUTHORITY_UNKNOWN)


def test_authority_credential_mismatch_denied(broker, svc, mgr, now):
    _grant_for_principal(svc, mgr, now, "principal:xiamingxing")
    pdp = PolicyEnforcementService(broker, manager=mgr)
    provider = _CountingProvider()
    wrong_cred = "credential:key:1:sha256:" + "d" * 64
    req = _make_request(credential_ref=wrong_cred)
    outcome = pdp.execute(req, provider)
    decision = pdp.decision("action:draft-reply")
    from omo.sovereignty.principal_authority import REASON_AUTHORITY_CREDENTIAL_MISMATCH
    _assert_denied_zero_effect(outcome, decision, reason=REASON_AUTHORITY_CREDENTIAL_MISMATCH)


def test_authority_version_rollback_denied(broker, svc, mgr, now):
    _grant_for_principal(svc, mgr, now, "principal:xiamingxing")
    pdp = PolicyEnforcementService(broker, manager=mgr)
    provider = _CountingProvider()
    rollback_cred = "credential:key:0:sha256:a3bba3adae0ebc76d0c42035e9f2c45172edaf945683ccdb9b4d9e40ccaf47ed"
    req = _make_request(credential_ref=rollback_cred)
    outcome = pdp.execute(req, provider)
    decision = pdp.decision("action:draft-reply")
    from omo.sovereignty.principal_authority import REASON_AUTHORITY_VERSION_ROLLBACK
    _assert_denied_zero_effect(outcome, decision, reason=REASON_AUTHORITY_VERSION_ROLLBACK)


def test_authority_fixture_only_production_denied(broker, svc, mgr, now):
    _grant_for_principal(svc, mgr, now, "principal:alice")
    from omo.sovereignty.principal_authority import DefaultPrincipalAuthority
    pdp = PolicyEnforcementService(
        broker,
        manager=mgr,
        authority=DefaultPrincipalAuthority(production=True),
    )
    provider = _CountingProvider()
    # principal:alice (fixture-only) must be rejected on the production path
    from omo.sovereignty.principal_authority import REASON_AUTHORITY_UNKNOWN
    req = _make_request(
        principal_id="principal:alice",
        principal_authority_ref="authority:omo:v1:principal:alice",
        principal_receipt_digest="sha256:" + "e" * 64,
        credential_ref="credential:key:1:sha256:" + "f" * 64,
    )
    outcome = pdp.execute(req, provider)
    decision = pdp.decision("action:draft-reply")
    _assert_denied_zero_effect(outcome, decision, reason=REASON_AUTHORITY_UNKNOWN)
