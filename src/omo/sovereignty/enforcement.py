"""W2-03 Sovereignty — PDP / Ledger single-user enforcement (BET-Y1Q2-T1-06).

Model-first: consumes the ECOS M2 generated ``PolicyDecision`` /
``ActionReceipt`` Pydantic contracts — never local mirror models.  OMO owns
the PDP and the durable decision/receipt causality on :class:`LedgerBroker`;
Agora owns the PEP adjacent to providers and reaches OMO only through the
narrow injection port ``AgoraPepProvider`` (env
``AGORA_PEP_PROVIDER=omo.sovereignty.enforcement:AgoraPepProvider``), so
there is no Agora→OMO hard Python import.

Execution order (fail-closed)::

    validate + PDP -> durable PolicyDecision -> durable ActionReceipt(started)
    -> provider at most once -> durable terminal ActionReceipt(succeeded|failed)

Failure rules:

- PDP unavailable / decision append / started append failure => 0 provider calls;
- provider exception => failed receipt (reason=provider_failed);
- terminal append failure => never returns succeeded; returns
  receipt_unconfirmed with the started receipt visible in replay.

Idempotency (ledger-backed): same ``action_id`` + same ``request_hash``
returns the previous state without re-calling the provider; same
``action_id`` with a different ``request_hash`` is rejected (deny).

Stable reasons: allowed, policy_denied, pdp_unavailable, ledger_unavailable,
provider_failed, receipt_unconfirmed — no broad exception maps to allow.

Out of scope: W2-04 projections, real side effects, ledger DDL/triggers,
cryptography, distributed/concurrent exactly-once, crash reconciliation.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from ecos.ssot.mof.generated.control.mof_control_models import (
    ActionReceipt,
    PolicyDecision,
)
from pydantic import ValidationError as PydanticValidationError

from omo.event_ledger.broker import DuplicateEventError, LedgerBroker, LedgerError
from omo.sovereignty.mandates import EVT_MANDATE_GRANT, MANDATE_PRODUCER, MandateManager
from omo.sovereignty.persistent_queue import PersistentQueue

# ---------------------------------------------------------------------------
# Identity / event constants
# ---------------------------------------------------------------------------

PDP_PRODUCER = "omo-pdp"
PDP_SPACE_ID = "sovereignty"

EVT_POLICY_DECISION = "Decision.Policy.v1"
EVT_ACTION_STARTED = "Action.Started.v1"
EVT_ACTION_SUCCEEDED = "Action.Succeeded.v1"
EVT_ACTION_FAILED = "Action.Failed.v1"

_RECEIPT_EVENT_TYPES = frozenset({EVT_ACTION_STARTED, EVT_ACTION_SUCCEEDED, EVT_ACTION_FAILED})

# ---------------------------------------------------------------------------
# Frozen stable reason vocabulary (BET-Y1Q2-T1-06 done_when)
# ---------------------------------------------------------------------------

REASON_ALLOWED = "allowed"
REASON_POLICY_DENIED = "policy_denied"
REASON_PDP_UNAVAILABLE = "pdp_unavailable"
REASON_LEDGER_UNAVAILABLE = "ledger_unavailable"
REASON_PROVIDER_FAILED = "provider_failed"
REASON_RECEIPT_UNCONFIRMED = "receipt_unconfirmed"

STABLE_REASONS: tuple[str, ...] = (
    REASON_ALLOWED,
    REASON_POLICY_DENIED,
    REASON_PDP_UNAVAILABLE,
    REASON_LEDGER_UNAVAILABLE,
    REASON_PROVIDER_FAILED,
    REASON_RECEIPT_UNCONFIRMED,
)
_FAILURE_REASONS = frozenset(
    {
        REASON_POLICY_DENIED,
        REASON_PDP_UNAVAILABLE,
        REASON_LEDGER_UNAVAILABLE,
        REASON_PROVIDER_FAILED,
        REASON_RECEIPT_UNCONFIRMED,
    }
)

OUTCOME_DENIED = "denied"
OUTCOME_SUCCEEDED = "succeeded"
OUTCOME_FAILED = "failed"
OUTCOME_UNCONFIRMED = "unconfirmed"

DEFAULT_DECISION_TTL_SECONDS = 300
ENV_LEDGER_DB = "OMO_EVENT_LEDGER_DB"
SERVER_RISKS = frozenset({"R0", "R1", "R2", "R3"})
_HASH_RE = re.compile(r"^[A-Za-z0-9_-]{8,}$")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _default_db_path() -> Path:
    env = os_environ().get(ENV_LEDGER_DB)
    if env:
        return Path(env).resolve()
    root = os_environ().get("WORKSPACE_ROOT", str(Path.home() / "Workspace"))
    return (Path(root) / "runtime" / "omo" / "event-ledger.sqlite3").resolve()


def os_environ() -> Mapping[str, str]:
    import os

    return os.environ


# ---------------------------------------------------------------------------
# Errors (each maps to a stable reason — never allow/succeeded)
# ---------------------------------------------------------------------------


class PolicyEnforcementError(ValueError):
    reason = REASON_PDP_UNAVAILABLE


class InvalidActionRequestError(PolicyEnforcementError):
    reason = REASON_PDP_UNAVAILABLE


# ---------------------------------------------------------------------------
# ActionRequest — the PDP input (full decision context)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ActionRequest:
    action_id: str
    principal_id: str
    executor_id: str
    episode_id: str
    mandate_id: str
    role_context_id: str
    responsibility_id: str
    capability: str
    server_risk: str
    requested_budget: float
    budget_unit: str
    disclosure_policy: str
    request_hash: str | None = None
    trace_id: str | None = None
    mandate_version: int = 1
    principal_authority_ref: str | None = None
    principal_receipt_digest: str | None = None
    credential_ref: str | None = None

    def validate(self) -> None:
        errors: list[str] = []

        def check(name: str, value: Any, pattern: str) -> None:
            if not isinstance(value, str) or not value or re.match(pattern, value) is None:
                errors.append(f"{name} must match {pattern!r}")

        check("action_id", self.action_id, r"^action:[A-Za-z0-9_:.-]+$")
        check("principal_id", self.principal_id, r"^principal:[A-Za-z0-9_:.-]+$")
        check("executor_id", self.executor_id, r"^agent:[A-Za-z0-9_:.-]+$")
        if not isinstance(self.episode_id, str) or len(self.episode_id) < 8:
            errors.append("episode_id must be a string of length >= 8")
        check("mandate_id", self.mandate_id, r"^mandate:[A-Za-z0-9_:.-]+$")
        check("role_context_id", self.role_context_id, r"^role:[A-Za-z0-9_:.-]+$")
        check(
            "responsibility_id",
            self.responsibility_id,
            r"^responsibility:[A-Za-z0-9_:.-]+$",
        )
        if not isinstance(self.capability, str) or not self.capability.startswith("bos://"):
            errors.append("capability must be an exact bos:// URI (no wildcards)")
        if self.server_risk not in SERVER_RISKS:
            errors.append("server_risk must be one of R0/R1/R2/R3")
        if (
            not isinstance(self.requested_budget, (int, float))
            or not math.isfinite(self.requested_budget)
            or self.requested_budget < 0
        ):
            errors.append("requested_budget must be a finite number >= 0")
        check("budget_unit", self.budget_unit, r"^[a-z][a-z0-9_-]*$")
        if not isinstance(self.disclosure_policy, str) or not self.disclosure_policy.startswith("disclosure:"):
            errors.append("disclosure_policy must start with 'disclosure:'")
        if self.request_hash is not None and _HASH_RE.match(self.request_hash) is None:
            errors.append("request_hash must match '^[A-Za-z0-9_-]{8,}$'")
        if self.trace_id is not None and _HASH_RE.match(self.trace_id) is None:
            errors.append("trace_id must match '^[A-Za-z0-9_-]{8,}$'")
        if not isinstance(self.mandate_version, int) or self.mandate_version < 1:
            errors.append("mandate_version must be an int >= 1")
        if (
            self.principal_authority_ref is not None
            and re.match(r"^authority:[A-Za-z0-9_.:/-]+$", self.principal_authority_ref) is None
        ):
            errors.append("principal_authority_ref must match '^authority:[A-Za-z0-9_.:/-]+$'")
        if (
            self.principal_receipt_digest is not None
            and re.match(r"^sha256:[a-f0-9]{64}$", self.principal_receipt_digest) is None
        ):
            errors.append("principal_receipt_digest must match '^sha256:[a-f0-9]{64}$'")
        if errors:
            raise InvalidActionRequestError("; ".join(errors))


def compute_request_hash(request: ActionRequest) -> str:
    """OMO-side canonical hash of the decision context (direct API default).

    Used only when the caller does not supply ``request_hash``.  The Agora
    adapter never calls this: it trusts Agora's top-level
    ``request_dict['request_hash']``.
    """
    payload = json.dumps(
        {
            "action_id": request.action_id,
            "principal_id": request.principal_id,
            "executor_id": request.executor_id,
            "episode_id": request.episode_id,
            "mandate_id": request.mandate_id,
            "capability": request.capability,
            "server_risk": request.server_risk,
            "requested_budget": request.requested_budget,
            "budget_unit": request.budget_unit,
            "disclosure_policy": request.disclosure_policy,
            "principal_authority_ref": request.principal_authority_ref or "",
            "principal_receipt_digest": request.principal_receipt_digest or "",
        },
        sort_keys=True,
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DecisionResult:
    decision: PolicyDecision
    persisted: bool
    reused: bool = False


@dataclass(frozen=True)
class ExecutionOutcome:
    decision_id: str | None
    status: str
    reason: str
    provider_calls: int
    started_receipt_id: str | None = None
    terminal_receipt_id: str | None = None
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "status": self.status,
            "reason": self.reason,
            "provider_calls": self.provider_calls,
            "started_receipt_id": self.started_receipt_id,
            "terminal_receipt_id": self.terminal_receipt_id,
            "detail": self.detail,
        }


# ---------------------------------------------------------------------------
# PolicyEnforcementService — PDP + durable decision/receipt causality
# ---------------------------------------------------------------------------


class PolicyEnforcementService:
    """Single-user PDP/Ledger enforcement over an already-connected broker."""

    def __init__(
        self,
        broker: LedgerBroker,
        *,
        manager: MandateManager | None = None,
        clock: Callable[[], str] = _utc_now,
        decision_ttl_seconds: int = DEFAULT_DECISION_TTL_SECONDS,
        authority: Any | None = None,
    ) -> None:
        self._broker = broker
        self._manager = manager or MandateManager(broker, clock=clock)
        self._clock = clock
        self._ttl = decision_ttl_seconds
        if authority is None:
            from omo.sovereignty.principal_authority import DefaultPrincipalAuthority

            authority = DefaultPrincipalAuthority()
        self._authority = authority

    @classmethod
    def open(cls, db_path: str | Any, **kwargs: Any) -> PolicyEnforcementService:
        return cls(LedgerBroker.connect(db_path), **kwargs)

    @property
    def broker(self) -> LedgerBroker:
        return self._broker

    # -- PDP -----------------------------------------------------------------

    def decide(self, request: ActionRequest) -> DecisionResult:
        """Evaluate one action request against the mandate ledger.

        Idempotency gate: same (action_id, request_hash) reuses the prior
        decision; same action_id with a different request_hash is rejected
        with a durable ``policy_denied``.  Fresh requests run W2-02 admission
        and the resulting PolicyDecision is appended durably BEFORE any side
        effect may run.  PDP failure or decision-append failure yields a deny
        decision with a stable reason (never allow).
        """
        request.validate()
        request_hash = request.request_hash or compute_request_hash(request)
        trace_id = request.trace_id or uuid4().hex[:24]

        # Principal authority verification happens BEFORE the idempotency gate:
        # a fresh request must first prove a verified identity, and a replayed
        # request carries the same verified authority fields.  Denial here is
        # fail-closed with zero provider/tool/ledger effect.
        authority_deny = self._verify_principal_authority(request, request_hash, trace_id)
        if authority_deny is not None:
            return DecisionResult(authority_deny, persisted=self._persist_decision(authority_deny))

        # Idempotency gate + PDP share one fail-closed boundary: a ledger
        # read failure anywhere here means the PDP is unavailable (deny).
        try:
            history = self.replay_decisions(request.action_id)
            for prior in history:
                if prior.request_hash == request_hash:
                    return DecisionResult(prior, persisted=False, reused=True)
            if history:
                issued_at = self._request_mandate_grant_floor(request)
                deny = self._build_decision(
                    request,
                    request_hash,
                    trace_id,
                    decision="deny",
                    reason=REASON_POLICY_DENIED,
                    description=(f"request_hash_mismatch: {request.action_id} already decided under a different hash"),
                    issued_at=issued_at,
                )
                return DecisionResult(deny, persisted=self._persist_decision(deny))

            admission = self._manager.admit(
                mandate_id=request.mandate_id,
                principal_id=request.principal_id,
                executor_id=request.executor_id,
                episode_id=request.episode_id,
                role_context_id=request.role_context_id,
                responsibility_id=request.responsibility_id,
                capability=request.capability,
                risk_level=request.server_risk,
                requested_budget=request.requested_budget,
                budget_unit=request.budget_unit,
                disclosure_policy=request.disclosure_policy,
            )
        except Exception as exc:
            deny = self._build_decision(
                request,
                request_hash,
                trace_id,
                decision="deny",
                reason=REASON_PDP_UNAVAILABLE,
                description=str(exc),
            )
            return DecisionResult(deny, persisted=False)

        try:
            timing = self._mandate_decision_timing(admission.mandate) if admission.mandate is not None else None
        except Exception as exc:
            deny = self._build_decision(
                request,
                request_hash,
                trace_id,
                decision="deny",
                reason=REASON_PDP_UNAVAILABLE,
                description=str(exc),
            )
            return DecisionResult(deny, persisted=False)

        if not admission.allowed:
            deny = self._build_decision(
                request,
                request_hash,
                trace_id,
                decision="deny",
                reason=REASON_POLICY_DENIED,
                description=admission.reason,
                mandate=admission.mandate,
                issued_at=timing[0] if timing is not None else None,
                expires_at=timing[1] if timing is not None else None,
            )
            return DecisionResult(deny, persisted=self._persist_decision(deny))

        if timing[2]:
            deny = self._build_decision(
                request,
                request_hash,
                trace_id,
                decision="deny",
                reason=REASON_POLICY_DENIED,
                description="mandate_expired_at_decision_time",
                mandate=admission.mandate,
                issued_at=timing[0],
                expires_at=timing[1],
            )
            return DecisionResult(deny, persisted=self._persist_decision(deny))

        allow = self._build_decision(
            request,
            request_hash,
            trace_id,
            decision="allow",
            reason=REASON_ALLOWED,
            description=None,
            mandate=admission.mandate,
            issued_at=timing[0],
            expires_at=timing[1],
        )
        try:
            self._append_decision(request, allow)
            return DecisionResult(allow, persisted=True)
        except DuplicateEventError:
            return DecisionResult(allow, persisted=False, reused=True)
        except LedgerError as exc:
            deny = self._build_decision(
                request,
                request_hash,
                trace_id,
                decision="deny",
                reason=REASON_LEDGER_UNAVAILABLE,
                description=str(exc),
                mandate=admission.mandate,
                issued_at=timing[0],
                expires_at=timing[1],
            )
            return DecisionResult(deny, persisted=False)

    def enqueue_allowed_action(self, request: ActionRequest, *, priority: int = 0) -> str:
        """Persist an admitted action for a later side-effect executor.

        Admission and queueing share the same ledger-backed broker, while the
        queue remains a separate replayable stream so execution can happen
        outside the policy decision call.
        """
        result = self.decide(request)
        if result.decision.decision != "allow":
            raise PolicyEnforcementError(
                f"cannot queue action {request.action_id}: {result.decision.reason}"
            )

        queue = PersistentQueue(self._broker, f"policy-actions:{request.principal_id}")
        return queue.enqueue(
            {
                "action_request": asdict(request),
                "decision_id": result.decision.decision_id,
            },
            priority=priority,
        )

    def _verify_principal_authority(
        self,
        request: ActionRequest,
        request_hash: str,
        trace_id: str,
    ) -> PolicyDecision | None:
        """Fail-closed principal authority gate (BET-Y1Q3-T4-04).

        Returns a deny PolicyDecision when authority verification fails, else
        None.  Never produces a provider/tool/ledger effect.
        """
        from omo.sovereignty.principal_authority import (
            REASON_AUTHORITY_CREDENTIAL_MISMATCH,
            REASON_AUTHORITY_DIGEST_UNVERIFIED,
            REASON_AUTHORITY_EXPIRED,
            REASON_AUTHORITY_PRINCIPAL_MISMATCH,
            REASON_AUTHORITY_REQUIRED,
            REASON_AUTHORITY_UNKNOWN,
            AuthorityVerificationError,
            digest_receipt,
        )

        # 1. missing authority receipt -> deny
        if not request.principal_authority_ref or not request.principal_receipt_digest:
            return self._build_decision(
                request,
                request_hash,
                trace_id,
                decision="deny",
                reason=REASON_AUTHORITY_REQUIRED,
                description="principal authority receipt required before admission",
            )

        now = _utc_now()
        credential_ref = request.credential_ref or ""
        try:
            receipt = self._authority.verify(
                request.principal_id,
                credential_ref,
                now=now,
            )
        except AuthorityVerificationError as exc:
            return self._build_decision(
                request,
                request_hash,
                trace_id,
                decision="deny",
                reason=exc.reason,
                description=str(exc),
            )
        except Exception as exc:  # pragma: no cover - authority infra failure
            return self._build_decision(
                request,
                request_hash,
                trace_id,
                decision="deny",
                reason=REASON_AUTHORITY_UNKNOWN,
                description=str(exc),
            )

        # 2. receipt principal mismatch
        if receipt.principal_id != request.principal_id:
            return self._build_decision(
                request,
                request_hash,
                trace_id,
                decision="deny",
                reason=REASON_AUTHORITY_PRINCIPAL_MISMATCH,
                description=(
                    f"principal authority receipt principal {receipt.principal_id!r} "
                    f"does not match request principal {request.principal_id!r}"
                ),
            )
        # 3. digest mismatch (authority digest must equal request digest)
        if digest_receipt(receipt) != request.principal_receipt_digest:
            return self._build_decision(
                request,
                request_hash,
                trace_id,
                decision="deny",
                reason=REASON_AUTHORITY_DIGEST_UNVERIFIED,
                description="principal_receipt_digest does not match authority digest",
            )
        # 4. expired receipt
        try:
            from datetime import datetime

            if datetime.fromisoformat(now) > datetime.fromisoformat(receipt.expires_at):
                return self._build_decision(
                    request,
                    request_hash,
                    trace_id,
                    decision="deny",
                    reason=REASON_AUTHORITY_EXPIRED,
                    description="principal authority receipt expired",
                )
        except ValueError:
            return self._build_decision(
                request,
                request_hash,
                trace_id,
                decision="deny",
                reason=REASON_AUTHORITY_EXPIRED,
                description="malformed receipt timestamp",
            )
        return None

    # -- Receipts -------------------------------------------------------------

    def persist_started(self, decision: PolicyDecision) -> ActionReceipt:
        """Durably append the started receipt BEFORE any provider call."""
        started = self._build_receipt(decision, status="started")
        try:
            self._append_receipt(started, key_suffix="started")
            return started
        except DuplicateEventError:
            return self._replay_started(decision)

    def persist_terminal(
        self,
        decision: PolicyDecision,
        *,
        status: str,
        started_at: datetime,
        result: dict[str, Any] | None = None,
        reason: str | None = None,
        detail: str | None = None,
    ) -> ActionReceipt:
        """Durably append the terminal succeeded|failed receipt."""
        if status not in (OUTCOME_SUCCEEDED, OUTCOME_FAILED):
            raise PolicyEnforcementError(f"terminal status must be succeeded or failed, got {status!r}")
        if status == OUTCOME_FAILED and reason in (None, "", REASON_ALLOWED):
            raise PolicyEnforcementError("failed receipt requires a stable failure reason")
        terminal = self._build_receipt(
            decision,
            status=status,
            started_at=started_at,
            completed_at=self._now(),
            reason=reason,
            result=result,
            description=detail,
        )
        try:
            self._append_receipt(terminal, key_suffix="terminal")
            return terminal
        except DuplicateEventError:
            return self._replay_terminal(decision)

    # -- Full flow ------------------------------------------------------------

    def execute(
        self,
        request: ActionRequest,
        provider: Callable[[ActionRequest], dict[str, Any]],
    ) -> ExecutionOutcome:
        """Full fail-closed flow; provider runs at most once."""
        try:
            result = self.decide(request)
        except InvalidActionRequestError as exc:
            return ExecutionOutcome(None, OUTCOME_DENIED, REASON_PDP_UNAVAILABLE, 0, detail=str(exc))
        decision = result.decision
        if decision.decision == "deny":
            return ExecutionOutcome(
                decision.decision_id,
                OUTCOME_DENIED,
                decision.reason,
                0,
                detail=decision.description or "",
            )
        if result.reused:
            return self._prior_outcome(decision)

        try:
            started = self.persist_started(decision)
        except LedgerError as exc:
            return ExecutionOutcome(
                decision.decision_id,
                OUTCOME_FAILED,
                REASON_LEDGER_UNAVAILABLE,
                0,
                detail=str(exc),
            )

        try:
            provider_result = provider(request)
            if not isinstance(provider_result, dict):
                raise TypeError(f"provider must return a dict, got {type(provider_result).__name__}")
        except Exception as exc:
            try:
                terminal = self.persist_terminal(
                    decision,
                    status=OUTCOME_FAILED,
                    started_at=started.started_at,
                    reason=REASON_PROVIDER_FAILED,
                    detail=str(exc),
                )
            except LedgerError as exc2:
                return ExecutionOutcome(
                    decision.decision_id,
                    OUTCOME_UNCONFIRMED,
                    REASON_RECEIPT_UNCONFIRMED,
                    1,
                    started_receipt_id=started.receipt_id,
                    detail=str(exc2),
                )
            return ExecutionOutcome(
                decision.decision_id,
                OUTCOME_FAILED,
                REASON_PROVIDER_FAILED,
                1,
                started_receipt_id=started.receipt_id,
                terminal_receipt_id=terminal.receipt_id,
                detail=str(exc),
            )

        try:
            terminal = self.persist_terminal(
                decision,
                status=OUTCOME_SUCCEEDED,
                started_at=started.started_at,
                result=provider_result,
            )
        except LedgerError as exc:
            return ExecutionOutcome(
                decision.decision_id,
                OUTCOME_UNCONFIRMED,
                REASON_RECEIPT_UNCONFIRMED,
                1,
                started_receipt_id=started.receipt_id,
                detail=str(exc),
            )
        return ExecutionOutcome(
            decision.decision_id,
            OUTCOME_SUCCEEDED,
            REASON_ALLOWED,
            1,
            started_receipt_id=started.receipt_id,
            terminal_receipt_id=terminal.receipt_id,
        )

    # -- Replay ---------------------------------------------------------------

    def replay_decisions(self, action_id: str | None = None) -> list[PolicyDecision]:
        out: list[PolicyDecision] = []
        for row in self._broker.read(producer=PDP_PRODUCER):
            if row.get("event_type") != EVT_POLICY_DECISION:
                continue
            payload = json.loads(row["payload_json"])
            if action_id is not None and payload.get("action_id") != action_id:
                continue
            out.append(PolicyDecision.model_validate(payload))
        return out

    def replay_receipts(self, action_id: str | None = None) -> list[ActionReceipt]:
        out: list[ActionReceipt] = []
        for row in self._broker.read(producer=PDP_PRODUCER):
            if row.get("event_type") not in _RECEIPT_EVENT_TYPES:
                continue
            payload = json.loads(row["payload_json"])
            if action_id is not None and payload.get("action_id") != action_id:
                continue
            out.append(ActionReceipt.model_validate(payload))
        return out

    def replay_events(self) -> list[dict[str, Any]]:
        return list(self._broker.read(producer=PDP_PRODUCER))

    def decision(self, action_id: str) -> PolicyDecision | None:
        decisions = self.replay_decisions(action_id)
        return decisions[-1] if decisions else None

    # -- Internals ------------------------------------------------------------

    def _now(self) -> datetime:
        ts = self._clock()
        try:
            dt = datetime.fromisoformat(ts)
        except (ValueError, TypeError) as exc:
            raise PolicyEnforcementError(f"clock returned invalid ISO datetime {ts!r}: {exc}") from exc
        if dt.tzinfo is None:
            raise PolicyEnforcementError(f"clock returned naive (non-timezone-aware) datetime {ts!r}")
        return dt

    def _build_decision(
        self,
        request: ActionRequest,
        request_hash: str,
        trace_id: str,
        *,
        decision: str,
        reason: str,
        description: str | None,
        mandate: Any | None = None,
        issued_at: datetime | None = None,
        expires_at: datetime | None = None,
    ) -> PolicyDecision:
        now = issued_at or self._now()
        if mandate is not None:
            mandate_version = mandate.mandate_version
            issued_at = now
            expires_at = expires_at or mandate.expires_at
        else:
            mandate_version = request.mandate_version
            issued_at, expires_at = now, expires_at or now + timedelta(seconds=self._ttl)
        try:
            return PolicyDecision(
                decision_id=f"decision:{uuid4().hex[:24]}",
                schema_version="policy-decision/v1",
                decision=decision,
                action_id=request.action_id,
                principal_id=request.principal_id,
                executor_id=request.executor_id,
                episode_id=request.episode_id,
                mandate_id=request.mandate_id,
                mandate_version=mandate_version,
                capability=request.capability,
                server_risk=request.server_risk,
                budget_limit=float(request.requested_budget),
                budget_unit=request.budget_unit,
                disclosure=request.disclosure_policy,
                request_hash=request_hash,
                trace_id=trace_id,
                issued_at=issued_at,
                expires_at=expires_at,
                reason=reason,
                description=description or None,
                principal_authority_ref=request.principal_authority_ref,
                principal_receipt_digest=request.principal_receipt_digest,
            )
        except PydanticValidationError as exc:
            raise InvalidActionRequestError(
                f"cannot build PolicyDecision for {request.action_id}: {exc.errors()}"
            ) from exc

    def _mandate_decision_timing(self, mandate: Any) -> tuple[datetime, datetime, bool]:
        """Clamp a mandate-bound decision to its durable grant boundary."""
        grant_at = self._persisted_mandate_grant_at(mandate.mandate_id, mandate.principal_id)
        issued_at = max(self._now(), grant_at)
        if issued_at >= mandate.expires_at:
            return issued_at, issued_at, True
        return issued_at, mandate.expires_at, False

    def _request_mandate_grant_floor(self, request: ActionRequest) -> datetime:
        """Use a matching durable grant as the local lower bound when present."""
        now = self._now()
        for row in self._broker.read(producer=MANDATE_PRODUCER):
            if (
                row.get("event_type") == EVT_MANDATE_GRANT
                and row.get("mandate_id") == request.mandate_id
                and row.get("principal_id") == request.principal_id
            ):
                return max(now, self._parse_mandate_grant_occurred_at(row))
        return now

    def _persisted_mandate_grant_at(self, mandate_id: str, principal_id: str) -> datetime:
        for row in self._broker.read(producer=MANDATE_PRODUCER):
            if (
                row.get("event_type") == EVT_MANDATE_GRANT
                and row.get("mandate_id") == mandate_id
                and row.get("principal_id") == principal_id
            ):
                return self._parse_mandate_grant_occurred_at(row)
        raise PolicyEnforcementError("admitted mandate has no persisted grant event")

    @staticmethod
    def _parse_mandate_grant_occurred_at(row: Mapping[str, Any]) -> datetime:
        try:
            grant_at = datetime.fromisoformat(str(row["occurred_at"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise PolicyEnforcementError("persisted mandate grant has invalid occurred_at") from exc
        if grant_at.tzinfo is None:
            raise PolicyEnforcementError("persisted mandate grant occurred_at must be timezone-aware")
        return grant_at

    def _build_receipt(
        self,
        decision: PolicyDecision,
        *,
        status: str,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        reason: str | None = None,
        result: dict[str, Any] | None = None,
        description: str | None = None,
    ) -> ActionReceipt:
        started = max(started_at or self._now(), decision.issued_at)
        if completed_at is not None:
            completed_at = max(completed_at, started)
        try:
            return ActionReceipt(
                receipt_id=f"receipt:{uuid4().hex[:24]}",
                schema_version="action-receipt/v1",
                decision_id=decision.decision_id,
                action_id=decision.action_id,
                principal_id=decision.principal_id,
                executor_id=decision.executor_id,
                episode_id=decision.episode_id,
                mandate_id=decision.mandate_id,
                mandate_version=decision.mandate_version,
                capability=decision.capability,
                server_risk=decision.server_risk,
                budget_limit=decision.budget_limit,
                budget_unit=decision.budget_unit,
                disclosure=decision.disclosure,
                request_hash=decision.request_hash,
                trace_id=decision.trace_id,
                issued_at=decision.issued_at,
                expires_at=decision.expires_at,
                status=status,
                started_at=started,
                completed_at=completed_at,
                reason=reason,
                result=result,
                description=description or None,
                principal_authority_ref=decision.principal_authority_ref,
                principal_receipt_digest=decision.principal_receipt_digest,
            )
        except PydanticValidationError as exc:
            raise PolicyEnforcementError(
                f"cannot build ActionReceipt for {decision.decision_id}: {exc.errors()}"
            ) from exc

    def _append_decision(self, request: ActionRequest, decision: PolicyDecision) -> int:
        return self._broker.append(
            event_type=EVT_POLICY_DECISION,
            producer=PDP_PRODUCER,
            principal_id=decision.principal_id,
            space_id=PDP_SPACE_ID,
            correlation_id=f"action|{decision.action_id}|{decision.request_hash}",
            idempotency_key=f"{decision.action_id}|{decision.request_hash}|decision",
            payload=decision.model_dump(mode="json"),
            episode_id=decision.episode_id,
            role_context_id=request.role_context_id,
            responsibility_id=request.responsibility_id,
            mandate_id=decision.mandate_id,
            occurred_at=decision.issued_at.isoformat(),
        )

    def _append_receipt(self, receipt: ActionReceipt, *, key_suffix: str) -> int:
        event_type = {
            "started": EVT_ACTION_STARTED,
            OUTCOME_SUCCEEDED: EVT_ACTION_SUCCEEDED,
            OUTCOME_FAILED: EVT_ACTION_FAILED,
        }[receipt.status]
        return self._broker.append(
            event_type=event_type,
            producer=PDP_PRODUCER,
            principal_id=receipt.principal_id,
            space_id=PDP_SPACE_ID,
            correlation_id=(f"action|{receipt.action_id}|{receipt.request_hash}|{receipt.status}"),
            idempotency_key=f"{receipt.action_id}|{receipt.request_hash}|{key_suffix}",
            payload=receipt.model_dump(mode="json"),
            episode_id=receipt.episode_id,
            mandate_id=receipt.mandate_id,
            causation_id=receipt.decision_id,
            occurred_at=(receipt.completed_at or receipt.started_at).isoformat(),
        )

    def _persist_decision(self, decision: PolicyDecision) -> bool:
        """Best-effort durable append for deny decisions.  True when durable."""
        request = ActionRequest(
            action_id=decision.action_id,
            principal_id=decision.principal_id,
            executor_id=decision.executor_id,
            episode_id=decision.episode_id,
            mandate_id=decision.mandate_id,
            role_context_id="role:unknown",
            responsibility_id="responsibility:unknown",
            capability=decision.capability,
            server_risk=decision.server_risk,
            requested_budget=decision.budget_limit,
            budget_unit=decision.budget_unit,
            disclosure_policy=decision.disclosure,
            request_hash=decision.request_hash,
            principal_authority_ref=decision.principal_authority_ref,
            principal_receipt_digest=decision.principal_receipt_digest,
        )
        try:
            self._append_decision(request, decision)
            return True
        except DuplicateEventError:
            return True
        except LedgerError:
            return False

    def _prior_outcome(self, decision: PolicyDecision) -> ExecutionOutcome:
        receipts = [r for r in self.replay_receipts(decision.action_id) if r.decision_id == decision.decision_id]
        started = next((r for r in receipts if r.status == "started"), None)
        terminal = next(
            (r for r in receipts if r.status in (OUTCOME_SUCCEEDED, OUTCOME_FAILED)),
            None,
        )
        if terminal is not None:
            reason = (
                REASON_ALLOWED if terminal.status == OUTCOME_SUCCEEDED else (terminal.reason or REASON_PROVIDER_FAILED)
            )
            return ExecutionOutcome(
                decision.decision_id,
                terminal.status,
                reason,
                0,
                started_receipt_id=started.receipt_id if started else None,
                terminal_receipt_id=terminal.receipt_id,
            )
        if started is not None:
            return ExecutionOutcome(
                decision.decision_id,
                OUTCOME_UNCONFIRMED,
                REASON_RECEIPT_UNCONFIRMED,
                0,
                started_receipt_id=started.receipt_id,
            )
        return ExecutionOutcome(decision.decision_id, OUTCOME_UNCONFIRMED, REASON_RECEIPT_UNCONFIRMED, 0)

    def _replay_started(self, decision: PolicyDecision) -> ActionReceipt:
        for receipt in self.replay_receipts(decision.action_id):
            if receipt.decision_id == decision.decision_id and receipt.status == "started":
                return receipt
        raise PolicyEnforcementError(f"started receipt missing for {decision.decision_id}")

    def _replay_terminal(self, decision: PolicyDecision) -> ActionReceipt:
        for receipt in self.replay_receipts(decision.action_id):
            if receipt.decision_id == decision.decision_id and receipt.status in (
                OUTCOME_SUCCEEDED,
                OUTCOME_FAILED,
            ):
                return receipt
        raise PolicyEnforcementError(f"terminal receipt missing for {decision.decision_id}")


# ---------------------------------------------------------------------------
# AgoraPepProvider — narrow injection port (no Agora→OMO hard import)
# ---------------------------------------------------------------------------
#
# Agora binds via env AGORA_PEP_PROVIDER=omo.sovereignty.enforcement:
# AgoraPepProvider (or an agora.pep entry point) and calls exactly:
#
#   evaluate(request_dict) -> ecos PolicyDecision
#   start_receipt(decision) -> ecos ActionReceipt
#   confirm_receipt(receipt, status, result, reason) -> bool
#
# request_hash: Agora computes the canonical hash over the FULL request and
# places it at the trusted top-level request_dict['request_hash']; OMO writes
# that value into the PolicyDecision and explicitly IGNORES the caller-
# controlled arguments._omo_policy.request_hash.  The OMO authorization
# context envelope lives in request_dict['arguments']['_omo_policy'].
# Agora bypasses this provider for registered read-only paths, so OMO only
# receives effectful requests. Terminal ledger failure is never swallowed:
# confirm_receipt returns False.


class AgoraPepProvider:
    """OMO PDP behind Agora's narrow SPI (evaluate/start_receipt/confirm_receipt)."""

    def __init__(
        self,
        db_path: str | Any | None = None,
        *,
        service: PolicyEnforcementService | None = None,
    ) -> None:
        self._service = service or PolicyEnforcementService.open(db_path or _default_db_path())
        # decision_id -> OMO decision context for confirm_receipt.
        self._decisions: dict[str, PolicyDecision] = {}

    def evaluate(self, request_dict: Mapping[str, Any]) -> PolicyDecision:
        """Evaluate one effectful request; persist the decision durably.

        ``request_hash`` is the trusted top-level value Agora computed over
        the full request; the caller-controlled ``_omo_policy.request_hash``
        is explicitly ignored.  A missing/invalid trusted hash or a missing
        authorization context raises :class:`InvalidActionRequestError`
        (Agora's PEP translates that to a fail-closed deny).
        """
        request_hash = request_dict.get("request_hash")
        if not isinstance(request_hash, str) or _HASH_RE.match(request_hash) is None:
            raise InvalidActionRequestError("missing trusted top-level request_hash in request_dict")
        request = self._to_action_request(request_dict, request_hash)
        result = self._service.decide(request)
        self._decisions[result.decision.decision_id] = result.decision
        return result.decision

    def start_receipt(self, decision: PolicyDecision) -> ActionReceipt:
        """Durably persist the started receipt BEFORE Agora dispatches.

        Raises on ledger failure so the provider is never called (fail-closed).
        """
        started = self._service.persist_started(decision)
        self._decisions[decision.decision_id] = decision
        return started

    def confirm_receipt(
        self,
        receipt: ActionReceipt,
        status: str,
        result: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> bool:
        """Persist the durable terminal receipt.  False on ledger failure."""
        decision = self._decisions.get(receipt.decision_id)
        if decision is None:
            decision = self._find_decision(receipt)
            if decision is None:
                return False
        failure_reason = reason if reason in _FAILURE_REASONS else REASON_PROVIDER_FAILED
        try:
            self._service.persist_terminal(
                decision,
                status=status if status in (OUTCOME_SUCCEEDED, OUTCOME_FAILED) else OUTCOME_FAILED,
                started_at=receipt.started_at,
                result=result,
                reason=None if status == OUTCOME_SUCCEEDED else failure_reason,
            )
            return True
        except (LedgerError, PolicyEnforcementError):
            return False

    # -- internals ------------------------------------------------------------

    def _find_decision(self, receipt: ActionReceipt) -> PolicyDecision | None:
        for decision in self._service.replay_decisions(receipt.action_id):
            if decision.decision_id == receipt.decision_id:
                return decision
        return None

    def _to_action_request(self, request_dict: Mapping[str, Any], request_hash: str) -> ActionRequest:
        """Map the effectful request to an ActionRequest.

        The OMO authorization context comes exclusively from
        ``arguments._omo_policy``; a missing/incomplete envelope raises
        :class:`InvalidActionRequestError` (fail-closed deny at the PEP).
        ``request_hash`` is the trusted top-level value and is never taken
        from the caller-controlled envelope.
        """
        arguments = request_dict.get("arguments")
        envelope = arguments.get("_omo_policy") if isinstance(arguments, dict) else None
        if not isinstance(envelope, dict) or not envelope:
            raise InvalidActionRequestError("missing _omo_policy authorization context in request arguments")
        try:
            return ActionRequest(
                action_id=envelope.get("action_id", ""),
                principal_id=envelope.get("principal_id", ""),
                executor_id=envelope.get("executor_id", ""),
                episode_id=envelope.get("episode_id", ""),
                mandate_id=envelope.get("mandate_id", ""),
                role_context_id=envelope.get("role_context_id", ""),
                responsibility_id=envelope.get("responsibility_id", ""),
                capability=envelope.get("capability", ""),
                server_risk=envelope.get("server_risk", ""),
                requested_budget=float(envelope.get("requested_budget", 0)),
                budget_unit=envelope.get("budget_unit", ""),
                disclosure_policy=envelope.get("disclosure_policy", ""),
                request_hash=request_hash,  # trusted top-level value
                trace_id=envelope.get("trace_id"),
                mandate_version=int(envelope.get("mandate_version", 1)),
                principal_authority_ref=envelope.get("principal_authority_ref"),
                principal_receipt_digest=envelope.get("principal_receipt_digest"),
                credential_ref=envelope.get("credential_ref"),
            )
        except (TypeError, ValueError) as exc:
            raise InvalidActionRequestError(f"malformed _omo_policy context: {exc}") from exc
