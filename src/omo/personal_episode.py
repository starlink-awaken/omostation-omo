"""W2-05 personal episode kernel over the causal Event Ledger.

This deliberately small service is the local, single-user seam between the
W2-04 inbox and the existing W2-02/03 authorization machinery.  It writes no
state outside :class:`LedgerBroker`: restarting the process therefore cannot
lose an approved episode or manufacture an authorization context.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from ecos.ssot.mof.generated.control.mof_control_models import (
    DelegationMandate,
    EventEnvelope,
    Signal,
)

from omo.event_ledger.broker import LedgerBroker
from omo.personal_episode_helpers import (
    _are_consecutive_weeks,
    _build_observation,
    _canonical_chain_observation,
    _deterministic,
    _evaluate_readiness_gate,
    _feedback_ref,
    _has_valid_revision_receipt,
    _iso_week_key,
    _median,
    _outcome_replay_matches,
    _parse_ts,
    _payload,
    _personal_draft_digest,
    _required_payload,
    _revision_fields,
    _sha256_digest,
    _short_hash,
    _utc_now,
    _validate_burden,
    _week_monday,
)
from omo.sovereignty.enforcement import EVT_ACTION_SUCCEEDED, PDP_PRODUCER
from omo.sovereignty.mandates import (
    EVT_MANDATE_GRANT,
    MANDATE_PRODUCER,
    STATUS_ACTIVE,
    MandateError,
    MandateManager,
)
from omo.sovereignty.roles import SovereigntyError, SovereigntyService

PERSONAL_EPISODE_PRODUCER = "omo-personal-episode"
PERSONAL_EPISODE_SPACE_ID = "personal"
CAPABILITY = "bos://personal/followup/draft"
RISK = "R0"
DISCLOSURE_POLICY = "disclosure:private"

EVT_EPISODE_DECISION = "Episode.Decision.v1"
EVT_SIGNAL_OBSERVED = "SignalObserved.v1"
EVT_EVIDENCE_LOCAL_DRAFT = "Evidence.LocalDraft.v1"
EVT_OUTCOME_HUMAN = "Outcome.Human.v1"

CHAIN_SCHEMA_VERSION = "personal-episode-chain/v1"
EVT_ROLE_CONTEXT_ASSIGNED = "RoleContextAssigned.v1"
EVT_RESPONSIBILITY_LINKED = "ResponsibilityLinked.v1"
EVT_DECISION_PROPOSED = "DecisionProposed.v1"
EVT_HUMAN_ADJUDICATION = "HumanAdjudication.v1"
EVT_MANDATE_GRANTED_CANONICAL = "MandateGranted.v1"
EVT_ACTION_SUCCEEDED_CANONICAL = "ActionSucceeded.v1"
EVT_EVIDENCE_RECORDED = "EvidenceRecorded.v1"
EVT_OUTCOME_OBSERVED = "OutcomeObserved.v1"
EVT_ADJUDICATION_RECORDED = "AdjudicationRecorded.v1"
EVT_MEMORY_CANDIDATE_PROPOSED = "MemoryCandidateProposed.v1"
EVT_EPISODE_CLOSED = "EpisodeClosed.v1"

PERSONAL_SIGNAL_SCENE_ID = "personal-followup-dogfood"
PERSONAL_SIGNAL_JOURNEY_ID = "manual-signal-to-adopted-local-draft"
PERSONAL_SIGNAL_OUTCOME_METRIC = "adopted_real_personal_outcome_count"
QUALIFYING_EPISODE_TARGET = 30

_OUTCOMES = frozenset({"accept", "edit", "reject", "defer", "ignore"})
VALID_OUTPUT_ORIGINS = frozenset({"system", "user_provided", "unknown"})
PERSONAL_DRAFT_EVIDENCE_PREFIX = "evidence://personal-draft/sha256:"
REVISION_RECEIPT_SCHEMA = "revision-receipt/v1"
OUTCOME_FEEDBACK_SCHEMA = "outcome-feedback/v1"
REVISION_FIELDS = frozenset({"title", "context", "deadline", "next_action"})


class PersonalEpisodeError(ValueError):
    """Stable domain error for the personal golden slice."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


@dataclass(frozen=True)
class PersonalEpisodeCard:
    episode_id: str
    request_id: str
    summary: str
    reused: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "episode_id": self.episode_id,
            "request_id": self.request_id,
            "summary": self.summary,
            "reused": self.reused,
        }


@dataclass(frozen=True)
class PersonalEpisodeConfirmation:
    episode_id: str
    mandate_id: str
    reused: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "episode_id": self.episode_id,
            "mandate_id": self.mandate_id,
            "reused": self.reused,
        }


@dataclass(frozen=True)
class PersonalLocalSignal:
    """Trusted, server-resolved metadata for one private local Markdown item.

    The web boundary must resolve the opaque ``item_id`` with Iris before it
    constructs this descriptor.  In particular, this type intentionally has
    no filesystem path and no document body field.
    """

    source_id: str
    item_id: str
    title: str
    content_sha256: str
    source_uri: str
    principal_id: str
    role_id: str
    responsibility_id: str
    executor_id: str


@dataclass(frozen=True)
class PersonalSignalIngestResult:
    """The causal signal/episode pair created from one local item."""

    signal_event_id: str
    signal_id: str
    episode: PersonalEpisodeCard
    reused: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_event_id": self.signal_event_id,
            "signal_id": self.signal_id,
            "episode": self.episode.to_dict(),
            "reused": self.reused,
        }


@dataclass(frozen=True)
class EpisodeDraftSnapshot:
    """Safe persisted Episode fields sufficient for a deterministic local draft.

    Carries only episode identity and draft fields (summary, why_now,
    deadline).  It deliberately has **no** raw signal body, filesystem
    path, content digest, or source URI — callers can hand this object
    to a draft-generation step without leaking private local-file
    metadata.

    Unlike :class:`PersonalExecutionContext`, the snapshot is available
    as soon as the episode decision is persisted; no active mandate is
    required.
    """

    episode_id: str
    request_id: str
    summary: str
    why_now: str | None = None
    deadline: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "episode_id": self.episode_id,
            "request_id": self.request_id,
            "summary": self.summary,
            "why_now": self.why_now,
            "deadline": self.deadline,
        }


@dataclass(frozen=True)
class PersonalExecutionContext:
    """Ledger-replayed context consumable as ``arguments._omo_policy``."""

    episode_id: str
    mandate_id: str
    principal_id: str
    executor_id: str
    role_context_id: str
    responsibility_id: str
    action_id: str
    trace_id: str
    principal_authority_ref: str | None = None
    principal_receipt_digest: str | None = None
    credential_ref: str | None = None

    @property
    def omo_policy(self) -> dict[str, Any]:
        """Return the complete fail-closed W2-03 PEP envelope."""
        policy: dict[str, Any] = {
            "action_id": self.action_id,
            "principal_id": self.principal_id,
            "executor_id": self.executor_id,
            "episode_id": self.episode_id,
            "mandate_id": self.mandate_id,
            "role_context_id": self.role_context_id,
            "responsibility_id": self.responsibility_id,
            "capability": CAPABILITY,
            "server_risk": RISK,
            "requested_risk": RISK,
            "requested_budget": 1.0,
            "budget_unit": "call",
            "disclosure_policy": DISCLOSURE_POLICY,
            "trace_id": self.trace_id,
            "mandate_version": 1,
        }
        if self.principal_authority_ref is not None:
            policy["principal_authority_ref"] = self.principal_authority_ref
        if self.principal_receipt_digest is not None:
            policy["principal_receipt_digest"] = self.principal_receipt_digest
        if self.credential_ref is not None:
            policy["credential_ref"] = self.credential_ref
        return policy

    def to_dict(self) -> dict[str, Any]:
        result = dict(self.omo_policy)
        result["_omo_policy"] = dict(self.omo_policy)
        return result


@dataclass(frozen=True)
class WeeklySample:
    """One natural-week (ISO Monday-based) aggregate for a principal.

    No raw signal body, path, source URI, or digest is exposed — only
    counts, verdict distributions, and summed burden fields.
    """

    week_key: str
    total_episodes: int
    qualifying_episodes: int
    system_accept_episodes: int
    complete_burden_episodes: int
    review_lt_saved_episodes: int
    summed_review_seconds: float | None
    summed_saved_seconds: float | None
    verdict_distribution: dict[str, int]
    system_evidence_count: int
    user_evidence_count: int
    unknown_evidence_count: int
    gate_met: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "week_key": self.week_key,
            "total_episodes": self.total_episodes,
            "qualifying_episodes": self.qualifying_episodes,
            "system_accept_episodes": self.system_accept_episodes,
            "complete_burden_episodes": self.complete_burden_episodes,
            "review_lt_saved_episodes": self.review_lt_saved_episodes,
            "summed_review_seconds": self.summed_review_seconds,
            "summed_saved_seconds": self.summed_saved_seconds,
            "verdict_distribution": dict(self.verdict_distribution),
            "system_evidence_count": self.system_evidence_count,
            "user_evidence_count": self.user_evidence_count,
            "unknown_evidence_count": self.unknown_evidence_count,
            "gate_met": self.gate_met,
        }


@dataclass(frozen=True)
class PrincipalObservation:
    """Deterministic, read-only per-principal observation over the same Ledger.

    The observation never exposes raw signal body, filesystem path,
    source URI, or content digest.  It leaves event count and hash chain
    unchanged — it only calls ``broker.read()``.
    """

    principal_id: str
    readiness: str
    total_episodes: int
    qualifying_episodes: int
    qualifying_target: int
    remaining_to_target: int
    verdict_distribution: dict[str, int]
    system_evidence_count: int
    user_evidence_count: int
    unknown_evidence_count: int
    signal_to_verdict_latency_seconds: float | None
    weekly_samples: list[WeeklySample]
    gate_gaps: list[str]
    chain_schema_version: str = CHAIN_SCHEMA_VERSION
    qualifying_episode_ids: list[str] = field(default_factory=list)
    episode_gaps: dict[str, list[str]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "principal_id": self.principal_id,
            "readiness": self.readiness,
            "total_episodes": self.total_episodes,
            "qualifying_episodes": self.qualifying_episodes,
            "qualifying_target": self.qualifying_target,
            "remaining_to_target": self.remaining_to_target,
            "verdict_distribution": dict(self.verdict_distribution),
            "system_evidence_count": self.system_evidence_count,
            "user_evidence_count": self.user_evidence_count,
            "unknown_evidence_count": self.unknown_evidence_count,
            "signal_to_verdict_latency_seconds": self.signal_to_verdict_latency_seconds,
            "weekly_samples": [s.to_dict() for s in self.weekly_samples],
            "gate_gaps": list(self.gate_gaps),
            "chain_schema_version": self.chain_schema_version,
            "qualifying_episode_ids": list(self.qualifying_episode_ids),
            "episode_gaps": {key: list(value) for key, value in self.episode_gaps.items()},
        }


class PersonalEpisodeService:
    """A deterministic, ledger-backed personal draft episode service."""

    def __init__(
        self,
        broker: LedgerBroker,
        *,
        clock: Callable[[], str] = _utc_now,
        principal_authority: Any = None,
        default_credential_ref: str | None = None,
    ) -> None:
        self._broker = broker
        self._clock = clock
        self._principal_authority = principal_authority
        self._default_credential_ref = default_credential_ref

    @classmethod
    def open(
        cls,
        db_path: str | Any,
        *,
        clock: Callable[[], str] = _utc_now,
        principal_authority: Any = None,
        default_credential_ref: str | None = None,
    ) -> PersonalEpisodeService:
        return cls(
            LedgerBroker.connect(db_path),
            clock=clock,
            principal_authority=principal_authority,
            default_credential_ref=default_credential_ref,
        )

    def start(
        self,
        *,
        principal_id: str,
        role_id: str,
        responsibility_id: str,
        executor_id: str,
        request_id: str,
        summary: str,
        why_now: str = "",
        deadline: str | None = None,
    ) -> PersonalEpisodeCard:
        """Append exactly one Inbox Decision card, idempotent by request id."""
        self._required("request_id", request_id)
        self._required("summary", summary)
        self._required("executor_id", executor_id)
        existing = self._find_start(principal_id, request_id)
        if existing is not None:
            payload = _payload(existing)
            return PersonalEpisodeCard(
                episode_id=str(existing["episode_id"]),
                request_id=request_id,
                summary=str(payload.get("summary", "")),
                reused=True,
            )

        assignment = self._active_assignment(principal_id, role_id, responsibility_id)
        episode_id = _deterministic("episode_", principal_id, role_id, responsibility_id, request_id)
        payload = {
            "episode_id": episode_id,
            "request_id": request_id,
            "summary": summary,
            "why_now": why_now or None,
            "deadline": deadline,
            "risk": RISK,
            "authority": "human_confirmation_required",
            "status": "pending_confirmation",
            "executor_id": executor_id,
            "role_id": role_id,
            "responsibility_id": responsibility_id,
        }
        decision_event_id = self._ensure_context_and_decision_chain(
            episode_id=episode_id,
            principal_id=principal_id,
            role_id=role_id,
            responsibility_id=responsibility_id,
            assignment=assignment,
            decision_payload=payload,
            causation_id=None,
        )
        self._broker.append(
            EVT_EPISODE_DECISION,
            producer=PERSONAL_EPISODE_PRODUCER,
            principal_id=principal_id,
            space_id=PERSONAL_EPISODE_SPACE_ID,
            correlation_id=f"personal-episode|{episode_id}",
            idempotency_key=f"start|{principal_id}|{request_id}",
            episode_id=episode_id,
            role_context_id=role_id,
            responsibility_id=responsibility_id,
            causation_id=decision_event_id,
            payload=payload,
            occurred_at=self._clock_ts(),
        )
        return PersonalEpisodeCard(episode_id=episode_id, request_id=request_id, summary=summary)

    def ingest_local_signal(self, signal: PersonalLocalSignal) -> PersonalSignalIngestResult:
        """Turn one trusted Iris-resolved local item into a causal draft card.

        This is deliberately a thin, single-user ingress: validation is
        complete before the first append; raw file content and paths never
        enter the Event Ledger; and replay is deterministic by
        ``principal_id + source_id + item_id + content_sha256``.  It does not
        attempt a cross-event transaction or concurrent exactly-once protocol.
        """
        self._validate_local_signal(signal)
        occurred_at = self._clock_ts()
        source_key = _short_hash(
            signal.principal_id,
            signal.source_id,
            signal.item_id,
            signal.content_sha256,
        )
        event_id = _deterministic("evt_", "local-signal", source_key)
        signal_id = _deterministic("signal_", "local-signal", source_key)
        episode_id = _deterministic(
            "episode_",
            "local-signal",
            signal.principal_id,
            signal.role_id,
            signal.responsibility_id,
            source_key,
        )
        request_id = f"local-signal:{source_key}"

        existing = self._find_local_signal(signal.principal_id, source_key)
        if existing is not None:
            existing_payload = _payload(existing)
            try:
                existing_episode = self._episode_for_signal(str(existing["event_id"]), signal.principal_id)
            except PersonalEpisodeError as exc:
                if exc.reason != "malformed_signal":
                    raise
            else:
                episode_payload = _payload(existing_episode)
                return PersonalSignalIngestResult(
                    signal_event_id=str(existing["event_id"]),
                    signal_id=str(existing_payload["signal_id"]),
                    episode=PersonalEpisodeCard(
                        episode_id=str(existing_episode["episode_id"]),
                        request_id=str(episode_payload["request_id"]),
                        summary=str(episode_payload["summary"]),
                        reused=True,
                    ),
                    reused=True,
                )

        # Validate authority only when a mutation is still required.  A durably
        # completed item remains replayable even if the role later changes;
        # an interrupted chain resumes only while the assignment is still valid.
        assignment = self._active_assignment(signal.principal_id, signal.role_id, signal.responsibility_id)
        if existing is None:
            envelope = EventEnvelope(
                event_id=event_id,
                schema_version="event-envelope/v1",
                source_ref=signal.source_uri,
                emitted_at=occurred_at,
                payload={
                    "source_id": signal.source_id,
                    "item_id": signal.item_id,
                    "title": signal.title,
                    "content_sha256": signal.content_sha256,
                    "source_uri": signal.source_uri,
                },
                trace_id=_deterministic("trace_", "local-signal", source_key),
            )
            generated_signal = Signal(
                signal_id=signal_id,
                schema_version="signal/v1",
                source_event_ref=envelope,
                detected_at=occurred_at,
                pattern="local_markdown_observed",
                confidence=1.0,
            )
            self._broker.append(
                EVT_SIGNAL_OBSERVED,
                producer=PERSONAL_EPISODE_PRODUCER,
                principal_id=signal.principal_id,
                space_id=PERSONAL_EPISODE_SPACE_ID,
                correlation_id=f"personal-signal|{event_id}",
                idempotency_key=f"local-signal|{source_key}",
                event_id=event_id,
                episode_id=episode_id,
                role_context_id=signal.role_id,
                responsibility_id=signal.responsibility_id,
                privacy_class="private",
                payload={
                    "source_key": source_key,
                    "source_id": signal.source_id,
                    "item_id": signal.item_id,
                    "title": signal.title,
                    "content_sha256": signal.content_sha256,
                    "source_uri": signal.source_uri,
                    "signal_id": signal_id,
                    "event_envelope": envelope.model_dump(mode="json"),
                    "signal": generated_signal.model_dump(mode="json"),
                },
                occurred_at=occurred_at,
            )
        decision_payload = {
            "episode_id": episode_id,
            "request_id": request_id,
            "summary": signal.title,
            "why_now": "A private local item was observed",
            "deadline": None,
            "risk": RISK,
            "authority": "human_confirmation_required",
            "status": "pending_confirmation",
            "executor_id": signal.executor_id,
            "role_id": signal.role_id,
            "responsibility_id": signal.responsibility_id,
            "source_signal_ref": event_id,
            "scene_id": PERSONAL_SIGNAL_SCENE_ID,
            "journey_id": PERSONAL_SIGNAL_JOURNEY_ID,
            "outcome_metric": PERSONAL_SIGNAL_OUTCOME_METRIC,
        }
        decision_event_id = self._ensure_context_and_decision_chain(
            episode_id=episode_id,
            principal_id=signal.principal_id,
            role_id=signal.role_id,
            responsibility_id=signal.responsibility_id,
            assignment=assignment,
            decision_payload=decision_payload,
            causation_id=event_id,
        )
        self._broker.append(
            EVT_EPISODE_DECISION,
            producer=PERSONAL_EPISODE_PRODUCER,
            principal_id=signal.principal_id,
            space_id=PERSONAL_EPISODE_SPACE_ID,
            correlation_id=f"personal-episode|{episode_id}",
            idempotency_key=f"local-signal-episode|{source_key}",
            episode_id=episode_id,
            role_context_id=signal.role_id,
            responsibility_id=signal.responsibility_id,
            causation_id=decision_event_id,
            privacy_class="private",
            payload=decision_payload,
            occurred_at=occurred_at,
        )
        return PersonalSignalIngestResult(
            signal_event_id=event_id,
            signal_id=signal_id,
            episode=PersonalEpisodeCard(
                episode_id=episode_id,
                request_id=request_id,
                summary=signal.title,
                reused=existing is not None,
            ),
            reused=existing is not None,
        )

    def confirm(
        self,
        *,
        episode_id: str,
        principal_id: str,
        executor_id: str,
        human_confirmed: bool,
    ) -> PersonalEpisodeConfirmation:
        """Grant one revocable A2/R0 local-draft mandate after human confirmation."""
        if human_confirmed is not True:
            raise PersonalEpisodeError("human_confirmation_required", "human_confirmed must be true")
        start_row = self._start_for_episode(episode_id, principal_id)
        payload = _payload(start_row)
        if payload.get("executor_id") != executor_id:
            raise PersonalEpisodeError("executor_mismatch", "executor does not match episode")
        role_id = _required_payload(payload, "role_id")
        responsibility_id = _required_payload(payload, "responsibility_id")
        assignment = self._active_assignment(principal_id, role_id, responsibility_id)
        mandate_id = _deterministic("mandate:personal-", episode_id)
        manager = MandateManager(self._broker, clock=self._clock)
        current = manager.get(mandate_id, principal_id)
        if current is not None and current.status != STATUS_ACTIVE:
            raise PersonalEpisodeError("mandate_not_active", "episode mandate is revoked")

        decision_row = self._canonical_event(episode_id, EVT_DECISION_PROPOSED)
        human_row: Mapping[str, Any] | None = None
        if decision_row is not None:
            human_row = self._append_chain_event(
                event_type=EVT_HUMAN_ADJUDICATION,
                episode_id=episode_id,
                principal_id=principal_id,
                role_id=role_id,
                responsibility_id=responsibility_id,
                mandate_id=mandate_id,
                causation_id=str(decision_row["event_id"]),
                payload={
                    "decision_event_id": str(decision_row["event_id"]),
                    "verdict": "authorize",
                    "executor_id": executor_id,
                },
            )

        if current is not None:
            self._ensure_canonical_mandate(
                episode_id=episode_id,
                principal_id=principal_id,
                role_id=role_id,
                responsibility_id=responsibility_id,
                mandate_id=mandate_id,
                human_row=human_row,
            )
            return PersonalEpisodeConfirmation(episode_id, mandate_id, reused=True)

        responsibility = next(item for item in assignment.responsibilities if item.resp_id == responsibility_id)
        now = self._clock_datetime()
        mandate = DelegationMandate(
            mandate_id=mandate_id,
            schema_version="delegation-mandate/v1",
            principal_id=principal_id,
            executor_id=executor_id,
            episode_id=episode_id,
            role_context_id=role_id,
            role_assignment_id=assignment.assignment_id,
            role_assignment_version=assignment.version,
            responsibility_id=responsibility_id,
            responsibility_version=responsibility.version,
            purpose="Create one local follow-up draft after human confirmation",
            capability_scope=[CAPABILITY],
            autonomy_level="A2",
            risk_ceiling=RISK,
            approval_mode="matrix",
            disclosure_policy=DISCLOSURE_POLICY,
            valid_from=now - timedelta(seconds=1),
            expires_at=now + timedelta(days=1),
            budget_limit=1.0,
            budget_unit="call",
            revocable=True,
            trace_id=_deterministic("trace_", episode_id, executor_id),
            mandate_version=1,
            status=STATUS_ACTIVE,
        )
        try:
            manager.grant(mandate)
        except MandateError as exc:
            raise PersonalEpisodeError("mandate_grant_failed", str(exc)) from exc
        self._ensure_canonical_mandate(
            episode_id=episode_id,
            principal_id=principal_id,
            role_id=role_id,
            responsibility_id=responsibility_id,
            mandate_id=mandate_id,
            human_row=human_row,
        )
        return PersonalEpisodeConfirmation(episode_id, mandate_id)

    def reload_execution_context(self, episode_id: str, principal_id: str) -> PersonalExecutionContext:
        """Rebuild the PEP envelope exclusively from persisted ledger events."""
        start_row = self._start_for_episode(episode_id, principal_id)
        payload = _payload(start_row)
        executor_id = _required_payload(payload, "executor_id")
        role_id = _required_payload(payload, "role_id")
        responsibility_id = _required_payload(payload, "responsibility_id")
        mandate_id = _deterministic("mandate:personal-", episode_id)
        mandate = MandateManager(self._broker, clock=self._clock).get(mandate_id, principal_id)
        if mandate is None or mandate.status != STATUS_ACTIVE:
            raise PersonalEpisodeError("episode_not_confirmed", "episode has no active mandate")
        principal_authority_ref = principal_receipt_digest = credential_ref = None
        if self._principal_authority is not None and self._default_credential_ref is not None:
            try:
                from omo.sovereignty.principal_authority import digest_receipt

                receipt = self._principal_authority.verify(
                    principal_id,
                    self._default_credential_ref,
                    now=self._clock(),
                )
                principal_authority_ref = receipt.authority_ref
                principal_receipt_digest = digest_receipt(receipt)
                credential_ref = self._default_credential_ref
            except Exception:
                pass
        return PersonalExecutionContext(
            episode_id=episode_id,
            mandate_id=mandate_id,
            principal_id=principal_id,
            executor_id=executor_id,
            role_context_id=role_id,
            responsibility_id=responsibility_id,
            action_id=_deterministic("action:personal-", episode_id),
            trace_id=mandate.trace_id,
            principal_authority_ref=principal_authority_ref,
            principal_receipt_digest=principal_receipt_digest,
            credential_ref=credential_ref,
        )

    def get_draft_snapshot(self, episode_id: str, principal_id: str) -> EpisodeDraftSnapshot:
        """Return safe persisted Episode fields for a deterministic local draft.

        Reads only the ``Episode.Decision.v1`` event from the ledger.
        The snapshot contains identity and draft fields (summary,
        why_now, deadline) but never raw signal body, filesystem paths,
        or source URIs.  Unlike :meth:`reload_execution_context`, this
        method does **not** require an active mandate — the snapshot is
        available as soon as the episode decision is persisted.
        """
        self._required("episode_id", episode_id)
        self._required("principal_id", principal_id)
        start_row = self._start_for_episode(episode_id, principal_id)
        payload = _payload(start_row)
        return EpisodeDraftSnapshot(
            episode_id=str(payload.get("episode_id", episode_id)),
            request_id=_required_payload(payload, "request_id"),
            summary=_required_payload(payload, "summary"),
            why_now=payload.get("why_now"),
            deadline=payload.get("deadline"),
        )

    def record_evidence(
        self,
        context: PersonalExecutionContext,
        evidence_uri: str,
        *,
        output_origin: str = "unknown",
    ) -> int:
        """Record the server-created local-draft artifact in the same episode.

        ``output_origin`` is a controlled vocabulary: ``system`` or
        ``user_provided``.  Legacy or omitted values persist as ``unknown``.
        """
        self._required("evidence_uri", evidence_uri)
        _personal_draft_digest(evidence_uri)
        if output_origin not in VALID_OUTPUT_ORIGINS:
            raise PersonalEpisodeError(
                "invalid_output_origin",
                "output_origin must be system/user_provided/unknown",
            )
        existing = self._find_event(context.episode_id, EVT_EVIDENCE_LOCAL_DRAFT, "evidence_uri", evidence_uri)
        if existing is None:
            sequence = self._broker.append(
                EVT_EVIDENCE_LOCAL_DRAFT,
                producer=PERSONAL_EPISODE_PRODUCER,
                principal_id=context.principal_id,
                space_id=PERSONAL_EPISODE_SPACE_ID,
                correlation_id=f"personal-episode|{context.episode_id}",
                idempotency_key=f"evidence|{context.episode_id}|{_short_hash(evidence_uri)}",
                episode_id=context.episode_id,
                role_context_id=context.role_context_id,
                responsibility_id=context.responsibility_id,
                mandate_id=context.mandate_id,
                payload={
                    "evidence_uri": evidence_uri,
                    "action_id": context.action_id,
                    "output_origin": output_origin,
                },
                evidence_uri=evidence_uri,
                occurred_at=self._clock_ts(),
            )
            existing = self._row_at(sequence)
        sequence = int(existing["sequence"])

        mandate_row = self._canonical_event(context.episode_id, EVT_MANDATE_GRANTED_CANONICAL)
        action_authority = self._action_succeeded_authority(context)
        if mandate_row is None or action_authority is None:
            return sequence
        action_row = self._append_chain_event(
            event_type=EVT_ACTION_SUCCEEDED_CANONICAL,
            episode_id=context.episode_id,
            principal_id=context.principal_id,
            role_id=context.role_context_id,
            responsibility_id=context.responsibility_id,
            mandate_id=context.mandate_id,
            causation_id=str(mandate_row["event_id"]),
            payload={
                "action_id": context.action_id,
                "authority_event_id": str(action_authority["event_id"]),
            },
        )
        self._append_chain_event(
            event_type=EVT_EVIDENCE_RECORDED,
            episode_id=context.episode_id,
            principal_id=context.principal_id,
            role_id=context.role_context_id,
            responsibility_id=context.responsibility_id,
            mandate_id=context.mandate_id,
            causation_id=str(action_row["event_id"]),
            identity=_short_hash(evidence_uri),
            payload={
                "evidence_uri": evidence_uri,
                "action_id": context.action_id,
                "output_origin": output_origin,
                "legacy_evidence_event_id": str(existing["event_id"]),
            },
            evidence_uri=evidence_uri,
        )
        return sequence

    def record_outcome(
        self,
        context: PersonalExecutionContext,
        verdict: str,
        *,
        feedback_id: str | None = None,
        review_duration_seconds: float | None = None,
        estimated_time_saved_seconds: float | None = None,
        revision_digest: str | None = None,
        changed_fields: list[str] | tuple[str, ...] | None = None,
    ) -> int:
        """Record one human feedback outcome using the closed vocabulary.

        ``verdict`` is limited to accept/edit/reject/defer/ignore.
        ``feedback_id`` identifies one caller request so its replay is
        idempotent while later feedback may append, even with the same verdict.
        Omitting it preserves the legacy verdict-based idempotency contract.
        ``review_duration_seconds`` and ``estimated_time_saved_seconds``
        are optional explicit non-negative finite values; omitted stays
        null.
        """
        if verdict not in _OUTCOMES:
            raise PersonalEpisodeError(
                "invalid_outcome_verdict",
                "verdict must be accept/edit/reject/defer/ignore",
            )
        _validate_burden("review_duration_seconds", review_duration_seconds)
        _validate_burden("estimated_time_saved_seconds", estimated_time_saved_seconds)
        candidate_ref = self._latest_evidence_ref(context)
        candidate_digest = _personal_draft_digest(candidate_ref)
        normalized_fields = _revision_fields(changed_fields)
        if verdict == "edit":
            if revision_digest is None or not normalized_fields:
                raise PersonalEpisodeError(
                    "revision_receipt_required",
                    "edit requires revision_digest and changed_fields",
                )
            normalized_revision_digest = _sha256_digest(revision_digest)
            if normalized_revision_digest == candidate_digest:
                raise PersonalEpisodeError(
                    "invalid_revision_receipt",
                    "edit revision_digest must differ from the candidate digest",
                )
        else:
            if normalized_fields:
                raise PersonalEpisodeError(
                    "invalid_revision_receipt",
                    "changed_fields are only valid for an edit verdict",
                )
            if revision_digest is not None and _sha256_digest(revision_digest) != candidate_digest:
                raise PersonalEpisodeError(
                    "invalid_revision_receipt",
                    "non-edit revision_digest must match the candidate digest",
                )
            normalized_revision_digest = candidate_digest
        persisted_feedback_id: str | None
        if feedback_id is None:
            persisted_feedback_id = None
        else:
            persisted_feedback_id = _feedback_ref(feedback_id)
        outcome_payload = {
            "outcome_feedback_schema": OUTCOME_FEEDBACK_SCHEMA,
            "feedback_id": persisted_feedback_id,
            "verdict": verdict,
            "action_id": context.action_id,
            "review_duration_seconds": review_duration_seconds,
            "estimated_time_saved_seconds": estimated_time_saved_seconds,
            "revision_receipt": {
                "schema": REVISION_RECEIPT_SCHEMA,
                "candidate_ref": candidate_ref,
                "revision_digest": normalized_revision_digest,
                "changed_fields": normalized_fields,
            },
        }
        if feedback_id is None:
            identity_key = verdict
            existing = self._find_event(context.episode_id, EVT_OUTCOME_HUMAN, "verdict", verdict)
        else:
            feedback_ref = _feedback_ref(feedback_id)
            identity_key = f"feedback|{_short_hash(feedback_ref)}"
            existing = self._find_event(
                context.episode_id,
                EVT_OUTCOME_HUMAN,
                "feedback_id",
                feedback_ref,
            )
            if existing is None:
                existing = self._find_event(
                    context.episode_id,
                    EVT_OUTCOME_HUMAN,
                    "feedback_id",
                    feedback_id,
                )
        if existing is not None:
            if not _outcome_replay_matches(
                _payload(existing),
                outcome_payload,
                raw_feedback_id=feedback_id,
            ):
                raise PersonalEpisodeError(
                    "feedback_replay_conflict",
                    "feedback replay does not match the recorded outcome",
                )
            sequence = int(existing["sequence"])
        else:
            sequence = self._broker.append(
                EVT_OUTCOME_HUMAN,
                producer=PERSONAL_EPISODE_PRODUCER,
                principal_id=context.principal_id,
                space_id=PERSONAL_EPISODE_SPACE_ID,
                correlation_id=f"personal-episode|{context.episode_id}",
                idempotency_key=f"outcome|{context.episode_id}|{identity_key}",
                episode_id=context.episode_id,
                role_context_id=context.role_context_id,
                responsibility_id=context.responsibility_id,
                mandate_id=context.mandate_id,
                payload=outcome_payload,
                occurred_at=self._clock_ts(),
            )
            existing = self._row_at(sequence)

        evidence_row = self._latest_canonical_evidence(context.episode_id)
        mandate_row = self._canonical_event(context.episode_id, EVT_MANDATE_GRANTED_CANONICAL)
        if evidence_row is None or mandate_row is None:
            return sequence

        chain_identity = _short_hash(identity_key)
        outcome_row = self._append_chain_event(
            event_type=EVT_OUTCOME_OBSERVED,
            episode_id=context.episode_id,
            principal_id=context.principal_id,
            role_id=context.role_context_id,
            responsibility_id=context.responsibility_id,
            mandate_id=context.mandate_id,
            causation_id=str(evidence_row["event_id"]),
            identity=chain_identity,
            payload={**outcome_payload, "legacy_outcome_event_id": str(existing["event_id"])},
        )
        adjudication_row = self._append_chain_event(
            event_type=EVT_ADJUDICATION_RECORDED,
            episode_id=context.episode_id,
            principal_id=context.principal_id,
            role_id=context.role_context_id,
            responsibility_id=context.responsibility_id,
            mandate_id=context.mandate_id,
            causation_id=str(outcome_row["event_id"]),
            identity=chain_identity,
            payload={
                "outcome_event_id": str(outcome_row["event_id"]),
                "verdict": verdict,
                "feedback_id": persisted_feedback_id,
            },
        )
        memory_row = self._append_chain_event(
            event_type=EVT_MEMORY_CANDIDATE_PROPOSED,
            episode_id=context.episode_id,
            principal_id=context.principal_id,
            role_id=context.role_context_id,
            responsibility_id=context.responsibility_id,
            mandate_id=context.mandate_id,
            causation_id=str(adjudication_row["event_id"]),
            identity=chain_identity,
            payload={
                "adjudication_event_id": str(adjudication_row["event_id"]),
                "candidate_kind": "human_revision_learning",
                "verdict": verdict,
            },
        )
        self._append_chain_event(
            event_type=EVT_EPISODE_CLOSED,
            episode_id=context.episode_id,
            principal_id=context.principal_id,
            role_id=context.role_context_id,
            responsibility_id=context.responsibility_id,
            mandate_id=context.mandate_id,
            causation_id=str(memory_row["event_id"]),
            identity=chain_identity,
            payload={
                "chain_schema_version": CHAIN_SCHEMA_VERSION,
                "terminal_verdict": verdict,
                "mandate_event_id": str(mandate_row["event_id"]),
                "evidence_event_id": str(evidence_row["event_id"]),
                "outcome_event_id": str(outcome_row["event_id"]),
                "adjudication_event_id": str(adjudication_row["event_id"]),
                "memory_candidate_event_id": str(memory_row["event_id"]),
            },
        )
        return sequence

    def _latest_evidence_ref(self, context: PersonalExecutionContext) -> str:
        evidence_rows = [
            row
            for row in self._broker.read(episode_id=context.episode_id)
            if row.get("event_type") == EVT_EVIDENCE_LOCAL_DRAFT and row.get("principal_id") == context.principal_id
        ]
        if not evidence_rows:
            raise PersonalEpisodeError(
                "revision_receipt_required",
                "human outcome requires a recorded never-send candidate",
            )
        evidence_rows.sort(key=lambda row: int(row.get("sequence", 0)))
        return _required_payload(_payload(evidence_rows[-1]), "evidence_uri")

    def _ensure_context_and_decision_chain(
        self,
        *,
        episode_id: str,
        principal_id: str,
        role_id: str,
        responsibility_id: str,
        assignment: Any,
        decision_payload: Mapping[str, Any],
        causation_id: str | None,
    ) -> str:
        role_row = self._append_chain_event(
            event_type=EVT_ROLE_CONTEXT_ASSIGNED,
            episode_id=episode_id,
            principal_id=principal_id,
            role_id=role_id,
            responsibility_id=responsibility_id,
            causation_id=causation_id,
            payload={
                "role_id": role_id,
                "role_assignment_id": assignment.assignment_id,
                "role_assignment_version": assignment.version,
            },
        )
        responsibility = next(item for item in assignment.responsibilities if item.resp_id == responsibility_id)
        responsibility_row = self._append_chain_event(
            event_type=EVT_RESPONSIBILITY_LINKED,
            episode_id=episode_id,
            principal_id=principal_id,
            role_id=role_id,
            responsibility_id=responsibility_id,
            causation_id=str(role_row["event_id"]),
            payload={
                "responsibility_id": responsibility_id,
                "responsibility_version": responsibility.version,
                "role_event_id": str(role_row["event_id"]),
            },
        )
        decision_row = self._append_chain_event(
            event_type=EVT_DECISION_PROPOSED,
            episode_id=episode_id,
            principal_id=principal_id,
            role_id=role_id,
            responsibility_id=responsibility_id,
            causation_id=str(responsibility_row["event_id"]),
            payload={**dict(decision_payload), "responsibility_event_id": str(responsibility_row["event_id"])},
        )
        return str(decision_row["event_id"])

    def _ensure_canonical_mandate(
        self,
        *,
        episode_id: str,
        principal_id: str,
        role_id: str,
        responsibility_id: str,
        mandate_id: str,
        human_row: Mapping[str, Any] | None,
    ) -> None:
        if human_row is None:
            return
        authority_rows = [
            row
            for row in self._broker.read(episode_id=episode_id, producer=MANDATE_PRODUCER)
            if row.get("event_type") == EVT_MANDATE_GRANT
            and row.get("principal_id") == principal_id
            and row.get("mandate_id") == mandate_id
        ]
        if not authority_rows:
            return
        authority_row = min(authority_rows, key=lambda row: int(row.get("sequence", 0)))
        if int(authority_row["sequence"]) <= int(human_row["sequence"]):
            return
        self._append_chain_event(
            event_type=EVT_MANDATE_GRANTED_CANONICAL,
            episode_id=episode_id,
            principal_id=principal_id,
            role_id=role_id,
            responsibility_id=responsibility_id,
            mandate_id=mandate_id,
            causation_id=str(human_row["event_id"]),
            payload={
                "mandate_id": mandate_id,
                "authority_event_id": str(authority_row["event_id"]),
                "revocable": True,
            },
        )

    def _action_succeeded_authority(self, context: PersonalExecutionContext) -> Mapping[str, Any] | None:
        rows = [
            row
            for row in self._broker.read(episode_id=context.episode_id, producer=PDP_PRODUCER)
            if row.get("event_type") == EVT_ACTION_SUCCEEDED
            and row.get("principal_id") == context.principal_id
            and row.get("mandate_id") == context.mandate_id
            and _payload(row).get("action_id") == context.action_id
        ]
        if not rows:
            return None
        return min(rows, key=lambda row: int(row.get("sequence", 0)))

    def _latest_canonical_evidence(self, episode_id: str) -> Mapping[str, Any] | None:
        rows = [
            row
            for row in self._broker.read(episode_id=episode_id, producer=PERSONAL_EPISODE_PRODUCER)
            if row.get("event_type") == EVT_EVIDENCE_RECORDED
        ]
        if not rows:
            return None
        return max(rows, key=lambda row: int(row.get("sequence", 0)))

    def _canonical_event(self, episode_id: str, event_type: str) -> Mapping[str, Any] | None:
        rows = [
            row
            for row in self._broker.read(episode_id=episode_id, producer=PERSONAL_EPISODE_PRODUCER)
            if row.get("event_type") == event_type
        ]
        if not rows:
            return None
        return min(rows, key=lambda row: int(row.get("sequence", 0)))

    def _append_chain_event(
        self,
        *,
        event_type: str,
        episode_id: str,
        principal_id: str,
        role_id: str,
        responsibility_id: str,
        causation_id: str | None,
        payload: Mapping[str, Any],
        mandate_id: str | None = None,
        identity: str = "primary",
        evidence_uri: str | None = None,
    ) -> Mapping[str, Any]:
        event_id = _deterministic("evt_", CHAIN_SCHEMA_VERSION, episode_id, event_type, identity)
        existing = next(
            (row for row in self._broker.read(episode_id=episode_id) if row.get("event_id") == event_id), None
        )
        expected_payload = dict(payload)
        if existing is not None:
            if (
                existing.get("event_type") != event_type
                or existing.get("principal_id") != principal_id
                or existing.get("causation_id") != causation_id
                or _payload(existing) != expected_payload
            ):
                raise PersonalEpisodeError(
                    "chain_replay_conflict",
                    f"canonical event replay conflict for {event_type}",
                )
            return existing
        sequence = self._broker.append(
            event_type,
            producer=PERSONAL_EPISODE_PRODUCER,
            principal_id=principal_id,
            space_id=PERSONAL_EPISODE_SPACE_ID,
            correlation_id=f"personal-episode|{episode_id}|{CHAIN_SCHEMA_VERSION}",
            idempotency_key=f"chain|{CHAIN_SCHEMA_VERSION}|{episode_id}|{event_type}|{identity}",
            event_id=event_id,
            episode_id=episode_id,
            role_context_id=role_id,
            responsibility_id=responsibility_id,
            mandate_id=mandate_id,
            causation_id=causation_id,
            privacy_class="private",
            payload=expected_payload,
            evidence_uri=evidence_uri,
            occurred_at=self._clock_ts(),
        )
        return self._row_at(sequence)

    def _row_at(self, sequence: int) -> Mapping[str, Any]:
        rows = self._broker.read(from_sequence=sequence, to_sequence=sequence)
        if len(rows) != 1:
            raise PersonalEpisodeError("ledger_readback_failed", "appended event could not be read back")
        return rows[0]

    def observe_principal(self, principal_id: str) -> PrincipalObservation:
        """Deterministic, read-only per-principal observation over the same Ledger.

        Computes verdict distribution, evidence origin counts, signal-to-
        verdict latency, natural-week samples, and a strict readiness gate:
        at least 30 qualifying episodes plus four consecutive weeks with at
        least three qualifying episodes each.  Never exposes raw body, path,
        source_uri, or digest.  Leaves count and hash chain unchanged — only
        calls ``broker.read()``.
        """
        self._required("principal_id", principal_id)

        all_rows = self._broker.read()
        rows = [
            row
            for row in all_rows
            if row.get("producer") == PERSONAL_EPISODE_PRODUCER and row.get("principal_id") == principal_id
        ]
        decision_rows = [row for row in rows if row.get("event_type") == EVT_EPISODE_DECISION]
        if not decision_rows:
            return PrincipalObservation(
                principal_id=principal_id,
                readiness="not_ready",
                total_episodes=0,
                qualifying_episodes=0,
                qualifying_target=QUALIFYING_EPISODE_TARGET,
                remaining_to_target=QUALIFYING_EPISODE_TARGET,
                verdict_distribution={},
                system_evidence_count=0,
                user_evidence_count=0,
                unknown_evidence_count=0,
                signal_to_verdict_latency_seconds=None,
                weekly_samples=[],
                gate_gaps=["no episodes observed"],
                chain_schema_version=CHAIN_SCHEMA_VERSION,
                qualifying_episode_ids=[],
                episode_gaps={},
            )

        chain_integrity_ok = bool(self._broker.verify_chain().get("ok"))
        observations = [
            _canonical_chain_observation(
                episode_id=str(row["episode_id"]),
                principal_id=principal_id,
                all_rows=all_rows,
                chain_integrity_ok=chain_integrity_ok,
            )
            for row in decision_rows
        ]

        return _build_observation(principal_id, observations)

    def _active_assignment(self, principal_id: str, role_id: str, responsibility_id: str):
        try:
            assignment = SovereigntyService(self._broker).current_assignment(principal_id, role_id)
        except SovereigntyError as exc:
            raise PersonalEpisodeError("role_not_active", str(exc)) from exc
        if assignment is None or assignment.status != STATUS_ACTIVE:
            raise PersonalEpisodeError("role_not_active", "role assignment is not active")
        if not any(item.resp_id == responsibility_id for item in assignment.responsibilities):
            raise PersonalEpisodeError(
                "responsibility_not_active",
                "responsibility is not assigned to active role",
            )
        return assignment

    def _validate_local_signal(self, signal: PersonalLocalSignal) -> None:
        if not isinstance(signal, PersonalLocalSignal):
            raise PersonalEpisodeError("invalid_signal_descriptor", "signal must be a PersonalLocalSignal")
        for name in (
            "source_id",
            "item_id",
            "title",
            "content_sha256",
            "source_uri",
            "principal_id",
            "role_id",
            "responsibility_id",
            "executor_id",
        ):
            self._required(name, getattr(signal, name))
        if "\n" in signal.title or "\r" in signal.title or len(signal.title) > 240:
            raise PersonalEpisodeError(
                "invalid_signal_title",
                "title must be one line and at most 240 characters",
            )
        if len(signal.content_sha256) != 64 or any(
            char not in "0123456789abcdef" for char in signal.content_sha256.lower()
        ):
            raise PersonalEpisodeError("invalid_signal_digest", "content_sha256 must be a SHA-256 hex digest")
        if not all(char.isalnum() or char in "._:-" for char in signal.source_id):
            raise PersonalEpisodeError("invalid_signal_source", "source_id must be a stable source identifier")
        source_prefix = "iris://local-files/"
        source_item_id = (
            signal.source_uri.removeprefix(source_prefix) if signal.source_uri.startswith(source_prefix) else ""
        )
        if (
            not source_item_id
            or source_item_id != signal.item_id
            or any(
                char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-="
                for char in source_item_id
            )
        ):
            raise PersonalEpisodeError(
                "invalid_signal_source",
                "source_uri must be a safe Iris local-files URI",
            )

    def _find_start(self, principal_id: str, request_id: str) -> Mapping[str, Any] | None:
        for row in self._broker.read(producer=PERSONAL_EPISODE_PRODUCER):
            if row.get("event_type") != EVT_EPISODE_DECISION or row.get("principal_id") != principal_id:
                continue
            if _payload(row).get("request_id") == request_id:
                return row
        return None

    def _find_local_signal(self, principal_id: str, source_key: str) -> Mapping[str, Any] | None:
        for row in self._broker.read(producer=PERSONAL_EPISODE_PRODUCER):
            if row.get("event_type") != EVT_SIGNAL_OBSERVED:
                continue
            if row.get("principal_id") != principal_id:
                continue
            if _payload(row).get("source_key") == source_key:
                return row
        return None

    def _episode_for_signal(self, signal_event_id: str, principal_id: str) -> Mapping[str, Any]:
        for row in self._broker.read(producer=PERSONAL_EPISODE_PRODUCER):
            if row.get("event_type") != EVT_EPISODE_DECISION:
                continue
            if row.get("principal_id") != principal_id:
                continue
            if row.get("causation_id") == signal_event_id:
                return row
            if _payload(row).get("source_signal_ref") == signal_event_id:
                return row
        raise PersonalEpisodeError("malformed_signal", "local signal has no causal episode decision")

    def _start_for_episode(self, episode_id: str, principal_id: str) -> Mapping[str, Any]:
        for row in self._broker.read(episode_id=episode_id):
            if row.get("event_type") == EVT_EPISODE_DECISION and row.get("principal_id") == principal_id:
                return row
        raise PersonalEpisodeError("episode_not_found", "personal episode decision was not found")

    def _find_event(
        self, episode_id: str, event_type: str, payload_key: str, payload_value: str
    ) -> Mapping[str, Any] | None:
        for row in self._broker.read(episode_id=episode_id):
            if row.get("event_type") == event_type and _payload(row).get(payload_key) == payload_value:
                return row
        return None

    def _clock_ts(self) -> str:
        return self._clock_datetime().isoformat()

    def _clock_datetime(self) -> datetime:
        try:
            value = datetime.fromisoformat(self._clock())
        except (TypeError, ValueError) as exc:
            raise PersonalEpisodeError("invalid_clock", "clock must return ISO-8601") from exc
        if value.tzinfo is None:
            raise PersonalEpisodeError("invalid_clock", "clock must be timezone-aware")
        return value

    @staticmethod
    def _required(name: str, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise PersonalEpisodeError("invalid_request", f"{name} must be non-empty")


__all__ = [
    "CAPABILITY",
    "CHAIN_SCHEMA_VERSION",
    "DISCLOSURE_POLICY",
    "EVT_ACTION_SUCCEEDED_CANONICAL",
    "EVT_ADJUDICATION_RECORDED",
    "EVT_DECISION_PROPOSED",
    "EVT_EPISODE_DECISION",
    "EVT_EPISODE_CLOSED",
    "EVT_EVIDENCE_LOCAL_DRAFT",
    "EVT_EVIDENCE_RECORDED",
    "EVT_HUMAN_ADJUDICATION",
    "EVT_MANDATE_GRANTED_CANONICAL",
    "EVT_MEMORY_CANDIDATE_PROPOSED",
    "EVT_OUTCOME_HUMAN",
    "EVT_OUTCOME_OBSERVED",
    "EVT_RESPONSIBILITY_LINKED",
    "EVT_ROLE_CONTEXT_ASSIGNED",
    "EVT_SIGNAL_OBSERVED",
    "PERSONAL_EPISODE_PRODUCER",
    "PERSONAL_SIGNAL_JOURNEY_ID",
    "PERSONAL_SIGNAL_OUTCOME_METRIC",
    "PERSONAL_SIGNAL_SCENE_ID",
    "VALID_OUTPUT_ORIGINS",
    "EpisodeDraftSnapshot",
    "PersonalEpisodeCard",
    "PersonalEpisodeConfirmation",
    "PersonalEpisodeError",
    "PersonalEpisodeService",
    "PersonalExecutionContext",
    "PersonalLocalSignal",
    "PersonalSignalIngestResult",
    "PrincipalObservation",
    "WeeklySample",
]
