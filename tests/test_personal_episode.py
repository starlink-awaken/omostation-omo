"""W2-05 personal episode kernel — real-ledger golden-path tests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from ecos.ssot.mof.generated.control.mof_control_models import EventEnvelope, Signal
from omo.episode_projection import build_episode_projection_snapshot
from omo.event_ledger import LedgerBroker
from omo.personal_episode import (
    EVT_EPISODE_DECISION,
    EVT_EVIDENCE_LOCAL_DRAFT,
    EVT_OUTCOME_HUMAN,
    EVT_SIGNAL_OBSERVED,
    PERSONAL_SIGNAL_JOURNEY_ID,
    PERSONAL_SIGNAL_OUTCOME_METRIC,
    PERSONAL_SIGNAL_SCENE_ID,
    VALID_OUTPUT_ORIGINS,
    EpisodeDraftSnapshot,
    PersonalEpisodeCard,
    PersonalEpisodeError,
    PersonalEpisodeService,
    PersonalLocalSignal,
    PrincipalObservation,
    WeeklySample,
)
from omo.sovereignty import REASON_ALLOW, MandateManager, SovereigntyService
from omo.sovereignty.enforcement import EVT_ACTION_SUCCEEDED, PDP_PRODUCER


NOW = "2026-08-12T12:00:00+00:00"


@pytest.fixture()
def broker(tmp_path):
    result = LedgerBroker.connect(tmp_path / "personal-episode.db")
    yield result
    result.close()


@pytest.fixture()
def service(broker):
    return PersonalEpisodeService(broker, clock=lambda: NOW)


def _assign(broker, *, responsibility: str = "follow-up"):
    return SovereigntyService(broker).assign(
        "principal:alice",
        "role:personal-steward",
        role_name="Personal Steward",
        scope="personal",
        responsibilities=[responsibility],
    )


def _start(service, request_id: str = "request-001"):
    return service.start(
        principal_id="principal:alice",
        role_id="role:personal-steward",
        responsibility_id="responsibility:follow-up",
        executor_id="agent:personal-steward",
        request_id=request_id,
        summary="Prepare a local follow-up draft",
        why_now="A commitment needs review",
        deadline="2026-08-13",
    )


def _local_signal(**changes):
    return replace(
        PersonalLocalSignal(
            source_id="iris-local-files",
            item_id="bm90ZXMvZm9sbG93LXVwLm1k",
            title="Follow up with the project team",
            content_sha256="a" * 64,
            source_uri="iris://local-files/bm90ZXMvZm9sbG93LXVwLm1k",
            principal_id="principal:alice",
            role_id="role:personal-steward",
            responsibility_id="responsibility:follow-up",
            executor_id="agent:personal-steward",
        ),
        **changes,
    )


def test_start_requires_active_assignment_with_requested_responsibility(broker, service):
    _assign(broker, responsibility="other-duty")

    with pytest.raises(PersonalEpisodeError) as exc:
        _start(service)

    assert exc.value.reason == "responsibility_not_active"
    assert broker.count() == 1  # only the role assignment


def test_start_is_ledger_backed_idempotent_and_creates_decision_card(broker, service):
    _assign(broker)
    first = _start(service)
    second = PersonalEpisodeService(broker, clock=lambda: NOW).start(
        principal_id="principal:alice",
        role_id="role:personal-steward",
        responsibility_id="responsibility:follow-up",
        executor_id="agent:personal-steward",
        request_id="request-001",
        summary="Prepare a local follow-up draft",
        why_now="A commitment needs review",
        deadline="2026-08-13",
    )

    assert first.episode_id == second.episode_id
    assert first.reused is False
    assert second.reused is True
    rows = broker.read(episode_id=first.episode_id)
    assert [row["event_type"] for row in rows] == ["Episode.Decision.v1"]
    assert rows[0]["payload_json"].find("request-001") >= 0


def test_confirm_requires_human_confirmation(broker, service):
    _assign(broker)
    episode = _start(service)

    with pytest.raises(PersonalEpisodeError) as exc:
        service.confirm(
            episode_id=episode.episode_id,
            principal_id="principal:alice",
            executor_id="agent:personal-steward",
            human_confirmed=False,
        )

    assert exc.value.reason == "human_confirmation_required"
    assert not broker.read(producer="omo-mandate")


def test_confirm_is_idempotent_and_admits_exact_a2_r0_mandate(broker, service):
    assignment = _assign(broker)
    episode = _start(service)
    first = service.confirm(
        episode_id=episode.episode_id,
        principal_id="principal:alice",
        executor_id="agent:personal-steward",
        human_confirmed=True,
    )
    second = PersonalEpisodeService(broker, clock=lambda: NOW).confirm(
        episode_id=episode.episode_id,
        principal_id="principal:alice",
        executor_id="agent:personal-steward",
        human_confirmed=True,
    )

    assert first.mandate_id == second.mandate_id
    assert first.reused is False
    assert second.reused is True
    mandate = MandateManager(broker, clock=lambda: NOW).get(
        first.mandate_id, "principal:alice"
    )
    assert mandate is not None
    assert mandate.autonomy_level == "A2"
    assert mandate.risk_ceiling == "R0"
    assert mandate.capability_scope == ["bos://personal/followup/draft"]
    assert mandate.revocable is True
    assert mandate.budget_limit == 1.0
    assert mandate.role_assignment_id == assignment.assignment_id
    admission = MandateManager(broker, clock=lambda: NOW).admit(
        first.mandate_id,
        "principal:alice",
        "agent:personal-steward",
        episode.episode_id,
        "role:personal-steward",
        "responsibility:follow-up",
        "bos://personal/followup/draft",
        "R0",
        1.0,
        "call",
        "disclosure:private",
    )
    assert admission.allowed is True
    assert admission.reason == REASON_ALLOW


def test_fresh_instance_replays_execution_context_for_pep(broker, service):
    _assign(broker)
    episode = _start(service)
    service.confirm(
        episode_id=episode.episode_id,
        principal_id="principal:alice",
        executor_id="agent:personal-steward",
        human_confirmed=True,
    )

    context = PersonalEpisodeService(broker, clock=lambda: NOW).reload_execution_context(
        episode.episode_id, "principal:alice"
    )

    assert context.episode_id == episode.episode_id
    assert context.mandate_id.startswith("mandate:")
    assert context.omo_policy["requested_risk"] == "R0"
    assert context.omo_policy["capability"] == "bos://personal/followup/draft"
    assert context.omo_policy["requested_budget"] == 1.0


def test_process_restart_replays_complete_pep_context_from_same_ledger(tmp_path):
    db_path = tmp_path / "restart-ledger.db"
    first_broker = LedgerBroker.connect(db_path)
    try:
        first_service = PersonalEpisodeService(first_broker, clock=lambda: NOW)
        _assign(first_broker)
        episode = _start(first_service, request_id="restart-request-001")
        confirmation = first_service.confirm(
            episode_id=episode.episode_id,
            principal_id="principal:alice",
            executor_id="agent:personal-steward",
            human_confirmed=True,
        )
    finally:
        first_broker.close()

    restarted_broker = LedgerBroker.connect(db_path)
    try:
        context = PersonalEpisodeService(
            restarted_broker, clock=lambda: NOW
        ).reload_execution_context(episode.episode_id, "principal:alice")
    finally:
        restarted_broker.close()

    assert context.omo_policy == {
        "action_id": context.action_id,
        "principal_id": "principal:alice",
        "executor_id": "agent:personal-steward",
        "episode_id": episode.episode_id,
        "mandate_id": confirmation.mandate_id,
        "role_context_id": "role:personal-steward",
        "responsibility_id": "responsibility:follow-up",
        "capability": "bos://personal/followup/draft",
        "server_risk": "R0",
        "requested_risk": "R0",
        "requested_budget": 1.0,
        "budget_unit": "call",
        "disclosure_policy": "disclosure:private",
        "trace_id": context.trace_id,
        "mandate_version": 1,
    }


def test_evidence_outcome_and_projection_share_episode_and_hash_chain(broker, service):
    _assign(broker)
    episode = _start(service)
    confirmed = service.confirm(
        episode_id=episode.episode_id,
        principal_id="principal:alice",
        executor_id="agent:personal-steward",
        human_confirmed=True,
    )
    context = service.reload_execution_context(episode.episode_id, "principal:alice")
    service.record_evidence(context, "file:///runtime/omo/personal-drafts/draft.json")
    service.record_outcome(context, "accept")

    with pytest.raises(PersonalEpisodeError) as exc:
        service.record_outcome(context, "maybe")
    assert exc.value.reason == "invalid_outcome_verdict"

    snapshot = build_episode_projection_snapshot(broker, principal_id="principal:alice")
    assert [card["episode"] for card in snapshot["inbox"]] == [episode.episode_id]
    members = snapshot["episodes"][0]["contains_event_refs"]
    assert len(members) == 4  # decision + mandate + evidence + outcome
    assert any(member["payload"].get("evidence_uri") for member in members)
    assert any(member["payload"].get("verdict") == "accept" for member in members)
    assert confirmed.mandate_id == context.mandate_id
    assert broker.verify_chain()["ok"] is True


def test_ingest_local_signal_is_causal_private_and_mof_validated(broker, service):
    _assign(broker)

    result = service.ingest_local_signal(_local_signal())

    rows = broker.read()
    signal_row = next(row for row in rows if row["event_type"] == EVT_SIGNAL_OBSERVED)
    episode_row = next(row for row in rows if row["event_type"] == EVT_EPISODE_DECISION)
    signal_payload = json.loads(signal_row["payload_json"])
    episode_payload = json.loads(episode_row["payload_json"])

    assert result.reused is False
    assert signal_row["episode_id"] is None
    assert signal_row["privacy_class"] == "private"
    assert episode_row["privacy_class"] == "private"
    assert signal_row["event_id"] == result.signal_event_id
    assert episode_row["causation_id"] == result.signal_event_id
    assert episode_payload["source_signal_ref"] == result.signal_event_id
    assert episode_payload["scene_id"] == PERSONAL_SIGNAL_SCENE_ID
    assert episode_payload["journey_id"] == PERSONAL_SIGNAL_JOURNEY_ID
    assert episode_payload["outcome_metric"] == PERSONAL_SIGNAL_OUTCOME_METRIC
    assert signal_payload["signal_id"] == result.signal_id
    assert signal_payload["content_sha256"] == "a" * 64
    assert "Follow up with the project team" in signal_row["payload_json"]
    assert "/Users/" not in signal_row["payload_json"]
    assert "file://" not in signal_row["payload_json"]
    assert EventEnvelope.model_validate(signal_payload["event_envelope"]).event_id == result.signal_event_id
    assert Signal.model_validate(signal_payload["signal"]).signal_id == result.signal_id
    assert broker.verify_chain()["ok"] is True


def test_ingest_local_signal_exact_replay_returns_same_pair(broker, service):
    _assign(broker)
    first = service.ingest_local_signal(_local_signal())
    second = PersonalEpisodeService(broker, clock=lambda: NOW).ingest_local_signal(
        _local_signal()
    )

    assert second.reused is True
    assert second.signal_event_id == first.signal_event_id
    assert second.signal_id == first.signal_id
    assert second.episode.episode_id == first.episode.episode_id
    assert broker.count() == 3  # role assignment + signal + episode decision


def test_ingest_local_signal_replays_original_pair_after_role_changes(broker, service):
    _assign(broker)
    first = service.ingest_local_signal(_local_signal())
    SovereigntyService(broker).assign(
        "principal:alice",
        "role:second-steward",
        role_name="Second Personal Steward",
        scope="personal",
        responsibilities=["other-follow-up"],
    )
    before_count = broker.count()
    before_hash = broker.read()[-1]["event_hash"]

    replay = service.ingest_local_signal(
        _local_signal(
            role_id="role:second-steward",
            responsibility_id="responsibility:other-follow-up",
        )
    )

    assert replay.reused is True
    assert replay.signal_event_id == first.signal_event_id
    assert replay.episode.episode_id == first.episode.episode_id
    assert broker.count() == before_count
    assert broker.read()[-1]["event_hash"] == before_hash
    assert broker.verify_chain()["ok"] is True


def test_ingest_local_signal_changed_digest_creates_a_new_causal_pair(broker, service):
    _assign(broker)
    first = service.ingest_local_signal(_local_signal())
    second = service.ingest_local_signal(_local_signal(content_sha256="b" * 64))

    assert first.signal_event_id != second.signal_event_id
    assert first.episode.episode_id != second.episode.episode_id
    assert len(broker.read(event_type=EVT_SIGNAL_OBSERVED)) == 2
    assert len(broker.read(event_type=EVT_EPISODE_DECISION)) == 2


@pytest.mark.parametrize(
    ("signal", "reason"),
    [
        (_local_signal(role_id="role:missing"), "role_not_active"),
        (
            _local_signal(responsibility_id="responsibility:missing"),
            "responsibility_not_active",
        ),
        (_local_signal(title=""), "invalid_request"),
        (_local_signal(content_sha256="not-a-digest"), "invalid_signal_digest"),
        (_local_signal(source_uri="file:///Users/alice/private.md"), "invalid_signal_source"),
    ],
)
def test_ingest_local_signal_rejects_invalid_input_before_any_append(
    broker, service, signal, reason
):
    _assign(broker)
    before_count = broker.count()
    before_hash = broker.read()[-1]["event_hash"]

    with pytest.raises(PersonalEpisodeError) as exc:
        service.ingest_local_signal(signal)

    assert exc.value.reason == reason
    assert broker.count() == before_count
    assert broker.read()[-1]["event_hash"] == before_hash
    assert broker.verify_chain()["ok"] is True


def test_ingest_local_signal_replays_after_process_restart(tmp_path):
    db_path = tmp_path / "local-signal-restart.db"
    first_broker = LedgerBroker.connect(db_path)
    try:
        _assign(first_broker)
        first = PersonalEpisodeService(first_broker, clock=lambda: NOW).ingest_local_signal(
            _local_signal()
        )
    finally:
        first_broker.close()

    restarted_broker = LedgerBroker.connect(db_path)
    try:
        replay = PersonalEpisodeService(
            restarted_broker, clock=lambda: NOW
        ).ingest_local_signal(_local_signal())
        assert replay.reused is True
        assert replay.signal_event_id == first.signal_event_id
        assert replay.episode.episode_id == first.episode.episode_id
        assert restarted_broker.count() == 3
        assert restarted_broker.verify_chain()["ok"] is True
    finally:
        restarted_broker.close()


# ---------------------------------------------------------------------------
# BET-Y1Q2-T2-02 — EpisodeDraftSnapshot (safe persisted fields for local draft)
# ---------------------------------------------------------------------------


def test_get_draft_snapshot_returns_safe_fields_for_start_episode(broker, service):
    """Snapshot from a start() episode carries summary/why_now/deadline/identity."""
    _assign(broker)
    episode = _start(service)

    snapshot = service.get_draft_snapshot(episode.episode_id, "principal:alice")

    assert isinstance(snapshot, EpisodeDraftSnapshot)
    assert snapshot.episode_id == episode.episode_id
    assert snapshot.request_id == "request-001"
    assert snapshot.summary == "Prepare a local follow-up draft"
    assert snapshot.why_now == "A commitment needs review"
    assert snapshot.deadline == "2026-08-13"


def test_get_draft_snapshot_returns_safe_fields_for_signal_episode(broker, service):
    """Snapshot from an ingest_local_signal() episode carries the same safe fields."""
    _assign(broker)
    result = service.ingest_local_signal(_local_signal())

    snapshot = service.get_draft_snapshot(
        result.episode.episode_id, "principal:alice"
    )

    assert snapshot.episode_id == result.episode.episode_id
    assert snapshot.request_id == result.episode.request_id
    assert snapshot.summary == "Follow up with the project team"
    assert snapshot.why_now == "A private local item was observed"
    assert snapshot.deadline is None


def test_get_draft_snapshot_available_before_confirmation(broker, service):
    """Snapshot is available immediately — no active mandate required."""
    _assign(broker)
    episode = _start(service)

    # reload_execution_context would raise episode_not_confirmed here,
    # but get_draft_snapshot must succeed.
    snapshot = service.get_draft_snapshot(episode.episode_id, "principal:alice")
    assert snapshot.summary == "Prepare a local follow-up draft"


def test_get_draft_snapshot_raises_for_missing_episode(broker, service):
    _assign(broker)

    with pytest.raises(PersonalEpisodeError) as exc:
        service.get_draft_snapshot("episode:nope", "principal:alice")

    assert exc.value.reason == "episode_not_found"


def test_get_draft_snapshot_is_immutable(broker, service):
    """Frozen dataclass: mutation must raise FrozenInstanceError."""
    _assign(broker)
    episode = _start(service)
    snapshot = service.get_draft_snapshot(episode.episode_id, "principal:alice")

    with pytest.raises(AttributeError):
        snapshot.summary = "tampered"  # type: ignore[misc]


def test_get_draft_snapshot_survives_process_restart(tmp_path):
    """Snapshot is deterministic across a process restart from the same ledger."""
    db_path = tmp_path / "draft-snapshot-restart.db"
    first_broker = LedgerBroker.connect(db_path)
    try:
        _assign(first_broker)
        episode = PersonalEpisodeService(
            first_broker, clock=lambda: NOW
        ).start(
            principal_id="principal:alice",
            role_id="role:personal-steward",
            responsibility_id="responsibility:follow-up",
            executor_id="agent:personal-steward",
            request_id="restart-draft-001",
            summary="Draft after restart",
            why_now="Needs attention",
            deadline="2026-09-01",
        )
    finally:
        first_broker.close()

    restarted_broker = LedgerBroker.connect(db_path)
    try:
        snapshot = PersonalEpisodeService(
            restarted_broker, clock=lambda: NOW
        ).get_draft_snapshot(episode.episode_id, "principal:alice")
    finally:
        restarted_broker.close()

    assert snapshot.episode_id == episode.episode_id
    assert snapshot.request_id == "restart-draft-001"
    assert snapshot.summary == "Draft after restart"
    assert snapshot.why_now == "Needs attention"
    assert snapshot.deadline == "2026-09-01"


def test_get_draft_snapshot_to_dict_round_trips_safe_fields(broker, service):
    """to_dict() exposes exactly the safe draft fields, no body/path/uri."""
    _assign(broker)
    episode = _start(service)
    snapshot = service.get_draft_snapshot(episode.episode_id, "principal:alice")

    data = snapshot.to_dict()
    assert set(data.keys()) == {
        "episode_id",
        "request_id",
        "summary",
        "why_now",
        "deadline",
    }
    # Ensure no signal body / path / uri leak into the snapshot.
    assert not hasattr(snapshot, "source_id")
    assert not hasattr(snapshot, "item_id")
    assert not hasattr(snapshot, "source_uri")
    assert not hasattr(snapshot, "content_sha256")


# ---------------------------------------------------------------------------
# BET-Y1Q2-T4-02 — Evidence output_origin, Outcome burden fields, observe_principal
# ---------------------------------------------------------------------------


def _confirmed_context(service, *, request_id="request-001"):
    """Helper: start + confirm + reload context. Assumes _assign already called."""
    episode = _start(service, request_id=request_id)
    service.confirm(
        episode_id=episode.episode_id,
        principal_id="principal:alice",
        executor_id="agent:personal-steward",
        human_confirmed=True,
    )
    return service.reload_execution_context(episode.episode_id, "principal:alice")


# ---- Evidence output_origin ----


def test_record_evidence_persists_system_output_origin(broker, service):
    _assign(broker)
    ctx = _confirmed_context(service)
    service.record_evidence(ctx, "file:///drafts/system.json", output_origin="system")

    rows = broker.read(episode_id=ctx.episode_id, event_type=EVT_EVIDENCE_LOCAL_DRAFT)
    payload = json.loads(rows[0]["payload_json"])
    assert payload["output_origin"] == "system"


def test_record_evidence_persists_user_provided_output_origin(broker, service):
    _assign(broker)
    ctx = _confirmed_context(service)
    service.record_evidence(ctx, "file:///drafts/user.json", output_origin="user_provided")

    rows = broker.read(episode_id=ctx.episode_id, event_type=EVT_EVIDENCE_LOCAL_DRAFT)
    payload = json.loads(rows[0]["payload_json"])
    assert payload["output_origin"] == "user_provided"


def test_record_evidence_defaults_output_origin_to_unknown(broker, service):
    _assign(broker)
    ctx = _confirmed_context(service)
    service.record_evidence(ctx, "file:///drafts/legacy.json")

    rows = broker.read(episode_id=ctx.episode_id, event_type=EVT_EVIDENCE_LOCAL_DRAFT)
    payload = json.loads(rows[0]["payload_json"])
    assert payload["output_origin"] == "unknown"


def test_record_evidence_rejects_invalid_output_origin(broker, service):
    _assign(broker)
    ctx = _confirmed_context(service)

    with pytest.raises(PersonalEpisodeError) as exc:
        service.record_evidence(ctx, "file:///drafts/bad.json", output_origin="hacker")

    assert exc.value.reason == "invalid_output_origin"


def test_record_evidence_idempotent_with_output_origin(broker, service):
    _assign(broker)
    ctx = _confirmed_context(service)
    seq1 = service.record_evidence(ctx, "file:///drafts/dup.json", output_origin="system")
    seq2 = service.record_evidence(ctx, "file:///drafts/dup.json", output_origin="user_provided")

    assert seq1 == seq2
    rows = broker.read(episode_id=ctx.episode_id, event_type=EVT_EVIDENCE_LOCAL_DRAFT)
    assert len(rows) == 1  # only one event, origin stays system


# ---- Outcome ignore + burden fields ----


def test_record_outcome_accepts_ignore_verdict(broker, service):
    _assign(broker)
    ctx = _confirmed_context(service)
    seq = service.record_outcome(ctx, "ignore")

    rows = broker.read(episode_id=ctx.episode_id, event_type=EVT_OUTCOME_HUMAN)
    payload = json.loads(rows[0]["payload_json"])
    assert payload["verdict"] == "ignore"
    assert seq > 0


def test_record_outcome_persists_burden_fields(broker, service):
    _assign(broker)
    ctx = _confirmed_context(service)
    service.record_outcome(
        ctx,
        "accept",
        review_duration_seconds=30,
        estimated_time_saved_seconds=300,
    )

    rows = broker.read(episode_id=ctx.episode_id, event_type=EVT_OUTCOME_HUMAN)
    payload = json.loads(rows[0]["payload_json"])
    assert payload["review_duration_seconds"] == 30
    assert payload["estimated_time_saved_seconds"] == 300


def test_record_outcome_omitted_burden_stays_null(broker, service):
    _assign(broker)
    ctx = _confirmed_context(service)
    service.record_outcome(ctx, "accept")

    rows = broker.read(episode_id=ctx.episode_id, event_type=EVT_OUTCOME_HUMAN)
    payload = json.loads(rows[0]["payload_json"])
    assert payload["review_duration_seconds"] is None
    assert payload["estimated_time_saved_seconds"] is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"review_duration_seconds": -1},
        {"estimated_time_saved_seconds": -0.1},
        {"review_duration_seconds": float("inf")},
        {"estimated_time_saved_seconds": float("nan")},
    ],
)
def test_record_outcome_rejects_invalid_burden(broker, service, kwargs):
    _assign(broker)
    ctx = _confirmed_context(service)

    with pytest.raises(PersonalEpisodeError) as exc:
        service.record_outcome(ctx, "accept", **kwargs)

    assert exc.value.reason == "invalid_burden"


def test_record_outcome_accepts_zero_burden(broker, service):
    _assign(broker)
    ctx = _confirmed_context(service)
    service.record_outcome(
        ctx,
        "accept",
        review_duration_seconds=0,
        estimated_time_saved_seconds=0,
    )

    rows = broker.read(episode_id=ctx.episode_id, event_type=EVT_OUTCOME_HUMAN)
    payload = json.loads(rows[0]["payload_json"])
    assert payload["review_duration_seconds"] == 0
    assert payload["estimated_time_saved_seconds"] == 0


# ---- observe_principal ----


def _make_full_episode(
    broker,
    *,
    clock_ts: str,
    request_id: str,
    output_origin: str = "system",
    verdict: str = "accept",
    review_seconds: float | None = 10,
    saved_seconds: float | None = 100,
    principal: str = "principal:alice",
    signal_sourced: bool = True,
    action_succeeded: bool = True,
):
    """Create a confirmed episode with evidence and outcome.

    When *signal_sourced* is True (default) the episode is created via
    ``ingest_local_signal`` and is gate-eligible.  When False it is a manual
    ``start()`` episode — observable but never gate-qualifying.

    When *action_succeeded* is True (default) an ``Action.Succeeded.v1``
    event is appended to the PDP producer, making the episode gate-eligible.
    """
    svc = PersonalEpisodeService(broker, clock=lambda ts=clock_ts: ts)

    if signal_sourced:
        sha = hashlib.sha256(f"sha-{request_id}".encode()).hexdigest()
        item_id = hashlib.sha256(f"item-{request_id}".encode()).hexdigest()[:24]
        signal = _local_signal(
            content_sha256=sha,
            item_id=item_id,
            source_uri=f"iris://local-files/{item_id}",
            title=f"Signal {request_id}",
            principal_id=principal,
        )
        result = svc.ingest_local_signal(signal)
        episode_id = result.episode.episode_id
    else:
        ep = svc.start(
            principal_id=principal,
            role_id="role:personal-steward",
            responsibility_id="responsibility:follow-up",
            executor_id="agent:personal-steward",
            request_id=request_id,
            summary=f"Episode {request_id}",
        )
        episode_id = ep.episode_id

    svc.confirm(
        episode_id=episode_id,
        principal_id=principal,
        executor_id="agent:personal-steward",
        human_confirmed=True,
    )
    ctx = svc.reload_execution_context(episode_id, principal)
    svc.record_evidence(ctx, f"file:///drafts/{request_id}.json", output_origin=output_origin)
    svc.record_outcome(
        ctx,
        verdict,
        review_duration_seconds=review_seconds,
        estimated_time_saved_seconds=saved_seconds,
    )

    if action_succeeded:
        broker.append(
            EVT_ACTION_SUCCEEDED,
            producer=PDP_PRODUCER,
            principal_id=principal,
            space_id="sovereignty",
            correlation_id=f"action|{ctx.action_id}|succeeded",
            idempotency_key=f"{ctx.action_id}|succeeded",
            episode_id=episode_id,
            payload={
                "action_id": ctx.action_id,
                "episode_id": episode_id,
                "principal_id": principal,
                "status": "succeeded",
            },
            occurred_at=clock_ts,
        )

    return PersonalEpisodeCard(
        episode_id=episode_id,
        request_id=request_id,
        summary=f"Episode {request_id}",
    )


_W1 = "2026-08-03T12:00:00+00:00"  # ISO week 32 Monday
_W2 = "2026-08-10T12:00:00+00:00"  # ISO week 33 Monday
_W3 = "2026-08-17T12:00:00+00:00"  # ISO week 34 Monday
_W4 = "2026-08-24T12:00:00+00:00"  # ISO week 35 Monday


def test_observe_principal_not_ready_when_empty(broker, service):
    obs = service.observe_principal("principal:alice")

    assert isinstance(obs, PrincipalObservation)
    assert obs.readiness == "not_ready"
    assert obs.total_episodes == 0
    assert obs.weekly_samples == []
    assert obs.signal_to_verdict_latency_seconds is None


def test_observe_principal_collecting_with_partial_data(broker, service):
    _assign(broker)
    _make_full_episode(broker, clock_ts=_W1, request_id="w1-1")
    _make_full_episode(broker, clock_ts=_W1, request_id="w1-2")

    obs = service.observe_principal("principal:alice")

    assert obs.readiness == "collecting"
    assert len(obs.weekly_samples) == 1
    assert obs.weekly_samples[0].gate_met is False
    assert "below threshold" in obs.gate_gaps[0] or "qualifying" in obs.gate_gaps[0]


def test_observe_principal_passed_four_consecutive_weeks(broker, service):
    _assign(broker)
    for wi, wk in enumerate([_W1, _W2, _W3, _W4]):
        for ei in range(3):
            _make_full_episode(
                broker,
                clock_ts=wk,
                request_id=f"w{wi}-ep{ei}",
            )

    obs = service.observe_principal("principal:alice")

    assert obs.readiness == "passed"
    assert obs.gate_gaps == []
    assert len(obs.weekly_samples) == 4
    assert all(s.gate_met for s in obs.weekly_samples)
    assert all(s.qualifying_episodes >= 3 for s in obs.weekly_samples)


def test_observe_principal_not_candidate_non_consecutive_weeks(broker, service):
    _assign(broker)
    # Weeks 1 and 3 only — gap in week 2
    for ei in range(3):
        _make_full_episode(broker, clock_ts=_W1, request_id=f"w1-{ei}")
        _make_full_episode(broker, clock_ts=_W3, request_id=f"w3-{ei}")

    obs = service.observe_principal("principal:alice")

    assert obs.readiness == "collecting"
    assert any("consecutive" in g or "gap" in g for g in obs.gate_gaps)


def test_observe_principal_not_candidate_missing_burden(broker, service):
    _assign(broker)
    for ei in range(3):
        _make_full_episode(
            broker,
            clock_ts=_W1,
            request_id=f"noburden-{ei}",
            review_seconds=None,
            saved_seconds=None,
        )

    obs = service.observe_principal("principal:alice")

    assert obs.readiness == "collecting"
    sample = obs.weekly_samples[0]
    assert sample.qualifying_episodes == 0
    assert sample.complete_burden_episodes == 0


def test_observe_principal_not_candidate_review_ge_saved(broker, service):
    _assign(broker)
    for ei in range(3):
        _make_full_episode(
            broker,
            clock_ts=_W1,
            request_id=f"revge-{ei}",
            review_seconds=200,
            saved_seconds=100,
        )

    obs = service.observe_principal("principal:alice")

    sample = obs.weekly_samples[0]
    assert sample.review_lt_saved_episodes == 0
    assert sample.qualifying_episodes == 0


def test_observe_principal_cross_principal_isolation(broker, service):
    _assign(broker)
    # Alice's data
    _make_full_episode(broker, clock_ts=_W1, request_id="alice-1")
    # Assign bob and create bob's data
    SovereigntyService(broker).assign(
        "principal:bob",
        "role:personal-steward",
        role_name="Personal Steward",
        scope="personal",
        responsibilities=["follow-up"],
    )
    _make_full_episode(
        broker,
        clock_ts=_W1,
        request_id="bob-1",
        principal="principal:bob",
    )

    obs_alice = service.observe_principal("principal:alice")
    obs_bob = service.observe_principal("principal:bob")

    assert obs_alice.total_episodes == 1
    assert obs_bob.total_episodes == 1
    # Different episode_ids — no cross-contamination
    alice_eps = {s.week_key for s in obs_alice.weekly_samples}
    bob_eps = {s.week_key for s in obs_bob.weekly_samples}
    assert alice_eps == bob_eps  # same week, but different episodes


def test_observe_principal_verdict_distribution(broker, service):
    _assign(broker)
    ctx1 = _confirmed_context(service, request_id="vd-1")
    service.record_evidence(ctx1, "file:///d/vd-1.json", output_origin="system")
    service.record_outcome(ctx1, "accept")

    ctx2 = _confirmed_context(service, request_id="vd-2")
    service.record_evidence(ctx2, "file:///d/vd-2.json", output_origin="system")
    service.record_outcome(ctx2, "reject")

    ctx3 = _confirmed_context(service, request_id="vd-3")
    service.record_evidence(ctx3, "file:///d/vd-3.json", output_origin="system")
    service.record_outcome(ctx3, "ignore")

    obs = service.observe_principal("principal:alice")

    assert obs.verdict_distribution == {"accept": 1, "reject": 1, "ignore": 1}


def test_observe_principal_evidence_origin_counts(broker, service):
    _assign(broker)
    ctx1 = _confirmed_context(service, request_id="ev-1")
    service.record_evidence(ctx1, "file:///d/ev-1.json", output_origin="system")
    service.record_outcome(ctx1, "accept")

    ctx2 = _confirmed_context(service, request_id="ev-2")
    service.record_evidence(ctx2, "file:///d/ev-2.json", output_origin="user_provided")
    service.record_outcome(ctx2, "accept")

    ctx3 = _confirmed_context(service, request_id="ev-3")
    service.record_evidence(ctx3, "file:///d/ev-3.json")  # default unknown
    service.record_outcome(ctx3, "accept")

    obs = service.observe_principal("principal:alice")

    assert obs.system_evidence_count == 1
    assert obs.user_evidence_count == 1
    assert obs.unknown_evidence_count == 1


def test_observe_principal_signal_to_verdict_latency(broker, service):
    _assign(broker)
    signal_time = "2026-08-12T10:00:00+00:00"
    outcome_time = "2026-08-12T12:00:00+00:00"

    svc_signal = PersonalEpisodeService(broker, clock=lambda: signal_time)
    ep = svc_signal.start(
        principal_id="principal:alice",
        role_id="role:personal-steward",
        responsibility_id="responsibility:follow-up",
        executor_id="agent:personal-steward",
        request_id="latency-1",
        summary="Latency test",
    )
    svc_signal.confirm(
        episode_id=ep.episode_id,
        principal_id="principal:alice",
        executor_id="agent:personal-steward",
        human_confirmed=True,
    )
    ctx = svc_signal.reload_execution_context(ep.episode_id, "principal:alice")
    svc_signal.record_evidence(ctx, "file:///d/lat.json", output_origin="system")

    svc_outcome = PersonalEpisodeService(broker, clock=lambda: outcome_time)
    svc_outcome.record_outcome(ctx, "accept", review_duration_seconds=10, estimated_time_saved_seconds=100)

    obs = service.observe_principal("principal:alice")

    assert obs.signal_to_verdict_latency_seconds is not None
    assert obs.signal_to_verdict_latency_seconds == 7200.0  # 2 hours


def test_observe_principal_read_only_leaves_count_and_hash_unchanged(broker, service):
    _assign(broker)
    _make_full_episode(broker, clock_ts=_W1, request_id="ro-1")

    before_count = broker.count()
    before_rows = broker.read()
    before_last_hash = before_rows[-1]["event_hash"]

    service.observe_principal("principal:alice")

    after_count = broker.count()
    after_rows = broker.read()
    after_last_hash = after_rows[-1]["event_hash"]

    assert after_count == before_count
    assert after_last_hash == before_last_hash


def test_observe_principal_no_raw_leakage_in_output(broker, service):
    _assign(broker)
    result = service.ingest_local_signal(_local_signal())
    episode_id = result.episode.episode_id
    service.confirm(
        episode_id=episode_id,
        principal_id="principal:alice",
        executor_id="agent:personal-steward",
        human_confirmed=True,
    )
    ctx = service.reload_execution_context(episode_id, "principal:alice")
    service.record_evidence(ctx, "file:///drafts/secret.json", output_origin="system")
    service.record_outcome(ctx, "accept", review_duration_seconds=5, estimated_time_saved_seconds=50)

    obs = service.observe_principal("principal:alice")
    data = obs.to_dict()
    serialized = json.dumps(data)

    # No raw body / path / uri / digest in observation output
    assert "file:///drafts/secret.json" not in serialized
    assert "/Users/" not in serialized
    assert "iris://local-files/" not in serialized
    assert "a" * 64 not in serialized
    assert "bm90ZXMv" not in serialized


def test_observe_principal_to_dict_round_trips(broker, service):
    _assign(broker)
    _make_full_episode(broker, clock_ts=_W1, request_id="td-1")

    obs = service.observe_principal("principal:alice")
    data = obs.to_dict()

    assert set(data.keys()) == {
        "principal_id",
        "readiness",
        "total_episodes",
        "verdict_distribution",
        "system_evidence_count",
        "user_evidence_count",
        "unknown_evidence_count",
        "signal_to_verdict_latency_seconds",
        "weekly_samples",
        "gate_gaps",
    }
    assert isinstance(data["weekly_samples"], list)
    if data["weekly_samples"]:
        ws = data["weekly_samples"][0]
        assert isinstance(ws, dict)
        assert "week_key" in ws
        assert "gate_met" in ws
        assert "verdict_distribution" in ws


def test_observe_principal_legacy_evidence_treated_as_unknown(broker, service):
    """Evidence recorded without output_origin (legacy) counts as unknown."""
    _assign(broker)
    ctx = _confirmed_context(service, request_id="legacy-ev")
    # Record evidence without output_origin — simulates legacy data
    service.record_evidence(ctx, "file:///drafts/legacy.json")
    service.record_outcome(ctx, "accept")

    obs = service.observe_principal("principal:alice")

    assert obs.unknown_evidence_count == 1
    assert obs.system_evidence_count == 0


def test_observe_principal_candidate_requires_system_evidence(broker, service):
    """user_provided evidence doesn't count for the gate."""
    _assign(broker)
    for wi, wk in enumerate([_W1, _W2, _W3, _W4]):
        for ei in range(3):
            _make_full_episode(
                broker,
                clock_ts=wk,
                request_id=f"usr-{wi}-{ei}",
                output_origin="user_provided",
            )

    obs = service.observe_principal("principal:alice")

    # user_provided evidence doesn't qualify for the gate
    assert obs.readiness == "collecting"
    assert obs.system_evidence_count == 0


# ---------------------------------------------------------------------------
# Review-blocker regression tests
# ---------------------------------------------------------------------------


def test_gate_requires_action_succeeded(broker, service):
    """Blocker 1: episodes without matching Action.Succeeded never qualify."""
    _assign(broker)
    for ei in range(3):
        _make_full_episode(
            broker,
            clock_ts=_W1,
            request_id=f"no-pdp-{ei}",
            action_succeeded=False,
        )
    obs = service.observe_principal("principal:alice")
    sample = obs.weekly_samples[0]
    assert sample.qualifying_episodes == 0
    assert sample.gate_met is False


def test_gate_requires_signal_source(broker, service):
    """Blocker 2: manually-started episodes never gate-qualify."""
    _assign(broker)
    for ei in range(3):
        _make_full_episode(
            broker,
            clock_ts=_W1,
            request_id=f"manual-{ei}",
            signal_sourced=False,
        )
    obs = service.observe_principal("principal:alice")
    sample = obs.weekly_samples[0]
    assert sample.qualifying_episodes == 0


def test_week_bucket_uses_outcome_time(broker, service):
    """Blocker 3: week bucket is effective outcome occurred_at, not signal time."""
    _assign(broker)
    # Signal + decision at _W1, outcome recorded at _W2 (next ISO week).
    svc = PersonalEpisodeService(broker, clock=lambda: _W1)
    sha = hashlib.sha256(b"wbucket").hexdigest()
    item_id = hashlib.sha256(b"wbucket-item").hexdigest()[:24]
    signal = _local_signal(
        content_sha256=sha,
        item_id=item_id,
        source_uri=f"iris://local-files/{item_id}",
        title="Week-bucket test",
    )
    result = svc.ingest_local_signal(signal)
    episode_id = result.episode.episode_id
    svc.confirm(
        episode_id=episode_id,
        principal_id="principal:alice",
        executor_id="agent:personal-steward",
        human_confirmed=True,
    )
    ctx = svc.reload_execution_context(episode_id, "principal:alice")
    svc.record_evidence(ctx, "file:///drafts/wbucket.json", output_origin="system")

    # Record outcome at _W2 (one week later).
    svc2 = PersonalEpisodeService(broker, clock=lambda: _W2)
    svc2.record_outcome(ctx, "accept", review_duration_seconds=5, estimated_time_saved_seconds=50)

    broker.append(
        EVT_ACTION_SUCCEEDED,
        producer=PDP_PRODUCER,
        principal_id="principal:alice",
        space_id="sovereignty",
        correlation_id=f"action|{ctx.action_id}|succeeded",
        idempotency_key=f"{ctx.action_id}|succeeded",
        episode_id=episode_id,
        payload={"action_id": ctx.action_id, "episode_id": episode_id, "status": "succeeded"},
        occurred_at=_W2,
    )

    obs = service.observe_principal("principal:alice")
    assert len(obs.weekly_samples) == 1
    # _W1 = 2026-W32, _W2 = 2026-W33
    assert obs.weekly_samples[0].week_key == "2026-W33"


def test_effective_verdict_accept_then_reject(broker, service):
    """Blocker 4: accept→reject makes effective verdict reject; no double-count."""
    _assign(broker)
    ep = _make_full_episode(
        broker,
        clock_ts=_W1,
        request_id="accept-reject",
        verdict="accept",
    )
    # Record a later reject on the same episode.
    svc = PersonalEpisodeService(broker, clock=lambda: _W1)
    ctx = svc.reload_execution_context(ep.episode_id, "principal:alice")
    svc.record_outcome(ctx, "reject")

    obs = service.observe_principal("principal:alice")

    # Effective verdict is reject (latest by sequence), not accept.
    assert obs.verdict_distribution.get("accept", 0) == 0
    assert obs.verdict_distribution.get("reject", 0) == 1
    if obs.weekly_samples:
        assert obs.weekly_samples[0].qualifying_episodes == 0


def test_gate_passed_then_rename_from_candidate(broker, service):
    """Blocker 5: four-week success returns 'passed', not 'candidate'."""
    _assign(broker)
    for wi, wk in enumerate([_W1, _W2, _W3, _W4]):
        for ei in range(3):
            _make_full_episode(broker, clock_ts=wk, request_id=f"rn-{wi}-{ei}")

    obs = service.observe_principal("principal:alice")
    assert obs.readiness == "passed"
    assert "candidate" not in obs.readiness
