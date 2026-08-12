"""W2-05 personal episode kernel — real-ledger golden-path tests."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from ecos.ssot.mof.generated.control.mof_control_models import EventEnvelope, Signal
from omo.episode_projection import build_episode_projection_snapshot
from omo.event_ledger import LedgerBroker
from omo.personal_episode import (
    EVT_EPISODE_DECISION,
    EVT_SIGNAL_OBSERVED,
    PERSONAL_SIGNAL_JOURNEY_ID,
    PERSONAL_SIGNAL_OUTCOME_METRIC,
    PERSONAL_SIGNAL_SCENE_ID,
    PersonalEpisodeError,
    PersonalEpisodeService,
    PersonalLocalSignal,
)
from omo.sovereignty import REASON_ALLOW, MandateManager, SovereigntyService


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
