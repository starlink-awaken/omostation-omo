"""Pure helpers for omo.personal_episode."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _payload(row: Mapping[str, Any]) -> Mapping[str, Any]:
    from omo.personal_episode import PersonalEpisodeError

    try:
        value = json.loads(str(row["payload_json"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise PersonalEpisodeError("malformed_episode", "episode payload is invalid") from exc
    if not isinstance(value, Mapping):
        raise PersonalEpisodeError("malformed_episode", "episode payload must be an object")
    return value


def _required_payload(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"payload.{key} is required")
    return value


def _short_hash(*parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:12]
    return digest


def _deterministic(prefix: str, *parts: str) -> str:
    return prefix + _short_hash(*parts)


def _validate_burden(name: str, value: float | None) -> None:
    from omo.personal_episode import PersonalEpisodeError

    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PersonalEpisodeError("invalid_burden", f"{name} must be a number")
    v = float(value)
    if v < 0 or not math.isfinite(v):
        raise PersonalEpisodeError("invalid_burden", f"{name} must be non-negative and finite")


def _sha256_digest(value: str) -> str:
    from omo.personal_episode import PersonalEpisodeError

    if not isinstance(value, str) or not value.startswith("sha256:"):
        raise PersonalEpisodeError("invalid_revision_receipt", "revision_digest must be sha256:<hex>")
    digest = value.removeprefix("sha256:")
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise PersonalEpisodeError("invalid_revision_receipt", "revision_digest must be sha256:<hex>")
    return value


def _personal_draft_digest(evidence_ref: str) -> str:
    from omo.personal_episode import PERSONAL_DRAFT_EVIDENCE_PREFIX, PersonalEpisodeError

    if not isinstance(evidence_ref, str) or not evidence_ref.startswith(PERSONAL_DRAFT_EVIDENCE_PREFIX):
        raise PersonalEpisodeError(
            "invalid_evidence_ref",
            "evidence_ref must be an opaque personal-draft digest",
        )
    try:
        return _sha256_digest("sha256:" + evidence_ref.removeprefix(PERSONAL_DRAFT_EVIDENCE_PREFIX))
    except PersonalEpisodeError as exc:
        raise PersonalEpisodeError(
            "invalid_evidence_ref",
            "evidence_ref does not contain a valid sha256 digest",
        ) from exc


def _feedback_ref(feedback_id: str) -> str:
    from omo.personal_episode import PersonalEpisodeError

    if (
        not isinstance(feedback_id, str)
        or not feedback_id.strip()
        or feedback_id != feedback_id.strip()
        or len(feedback_id) > 240
    ):
        raise PersonalEpisodeError(
            "invalid_feedback_id",
            "feedback_id must be a non-empty identifier of at most 240 characters",
        )
    return f"feedback://sha256:{hashlib.sha256(feedback_id.encode('utf-8')).hexdigest()}"


def _outcome_replay_matches(
    recorded: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    raw_feedback_id: str | None,
) -> bool:
    normalized = dict(recorded)
    if raw_feedback_id is not None and normalized.get("feedback_id") == raw_feedback_id:
        normalized["feedback_id"] = expected.get("feedback_id")
    if "outcome_feedback_schema" in normalized or "revision_receipt" in normalized:
        return normalized == dict(expected)
    legacy_fields = (
        "feedback_id",
        "verdict",
        "action_id",
        "review_duration_seconds",
        "estimated_time_saved_seconds",
    )
    return all(normalized.get(field) == expected.get(field) for field in legacy_fields)


def _revision_fields(values: list[str] | tuple[str, ...] | None) -> list[str]:
    if not values:
        return []
    allowed = {"title", "context", "deadline", "next_action"}
    return [value for value in values if value in allowed]


def _has_valid_revision_receipt(
    outcome: Mapping[str, Any] | None,
    evidence_payloads: list[Mapping[str, Any]],
) -> bool:
    if outcome is None or outcome.get("outcome_feedback_schema") != "outcome-feedback/v1":
        return False
    receipt = outcome.get("revision_receipt")
    if not isinstance(receipt, Mapping) or receipt.get("schema") != "revision-receipt/v1":
        return False
    if not evidence_payloads:
        return False
    latest_ref = evidence_payloads[-1].get("evidence_uri")
    if receipt.get("candidate_ref") != latest_ref or not isinstance(latest_ref, str):
        return False
    try:
        candidate_digest = _personal_draft_digest(latest_ref)
        revision_digest = _sha256_digest(str(receipt.get("revision_digest", "")))
    except ValueError:
        return False
    changed_fields = receipt.get("changed_fields")
    if not isinstance(changed_fields, list) or any(
        not isinstance(value, str) or value not in {"title", "context", "deadline", "next_action"}
        for value in changed_fields
    ):
        return False
    if outcome.get("verdict") == "edit":
        return bool(changed_fields) and revision_digest != candidate_digest
    return not changed_fields and revision_digest == candidate_digest


def _canonical_chain_observation(
    *,
    episode_id: str,
    principal_id: str,
    all_rows: list[Mapping[str, Any]],
    chain_integrity_ok: bool = True,
) -> dict[str, Any]:
    from omo.personal_episode import (
        EVT_ACTION_SUCCEEDED_CANONICAL,
        EVT_ADJUDICATION_RECORDED,
        EVT_DECISION_PROPOSED,
        EVT_EPISODE_CLOSED,
        EVT_EPISODE_DECISION,
        EVT_EVIDENCE_LOCAL_DRAFT,
        EVT_EVIDENCE_RECORDED,
        EVT_HUMAN_ADJUDICATION,
        EVT_MANDATE_GRANTED_CANONICAL,
        EVT_MEMORY_CANDIDATE_PROPOSED,
        EVT_OUTCOME_HUMAN,
        EVT_OUTCOME_OBSERVED,
        EVT_RESPONSIBILITY_LINKED,
        EVT_ROLE_CONTEXT_ASSIGNED,
        EVT_SIGNAL_OBSERVED,
        PERSONAL_EPISODE_PRODUCER,
    )
    from omo.sovereignty.enforcement import EVT_ACTION_SUCCEEDED, PDP_PRODUCER
    from omo.sovereignty.mandates import EVT_MANDATE_GRANT, EVT_MANDATE_REVOKE, MANDATE_PRODUCER

    rows = [row for row in all_rows if str(row.get("episode_id") or "") == episode_id]
    rows.sort(key=lambda row: int(row.get("sequence", 0)))
    by_id = {str(row.get("event_id")): row for row in rows if row.get("event_id")}
    personal_rows = [row for row in rows if row.get("producer") == PERSONAL_EPISODE_PRODUCER]
    decision = next((row for row in personal_rows if row.get("event_type") == EVT_EPISODE_DECISION), None)
    decision_dt = _parse_ts(decision.get("occurred_at")) if decision else None

    legacy_evidence = [row for row in personal_rows if row.get("event_type") == EVT_EVIDENCE_LOCAL_DRAFT]
    legacy_outcomes = [row for row in personal_rows if row.get("event_type") == EVT_OUTCOME_HUMAN]
    fallback_outcome = _payload(legacy_outcomes[-1]) if legacy_outcomes else None
    fallback_outcome_dt = _parse_ts(legacy_outcomes[-1].get("occurred_at")) if legacy_outcomes else None

    leg_types = [
        EVT_SIGNAL_OBSERVED,
        EVT_ROLE_CONTEXT_ASSIGNED,
        EVT_RESPONSIBILITY_LINKED,
        EVT_DECISION_PROPOSED,
        EVT_HUMAN_ADJUDICATION,
        EVT_MANDATE_GRANTED_CANONICAL,
        EVT_ACTION_SUCCEEDED_CANONICAL,
        EVT_EVIDENCE_RECORDED,
        EVT_OUTCOME_OBSERVED,
        EVT_ADJUDICATION_RECORDED,
        EVT_MEMORY_CANDIDATE_PROPOSED,
        EVT_EPISODE_CLOSED,
    ]
    gap_names = {
        EVT_SIGNAL_OBSERVED: "missing_signal_observed",
        EVT_ROLE_CONTEXT_ASSIGNED: "missing_role_context_assigned",
        EVT_RESPONSIBILITY_LINKED: "missing_responsibility_linked",
        EVT_DECISION_PROPOSED: "missing_decision_proposed",
        EVT_HUMAN_ADJUDICATION: "missing_human_adjudication",
        EVT_MANDATE_GRANTED_CANONICAL: "missing_mandate_granted",
        EVT_ACTION_SUCCEEDED_CANONICAL: "missing_action_succeeded",
        EVT_EVIDENCE_RECORDED: "missing_evidence_recorded",
        EVT_OUTCOME_OBSERVED: "missing_outcome_observed",
        EVT_ADJUDICATION_RECORDED: "missing_adjudication_recorded",
        EVT_MEMORY_CANDIDATE_PROPOSED: "missing_memory_candidate_proposed",
        EVT_EPISODE_CLOSED: "missing_episode_closed",
    }
    gaps: list[str] = []
    if not chain_integrity_ok:
        gaps.append("ledger_hash_chain_invalid")

    closures = [row for row in personal_rows if row.get("event_type") == EVT_EPISODE_CLOSED]
    chain: dict[str, Mapping[str, Any]] = {}
    if closures:
        current = closures[-1]
        for expected in reversed(leg_types):
            if current.get("event_type") != expected:
                gaps.append(gap_names[expected])
                break
            chain[expected] = current
            if expected == EVT_SIGNAL_OBSERVED:
                break
            cause = current.get("causation_id")
            current = by_id.get(str(cause))
            if current is None:
                gaps.append("causation_link_missing")
                break

    for event_type in leg_types:
        if event_type not in chain:
            code = gap_names[event_type]
            if code not in gaps:
                gaps.append(code)

    ordered = [chain[event_type] for event_type in leg_types if event_type in chain]
    if len(ordered) == len(leg_types):
        sequences = [int(row.get("sequence", 0)) for row in ordered]
        if sequences != sorted(sequences) or len(set(sequences)) != len(sequences):
            gaps.append("chain_order_invalid")
        for row in ordered:
            if row.get("principal_id") != principal_id:
                gaps.append("cross_principal_chain")
                break
            if row.get("episode_id") != episode_id:
                gaps.append("cross_episode_chain")
                break
            if row.get("producer") != PERSONAL_EPISODE_PRODUCER:
                gaps.append("producer_mismatch")
                break
        if ordered:
            expected_role = ordered[0].get("role_context_id")
            expected_responsibility = ordered[0].get("responsibility_id")
            if any(
                row.get("role_context_id") != expected_role or row.get("responsibility_id") != expected_responsibility
                for row in ordered
            ):
                gaps.append("role_responsibility_identity_mismatch")

    mandate_row = chain.get(EVT_MANDATE_GRANTED_CANONICAL)
    action_row = chain.get(EVT_ACTION_SUCCEEDED_CANONICAL)
    evidence_row = chain.get(EVT_EVIDENCE_RECORDED)
    outcome_row = chain.get(EVT_OUTCOME_OBSERVED)
    adjudication_row = chain.get(EVT_ADJUDICATION_RECORDED)
    memory_row = chain.get(EVT_MEMORY_CANDIDATE_PROPOSED)
    closure_row = chain.get(EVT_EPISODE_CLOSED)

    authority_mandate = None
    if mandate_row is not None:
        authority_id = _payload(mandate_row).get("authority_event_id")
        authority_mandate = by_id.get(str(authority_id))
        if (
            authority_mandate is None
            or authority_mandate.get("producer") != MANDATE_PRODUCER
            or authority_mandate.get("event_type") != EVT_MANDATE_GRANT
            or authority_mandate.get("principal_id") != principal_id
            or authority_mandate.get("episode_id") != episode_id
            or authority_mandate.get("mandate_id") != mandate_row.get("mandate_id")
        ):
            gaps.append("mandate_authority_invalid")

    authority_action = None
    if action_row is not None:
        authority_id = _payload(action_row).get("authority_event_id")
        authority_action = by_id.get(str(authority_id))
        if (
            authority_action is None
            or authority_action.get("producer") != PDP_PRODUCER
            or authority_action.get("event_type") != EVT_ACTION_SUCCEEDED
            or authority_action.get("principal_id") != principal_id
            or authority_action.get("episode_id") != episode_id
            or authority_action.get("mandate_id") != action_row.get("mandate_id")
        ):
            gaps.append("action_authority_invalid")

    if authority_mandate is not None and authority_action is not None:
        grant_seq = int(authority_mandate.get("sequence", 0))
        action_seq = int(authority_action.get("sequence", 0))
        if grant_seq >= action_seq:
            gaps.append("mandate_after_action")
        mandate_id = authority_mandate.get("mandate_id")
        if any(
            row.get("producer") == MANDATE_PRODUCER
            and row.get("event_type") == EVT_MANDATE_REVOKE
            and row.get("mandate_id") == mandate_id
            and int(row.get("sequence", 0)) <= action_seq
            for row in rows
        ):
            gaps.append("mandate_revoked_before_action")
        action_payload = _payload(authority_action)
        authority_ref = action_payload.get("principal_authority_ref")
        receipt_digest = action_payload.get("principal_receipt_digest")
        if not isinstance(authority_ref, str) or not authority_ref.startswith("authority:"):
            gaps.append("principal_authority_unbound")
        digest_hex = receipt_digest.removeprefix("sha256:") if isinstance(receipt_digest, str) else ""
        if (
            not isinstance(receipt_digest, str)
            or not receipt_digest.startswith("sha256:")
            or len(digest_hex) != 64
            or any(char not in "0123456789abcdef" for char in digest_hex)
        ):
            gaps.append("principal_authority_unbound")

    effective_outcome = _payload(outcome_row) if outcome_row is not None else fallback_outcome
    effective_outcome_dt = _parse_ts(outcome_row.get("occurred_at")) if outcome_row is not None else fallback_outcome_dt
    evidence_payloads = (
        [_payload(evidence_row)] if evidence_row is not None else [_payload(row) for row in legacy_evidence]
    )
    has_revision_receipt = _has_valid_revision_receipt(effective_outcome, evidence_payloads)
    if outcome_row is not None and not has_revision_receipt:
        gaps.append("revision_receipt_invalid")

    if closure_row is not None and all(
        row is not None for row in (mandate_row, evidence_row, outcome_row, adjudication_row, memory_row)
    ):
        closure = _payload(closure_row)
        if closure.get("chain_schema_version") != "personal-episode-chain/v1":
            gaps.append("closure_schema_invalid")
        expected_bindings = {
            "mandate_event_id": mandate_row["event_id"],
            "evidence_event_id": evidence_row["event_id"],
            "outcome_event_id": outcome_row["event_id"],
            "adjudication_event_id": adjudication_row["event_id"],
            "memory_candidate_event_id": memory_row["event_id"],
        }
        if any(closure.get(key) != value for key, value in expected_bindings.items()):
            gaps.append("closure_binding_invalid")
        outcome = _payload(outcome_row)
        adjudication = _payload(adjudication_row)
        memory = _payload(memory_row)
        if not (
            closure.get("terminal_verdict")
            == outcome.get("verdict")
            == adjudication.get("verdict")
            == memory.get("verdict")
        ):
            gaps.append("terminal_verdict_mismatch")

    signal_row = chain.get(EVT_SIGNAL_OBSERVED)
    signal_dt = _parse_ts(signal_row.get("occurred_at")) if signal_row is not None else decision_dt
    unique_gaps = list(dict.fromkeys(gaps))
    return {
        "episode_id": episode_id,
        "signal_dt": signal_dt,
        "decision_dt": decision_dt,
        "has_signal_source": signal_row is not None,
        "has_action_succeeded": authority_action is not None,
        "evidence_origins": [payload.get("output_origin", "unknown") for payload in evidence_payloads],
        "effective_outcome": effective_outcome,
        "effective_outcome_dt": effective_outcome_dt,
        "has_revision_receipt": has_revision_receipt,
        "receipt_candidate_origin": (
            evidence_payloads[-1].get("output_origin", "unknown")
            if has_revision_receipt and evidence_payloads
            else None
        ),
        "has_complete_chain": not unique_gaps,
        "chain_gaps": unique_gaps,
    }


def _iso_week_key(dt: datetime) -> str:
    year, week, _ = dt.date().isocalendar()
    return f"{year}-W{week:02d}"


def _week_monday(week_key: str) -> date:
    year, week = week_key.split("-W")
    jan4 = date(int(year), 1, 4)
    start = jan4 - timedelta(days=jan4.weekday())
    return start + timedelta(weeks=int(week) - 1)


def _are_consecutive_weeks(samples: list[Any]) -> bool:
    if len(samples) < 2:
        return True
    for i in range(len(samples) - 1):
        current = _week_monday(samples[i].week_key)
        next_week = _week_monday(samples[i + 1].week_key)
        if (next_week - current).days != 7:
            return False
    return True


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 0:
        return (ordered[mid - 1] + ordered[mid]) / 2.0
    return ordered[mid]


def _build_observation(
    principal_id: str,
    observations: list[dict[str, Any]],
) -> Any:
    from omo.personal_episode import (
        QUALIFYING_EPISODE_TARGET,
        PrincipalObservation,
        WeeklySample,
    )

    total_episodes = len(observations)

    verdict_dist: dict[str, int] = {}
    for ep in observations:
        eff = ep["effective_outcome"]
        if eff is not None:
            v = eff.get("verdict")
            if v:
                verdict_dist[v] = verdict_dist.get(v, 0) + 1

    system_ev = sum(1 for ep in observations for o in ep["evidence_origins"] if o == "system")
    user_ev = sum(1 for ep in observations for o in ep["evidence_origins"] if o == "user_provided")
    unknown_ev = sum(1 for ep in observations for o in ep["evidence_origins"] if o not in ("system", "user_provided"))

    latencies: list[float] = []
    for ep in observations:
        sig_dt: datetime | None = ep["signal_dt"]
        outcome_dt: datetime | None = ep["effective_outcome_dt"]
        if sig_dt is not None and outcome_dt is not None:
            delta = (outcome_dt - sig_dt).total_seconds()
            if delta >= 0:
                latencies.append(delta)
    median_latency = _median(latencies)

    week_groups: dict[str, list[dict[str, Any]]] = {}
    for ep in observations:
        outcome_dt = ep["effective_outcome_dt"]
        if outcome_dt is None:
            continue
        wk = _iso_week_key(outcome_dt)
        week_groups.setdefault(wk, []).append(ep)

    weekly_samples: list[WeeklySample] = []
    for wk in sorted(week_groups):
        eps = week_groups[wk]
        total = len(eps)

        vd: dict[str, int] = {}
        sys_ev_w = usr_ev_w = unk_ev_w = 0
        for ep in eps:
            eff = ep["effective_outcome"]
            if eff is not None:
                v = eff.get("verdict")
                if v:
                    vd[v] = vd.get(v, 0) + 1
            for o in ep["evidence_origins"]:
                if o == "system":
                    sys_ev_w += 1
                elif o == "user_provided":
                    usr_ev_w += 1
                else:
                    unk_ev_w += 1

        sys_accept_eps = 0
        complete_burden_eps = 0
        review_lt_saved_eps = 0
        qualifying_eps = 0
        summed_review = 0.0
        summed_saved = 0.0
        has_review = False
        has_saved = False

        for ep in eps:
            has_system = ep.get("receipt_candidate_origin") == "system"
            eff = ep["effective_outcome"]
            is_accept = eff is not None and eff.get("verdict") == "accept"
            review_raw = eff.get("review_duration_seconds") if eff else None
            saved_raw = eff.get("estimated_time_saved_seconds") if eff else None
            complete = review_raw is not None and saved_raw is not None
            review_lt = False
            if review_raw is not None and saved_raw is not None:
                try:
                    review_lt = float(review_raw) < float(saved_raw)
                except (TypeError, ValueError):
                    review_lt = False

            if review_raw is not None:
                summed_review += float(review_raw)
                has_review = True
            if saved_raw is not None:
                summed_saved += float(saved_raw)
                has_saved = True

            if is_accept and has_system:
                sys_accept_eps += 1
            if complete:
                complete_burden_eps += 1
            if review_lt:
                review_lt_saved_eps += 1
            if (
                is_accept
                and has_system
                and complete
                and review_lt
                and ep.get("has_complete_chain", False)
                and ep.get("has_signal_source", False)
                and ep.get("has_action_succeeded", False)
                and ep.get("has_revision_receipt", False)
            ):
                qualifying_eps += 1

        gate_met = qualifying_eps >= 3
        weekly_samples.append(
            WeeklySample(
                week_key=wk,
                total_episodes=total,
                qualifying_episodes=qualifying_eps,
                system_accept_episodes=sys_accept_eps,
                complete_burden_episodes=complete_burden_eps,
                review_lt_saved_episodes=review_lt_saved_eps,
                summed_review_seconds=summed_review if has_review else None,
                summed_saved_seconds=summed_saved if has_saved else None,
                verdict_distribution=vd,
                system_evidence_count=sys_ev_w,
                user_evidence_count=usr_ev_w,
                unknown_evidence_count=unk_ev_w,
                gate_met=gate_met,
            )
        )

    qualifying_episodes = sum(sample.qualifying_episodes for sample in weekly_samples)
    qualifying_episode_ids = [
        str(ep["episode_id"])
        for ep in observations
        if ep.get("has_complete_chain", False)
        and ep.get("effective_outcome") is not None
        and ep["effective_outcome"].get("verdict") == "accept"
        and ep.get("receipt_candidate_origin") == "system"
        and ep["effective_outcome"].get("review_duration_seconds") is not None
        and ep["effective_outcome"].get("estimated_time_saved_seconds") is not None
        and float(ep["effective_outcome"]["review_duration_seconds"])
        < float(ep["effective_outcome"]["estimated_time_saved_seconds"])
        and ep.get("has_signal_source", False)
        and ep.get("has_action_succeeded", False)
        and ep.get("has_revision_receipt", False)
    ]

    # Gate evaluation: 30 qualifying episodes and 4 consecutive qualifying weeks.
    readiness, gaps = _evaluate_readiness_gate(
        weekly_samples,
        qualifying_episodes=qualifying_episodes,
        target=QUALIFYING_EPISODE_TARGET,
    )

    return PrincipalObservation(
        principal_id=principal_id,
        readiness=readiness,
        total_episodes=total_episodes,
        qualifying_episodes=qualifying_episodes,
        qualifying_target=QUALIFYING_EPISODE_TARGET,
        remaining_to_target=max(0, QUALIFYING_EPISODE_TARGET - qualifying_episodes),
        verdict_distribution=verdict_dist,
        system_evidence_count=system_ev,
        user_evidence_count=user_ev,
        unknown_evidence_count=unknown_ev,
        signal_to_verdict_latency_seconds=median_latency,
        weekly_samples=weekly_samples,
        gate_gaps=gaps,
        chain_schema_version="personal-episode-chain/v1",
        qualifying_episode_ids=qualifying_episode_ids,
        episode_gaps={str(ep["episode_id"]): list(ep.get("chain_gaps", [])) for ep in observations},
    )


def _evaluate_readiness_gate(
    weekly_samples: list[Any],
    *,
    qualifying_episodes: int,
    target: int,
) -> tuple[str, list[str]]:
    if not weekly_samples:
        return "not_ready", ["no weekly samples"]

    qualifying = [s for s in weekly_samples if s.gate_met]
    consecutive_window_found = False

    if len(qualifying) >= 4:
        for i in range(len(qualifying) - 3):
            window = qualifying[i : i + 4]
            if _are_consecutive_weeks(window):
                consecutive_window_found = True
                break

    if qualifying_episodes >= target and consecutive_window_found:
        return "passed", []

    gaps: list[str] = []
    if qualifying_episodes < target:
        gaps.append(f"only {qualifying_episodes} qualifying episode(s), need {target}")
    met_weeks = [s for s in weekly_samples if s.gate_met]
    if not met_weeks:
        gaps.append(
            "no qualifying weeks yet (need >=3 system-accept episodes with complete burden and review<saved per week)"
        )
    elif not consecutive_window_found:
        gaps.append(f"only {len(met_weeks)} qualifying week(s), need 4 consecutive")
        for i in range(len(met_weeks) - 1):
            wk_a = met_weeks[i]
            wk_b = met_weeks[i + 1]
            monday_a = _week_monday(wk_a.week_key)
            monday_b = _week_monday(wk_b.week_key)
            if (monday_b - monday_a).days != 7:
                gaps.append(f"non-consecutive gap between {wk_a.week_key} and {wk_b.week_key}")
        below = [s for s in weekly_samples if not s.gate_met]
        if below:
            gaps.append(f"{len(below)} week(s) below threshold (need >=3 qualifying episodes each)")

    return "collecting", gaps
