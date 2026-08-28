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
    return True


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
    from omo.personal_episode import PrincipalObservation, WeeklySample

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

    # Gate evaluation: 4 consecutive qualifying weeks.
    readiness, gaps = _evaluate_readiness_gate(weekly_samples)

    return PrincipalObservation(
        principal_id=principal_id,
        readiness=readiness,
        total_episodes=total_episodes,
        verdict_distribution=verdict_dist,
        system_evidence_count=system_ev,
        user_evidence_count=user_ev,
        unknown_evidence_count=unknown_ev,
        signal_to_verdict_latency_seconds=median_latency,
        weekly_samples=weekly_samples,
        gate_gaps=gaps,
    )


def _evaluate_readiness_gate(
    weekly_samples: list[Any],
) -> tuple[str, list[str]]:
    if not weekly_samples:
        return "not_ready", ["no weekly samples"]

    qualifying = [s for s in weekly_samples if s.gate_met]

    if len(qualifying) >= 4:
        for i in range(len(qualifying) - 3):
            window = qualifying[i : i + 4]
            if _are_consecutive_weeks(window):
                return "passed", []

    gaps: list[str] = []
    met_weeks = [s for s in weekly_samples if s.gate_met]
    if not met_weeks:
        gaps.append(
            "no qualifying weeks yet (need >=3 system-accept episodes with complete burden and review<saved per week)"
        )
    else:
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
