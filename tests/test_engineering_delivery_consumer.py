from __future__ import annotations

import hashlib
import hmac
import io
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

import omo.engineering_delivery_consumer as consumer
import omo.omo_external_resources as external_resources
from omo.engineering_delivery_consumer import (
    EngineeringDeliveryConsumerError,
    EngineeringDeliveryProjectionError,
    build_engineering_delivery_review_queue,
    build_engineering_delivery_shadow_observer,
    consume_engineering_delivery,
    normalize_engineering_delivery_review,
    record_engineering_delivery_review,
)
from omo.omo_belief import MOSBeliefManager
from omo.outcome_feedback import read_outcome_feedback, record_outcome_feedback
from omo.workflow_mesh import WorkflowMeshStore, new_workflow_event

SCENE = {
    "scene_id": "engineering-delivery",
    "journey_id": "intent-to-evidence",
    "outcome_metric": "verified_delivery_lead_time",
}
NOW = "2026-08-20T10:00:00Z"
SIGNING_KEY = "test-engineering-review-signing-key-0001"


@pytest.fixture(autouse=True)
def _fixed_review_clock(monkeypatch):
    monkeypatch.setattr(consumer, "_utc_now", lambda: "2026-08-21T10:00:00Z")
    monkeypatch.setenv("COCKPIT_ENGINEERING_REVIEW_SIGNING_KEY", SIGNING_KEY)


def _grant(run_id: str) -> dict[str, object]:
    grant: dict[str, object] = {
        "admission_id": f"admit-{run_id}",
        "status": "admitted",
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "backend": "engineering-delivery-test",
        "step_run_ids": [f"{run_id}:execute"],
        "capabilities": ["metadata-read"],
        "policy_digest": "engineering-delivery-shadow/v1",
        "issued_at": NOW,
        "expires_at": "2026-08-20T11:00:00Z",
    }
    unsigned = json.dumps(grant, sort_keys=True, separators=(",", ":")).encode()
    grant["proof"] = hashlib.sha256(unsigned).hexdigest()
    return grant


def _succeeded_run(omo_dir, run_id: str = "run-delivery-1", *, scene=SCENE) -> WorkflowMeshStore:
    store = WorkflowMeshStore(omo_dir)
    grant = _grant(run_id)
    store.append(new_workflow_event("WorkflowRequested", run_id, scene_binding=scene))
    store.append(new_workflow_event("WorkflowAdmitted", run_id, payload={"admission": grant, **grant}))
    context = {"step_run_id": f"{run_id}:execute", "admission_id": grant["admission_id"]}
    store.append(new_workflow_event("StepDispatched", run_id, payload=context))
    store.append(new_workflow_event("StepStarted", run_id, payload=context))
    store.append(new_workflow_event("WorkflowSucceeded", run_id))
    return store


def _delivery(delivery_id: str = "delivery-1") -> dict[str, object]:
    return {
        "delivery_id": delivery_id,
        "repository_ref": "github://starlink-awaken/omostation",
        "pr_url": "https://github.com/starlink-awaken/omostation/pull/1842",
        "merge_sha": "a" * 40,
        "requested_at": "2026-08-20T09:00:00Z",
        "merged_at": NOW,
        "evidence_refs": ["evidence://github/pr/1842", "evidence://ci/run/991"],
    }


def _review(delivery_id: str = "delivery-1", *, decision: str = "adopted") -> dict[str, object]:
    return {
        "delivery_id": delivery_id,
        "decision": decision,
        "evidence_refs": ["evidence://human-review/1842"],
    }


def _assertion(
    review: dict[str, object],
    *,
    actor: str = "operator://reviewer-1",
    issued_at: str = "2026-08-21T10:00:00Z",
    workflow_run_id: str = "run-delivery-1",
    candidate_receipt_id: str | None = None,
) -> dict[str, str]:
    binding = {
        "workflow_run_id": workflow_run_id,
        "candidate_receipt_id": candidate_receipt_id or str(review["delivery_id"]),
        "review": review,
    }
    body = {
        "schema": "cockpit-human-principal-assertion/v2",
        "principal_ref": actor,
        "source_class": "real_human",
        "issued_at": issued_at,
        "binding_digest": hashlib.sha256(
            json.dumps(binding, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
    signature = hmac.new(
        SIGNING_KEY.encode(),
        json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(),
        hashlib.sha256,
    ).hexdigest()
    return {**body, "signature": signature}


def test_normalize_review_exposes_the_exact_signed_broker_payload():
    assert normalize_engineering_delivery_review(
        {
            "delivery_id": " delivery-1 ",
            "decision": " ADOPTED ",
            "evidence_refs": [" evidence://human-review/1842 "],
        }
    ) == {
        "delivery_id": "delivery-1",
        "decision": "adopted",
        "evidence_refs": ["evidence://human-review/1842"],
    }


def test_machine_consume_records_only_receipt_and_submitted_feedback(tmp_path):
    store = _succeeded_run(tmp_path)

    first = consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")
    replay = consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")

    assert first["schema"] == "engineering-delivery-consumption/v1"
    assert first["status"] == "recorded"
    assert replay["status"] == "deduplicated"
    assert first["scene"] == {
        "scene_id": "engineering-delivery",
        "tier": "shadow",
        "value_indicator_policy": False,
    }
    assert first["controls"] == {
        "proposal_only": True,
        "activation": "forbidden",
        "workflow_run_creation": False,
        "provider_invocation": False,
        "automatic_promotion": False,
        "personal_value_attribution": False,
    }
    feedback = read_outcome_feedback(tmp_path)
    assert len(feedback) == 1
    assert feedback[0]["consumption_state"] == "submitted"
    assert feedback[0]["actor"] == "system://engineering-delivery-consumer"
    evidence = store.snapshot("run-delivery-1")["evidence"]
    assert list(evidence) == ["external:engineering-delivery:delivery-1"]
    assert "reviewDecision" not in json.dumps(first)


def test_external_resources_cli_exposes_machine_ingest_but_not_human_review(tmp_path, monkeypatch, capsys):
    _succeeded_run(tmp_path)
    monkeypatch.setattr(external_resources, "find_omo_dir", lambda: tmp_path)
    monkeypatch.setattr(external_resources.sys, "stdin", io.StringIO(json.dumps(_delivery())))
    assert (
        external_resources.main(["consume-engineering-delivery", "--workflow-run-id", "run-delivery-1", "--stdin"]) == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "recorded"

    with pytest.raises(SystemExit) as exc_info:
        external_resources.main(
            [
                "review-engineering-delivery",
                "--workflow-run-id",
                "run-delivery-1",
                "--actor",
                "operator://spoofed-reviewer",
                "--stdin",
            ]
        )
    assert exc_info.value.code == 2


@pytest.mark.parametrize("unsafe_key", ["decision", "reviewDecision", "raw_content", "repository_path", "secret"])
def test_machine_consume_rejects_verdict_or_unsafe_fields(tmp_path, unsafe_key):
    _succeeded_run(tmp_path)
    payload = _delivery()
    payload[unsafe_key] = "APPROVED"

    with pytest.raises(EngineeringDeliveryConsumerError, match="unsupported|forbidden"):
        consume_engineering_delivery(tmp_path, payload, workflow_run_id="run-delivery-1")

    assert read_outcome_feedback(tmp_path) == []


def test_machine_consume_requires_exact_engineering_delivery_scene(tmp_path):
    _succeeded_run(tmp_path, scene={**SCENE, "scene_id": "other"})

    with pytest.raises(EngineeringDeliveryConsumerError, match="scene binding"):
        consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")


def test_human_review_is_idempotent_and_projects_primary_log_and_mos(tmp_path):
    _succeeded_run(tmp_path)
    consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")

    first = record_engineering_delivery_review(
        tmp_path,
        _review(),
        workflow_run_id="run-delivery-1",
        principal_assertion=_assertion(_review()),
    )
    replay = record_engineering_delivery_review(
        tmp_path,
        _review(),
        workflow_run_id="run-delivery-1",
        principal_assertion=_assertion(_review()),
    )

    assert first["status"] == "recorded"
    assert replay["status"] == "deduplicated"
    assert first["decision"] == "adopted"
    assert first["qualified_decision_outcome"]["human_verdict"] == "adopted"
    assert first["qualified_decision_outcome"]["value_indicator_policy"] is False
    assert first["qualified_decision_outcome"]["adjudication_receipt_id"].startswith("human-adjudication:")
    assert first["qualified_decision_outcome"]["adjudication_assertion"]["source_class"] == "real_human"
    assert first["reviewed_at"] == "2026-08-21T10:00:00Z"
    assert first["outcome_id"] == "outcome:engineering-delivery:delivery-1"
    assert first["decision_outcome_id"] == first["qualified_decision_outcome"]["decision_outcome_id"]
    assert first["value_indicator_policy"] is False
    assert first["mos_projection"]["status"] == "projected"
    assert len(read_outcome_feedback(tmp_path)) == 2
    mos = MOSBeliefManager(root=tmp_path, registry_file=tmp_path / "memory-os.yaml")
    outcomes = mos._load_state()["decision_outcomes"]
    assert len(outcomes) == 1
    assert outcomes[0]["source_run_id"] == first["qualified_decision_outcome"]["decision_outcome_id"]
    assert outcomes[0]["metadata"] == {
        "scene_id": "engineering-delivery",
        "tier": "shadow",
        "source_class": "real_human",
        "value_indicator_policy": False,
        "personal_value_attribution": False,
    }

    queue = build_engineering_delivery_review_queue(tmp_path)
    assert queue["summary"] == {"row_count": 1, "pending_review_count": 0, "reviewed_count": 1}
    assert queue["rows"][0]["review_status"] == "reviewed"
    assert queue["rows"][0]["latest_decision"] == "adopted"
    assert queue["controls"]["read_only"] is True


@pytest.mark.parametrize("actor", ["agent://worker-1", "system://cockpit", "automation", "cockpit-user", ""])
def test_human_review_rejects_non_human_actor(tmp_path, actor):
    _succeeded_run(tmp_path)
    consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")

    with pytest.raises(EngineeringDeliveryConsumerError, match="human actor"):
        record_engineering_delivery_review(
            tmp_path,
            _review(),
            workflow_run_id="run-delivery-1",
            principal_assertion=_assertion(_review(), actor=actor),
        )


def test_human_review_rejects_forged_principal_assertion(tmp_path):
    _succeeded_run(tmp_path)
    consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")
    assertion = _assertion(_review())
    assertion["signature"] = "0" * 64

    with pytest.raises(EngineeringDeliveryConsumerError, match="invalid human principal assertion signature"):
        record_engineering_delivery_review(
            tmp_path,
            _review(),
            workflow_run_id="run-delivery-1",
            principal_assertion=assertion,
        )


def test_human_review_assertion_cannot_replay_across_workflow_runs(tmp_path):
    _succeeded_run(tmp_path, "run-delivery-1")
    _succeeded_run(tmp_path, "run-delivery-2")
    consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")
    consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-2")
    assertion = _assertion(_review(), workflow_run_id="run-delivery-1")

    with pytest.raises(EngineeringDeliveryConsumerError, match="delivery binding mismatch"):
        record_engineering_delivery_review(
            tmp_path,
            _review(),
            workflow_run_id="run-delivery-2",
            principal_assertion=assertion,
        )


def test_human_review_requires_opaque_evidence_and_fails_closed_on_conflict(tmp_path):
    _succeeded_run(tmp_path)
    consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")
    invalid = _review()
    invalid["evidence_refs"] = ["/Users/example/review.txt"]
    with pytest.raises(EngineeringDeliveryConsumerError, match="opaque URI"):
        record_engineering_delivery_review(
            tmp_path,
            invalid,
            workflow_run_id="run-delivery-1",
            principal_assertion=_assertion(invalid, actor="human://reviewer-1"),
        )

    record_engineering_delivery_review(
        tmp_path,
        _review(),
        workflow_run_id="run-delivery-1",
        principal_assertion=_assertion(_review(), actor="human://reviewer-1"),
    )
    changed = _review()
    changed["decision"] = "rejected"
    with pytest.raises(EngineeringDeliveryConsumerError, match="conflicting qualified decision-outcome"):
        record_engineering_delivery_review(
            tmp_path,
            changed,
            workflow_run_id="run-delivery-1",
            principal_assertion=_assertion(changed, actor="human://reviewer-1"),
        )


@pytest.mark.parametrize(
    "evidence_ref",
    [
        "evidence://test/review-1",
        "evidence://synthetic/review-1",
        "evidence://user_provided/review-1",
    ],
)
def test_human_review_rejects_non_real_evidence_classes(tmp_path, evidence_ref):
    _succeeded_run(tmp_path)
    consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")
    review = _review()
    review["evidence_refs"] = [evidence_ref]

    with pytest.raises(EngineeringDeliveryConsumerError, match="test, synthetic, or user_provided"):
        record_engineering_delivery_review(
            tmp_path,
            review,
            workflow_run_id="run-delivery-1",
            principal_assertion=_assertion(review),
        )


def test_generic_feedback_cannot_promote_or_block_engineering_delivery(tmp_path):
    _succeeded_run(tmp_path)
    consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")
    record_outcome_feedback(
        tmp_path,
        {
            "workflow_run_id": "run-delivery-1",
            "outcome_id": "outcome:engineering-delivery:delivery-1",
            "scene_binding": SCENE,
            "consumption_state": "rejected",
            "consumer_ref": "operator://spoofed-reviewer",
            "result_ref": "receipt://engineering-delivery/delivery-1",
            "evidence_refs": ["evidence://human-review/spoofed"],
            "value": {},
            "observed_at": "2026-08-21T09:00:00Z",
        },
        actor="operator://spoofed-reviewer",
    )

    pending = build_engineering_delivery_review_queue(tmp_path)
    assert pending["rows"][0]["review_status"] == "pending"
    recorded = record_engineering_delivery_review(
        tmp_path,
        _review(),
        workflow_run_id="run-delivery-1",
        principal_assertion=_assertion(_review(), actor="operator://authenticated-reviewer"),
    )
    assert recorded["status"] == "recorded"
    assert build_engineering_delivery_review_queue(tmp_path)["rows"][0]["latest_decision"] == "adopted"


def test_concurrent_conflicting_human_reviews_serialize_without_deadlock(tmp_path):
    _succeeded_run(tmp_path)
    consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")

    def submit(decision: str):
        try:
            return record_engineering_delivery_review(
                tmp_path,
                _review(decision=decision),
                workflow_run_id="run-delivery-1",
                principal_assertion=_assertion(
                    _review(decision=decision),
                    actor="operator://reviewer-1",
                ),
            )
        except EngineeringDeliveryConsumerError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(submit, decision) for decision in ("adopted", "rejected")]
        results = [future.result(timeout=5) for future in futures]

    assert sum(isinstance(result, dict) for result in results) == 1
    assert sum(isinstance(result, EngineeringDeliveryConsumerError) for result in results) == 1
    assert len(read_outcome_feedback(tmp_path)) == 2


def test_mos_failure_is_explicit_and_retry_completes_projection(tmp_path, monkeypatch):
    _succeeded_run(tmp_path)
    consume_engineering_delivery(tmp_path, _delivery(), workflow_run_id="run-delivery-1")

    original = consumer.MOSBeliefManager.record_decision_outcome

    def fail_projection(*_args, **_kwargs):
        raise OSError("mos unavailable")

    monkeypatch.setattr(consumer.MOSBeliefManager, "record_decision_outcome", fail_projection)
    with pytest.raises(EngineeringDeliveryProjectionError, match="MOS projection degraded"):
        record_engineering_delivery_review(
            tmp_path,
            _review(),
            workflow_run_id="run-delivery-1",
            principal_assertion=_assertion(_review()),
        )
    degraded_observer = build_engineering_delivery_shadow_observer(tmp_path, as_of=datetime(2026, 8, 22, tzinfo=UTC))
    assert degraded_observer["qualifying_decision_outcomes"] == 0
    projection_records = consumer._projection_records(tmp_path)
    assert [item["status"] for item in projection_records] == ["pending", "degraded"]

    monkeypatch.setattr(consumer.MOSBeliefManager, "record_decision_outcome", original)
    replay = record_engineering_delivery_review(
        tmp_path,
        _review(),
        workflow_run_id="run-delivery-1",
        principal_assertion=_assertion(_review()),
    )
    assert replay["status"] == "deduplicated"
    assert replay["mos_projection"]["status"] == "projected"
    projected_observer = build_engineering_delivery_shadow_observer(tmp_path, as_of=datetime(2026, 8, 22, tzinfo=UTC))
    assert projected_observer["qualifying_decision_outcomes"] == 1
    assert [item["status"] for item in consumer._projection_records(tmp_path)] == [
        "pending",
        "degraded",
        "pending",
        "projected",
    ]


def test_rolling_observer_uses_half_open_window_and_never_auto_passes(tmp_path, monkeypatch):
    as_of = datetime(2026, 8, 22, 0, 0, tzinfo=UTC)
    reviewed_times = [
        as_of,
        as_of - timedelta(days=7),
        as_of - timedelta(days=7, seconds=1),
        *(as_of - timedelta(hours=index) for index in range(1, 20)),
    ]
    for index, reviewed_at in enumerate(reviewed_times):
        run_id = f"run-{index}"
        delivery_id = f"delivery-{index}"
        _succeeded_run(tmp_path, run_id)
        consume_engineering_delivery(tmp_path, _delivery(delivery_id), workflow_run_id=run_id)
        monkeypatch.setattr(
            consumer,
            "_utc_now",
            lambda value=reviewed_at: value.isoformat().replace("+00:00", "Z"),
        )
        record_engineering_delivery_review(
            tmp_path,
            _review(delivery_id),
            workflow_run_id=run_id,
            principal_assertion=_assertion(
                _review(delivery_id),
                actor=f"operator://reviewer-{index}",
                issued_at=reviewed_at.isoformat().replace("+00:00", "Z"),
                workflow_run_id=run_id,
            ),
        )

    observer = build_engineering_delivery_shadow_observer(tmp_path, as_of=as_of)

    assert observer["window"] == {
        "start": "2026-08-15T00:00:00Z",
        "end_exclusive": "2026-08-22T00:00:00Z",
    }
    assert observer["qualifying_decision_outcomes"] == 20
    assert observer["status"] == "ready_for_human_review"
    assert observer["human_gate"] == "not_decided"
    assert observer["value_indicator_policy"] is False


def test_rolling_observer_reports_unprovable_for_corrupt_primary_log(tmp_path):
    path = tmp_path / consumer.QUALIFIED_DECISION_OUTCOME_LOG
    path.parent.mkdir(parents=True)
    path.write_text("not-json\n", encoding="utf-8")

    result = build_engineering_delivery_shadow_observer(
        tmp_path,
        as_of=datetime(2026, 8, 22, tzinfo=UTC),
    )

    assert result["status"] == "unprovable"
    assert result["verdict"] == "UNPROVABLE"
    assert result["human_gate"] == "not_ready"
    assert result["error"] == "qualified_decision_outcomes_unreadable"
    assert "JSON" not in json.dumps(result)
