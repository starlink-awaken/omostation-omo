from pathlib import Path

from omo.work_case import ExternalAction, ExternalActionState, WorkCase, WorkCaseState, create_work_case_draft


def test_external_action_requires_the_exact_approved_snapshot_before_execution():
    action = ExternalAction.propose(
        case_id="CASE-001",
        action_type="email_send",
        recipients=["unit-a@example.test", "unit-b@example.test"],
        content_digest="sha256:content",
        attachment_digests=["sha256:attachment"],
    )

    assert action.state is ExternalActionState.PROPOSED
    assert not action.can_execute("sha256:stale")

    action.confirm(action.approval_digest)

    assert action.state is ExternalActionState.CONFIRMED
    assert action.can_execute(action.approval_digest)


def test_work_case_requires_plan_confirmation_and_keeps_the_latest_valid_submission():
    work_case = WorkCase.draft(case_id="CASE-001", title="数据调查")

    assert work_case.state is WorkCaseState.DRAFT
    work_case.set_plan("sha256:plan-v1")
    work_case.confirm_plan("sha256:plan-v1")
    work_case.start_execution()

    work_case.record_submission("unit-a", "sha256:submission-v1", valid=True)
    work_case.record_submission("unit-a", "sha256:submission-v2", valid=True)

    assert work_case.state is WorkCaseState.EXECUTING
    assert [item.version for item in work_case.submissions] == [1, 2]
    assert work_case.current_submission("unit-a").digest == "sha256:submission-v2"


def test_work_case_draft_uses_the_planned_task_ingress(monkeypatch, tmp_path):
    seen = {}

    def fake_create(omo_dir: Path, *, task_data, ingress_plane, source_ref):
        seen.update(omo_dir=omo_dir, task_data=task_data, ingress_plane=ingress_plane, source_ref=source_ref)
        return task_data

    monkeypatch.setattr("omo.omo_ingress_task_lifecycle.create_planned_task", fake_create)

    created = create_work_case_draft(tmp_path, case_id="CASE-001", title="数据调查", source_ref="oa://task-1")

    assert created["id"] == "CASE-001"
    assert seen["ingress_plane"] == "projects/omo:work_case"
    assert seen["task_data"]["work_case"]["status"] == "draft"
