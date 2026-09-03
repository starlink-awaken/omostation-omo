from omo.work_case import ExternalAction, ExternalActionState, WorkCase, WorkCaseState


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
