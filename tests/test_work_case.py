from omo.work_case import ExternalAction, ExternalActionState


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
