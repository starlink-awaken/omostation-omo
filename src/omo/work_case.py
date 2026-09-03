"""Work-case domain primitives for controlled external actions.

This module deliberately contains no channel client.  Adapters may execute an
action only after OMO has matched the operator-approved immutable snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from hashlib import sha256
from json import dumps
from pathlib import Path
from typing import Any
from uuid import uuid4


class ExternalActionState(StrEnum):
    PROPOSED = "proposed"
    CONFIRMED = "confirmed"


class WorkCaseState(StrEnum):
    DRAFT = "draft"
    PLANNED = "planned"
    EXECUTING = "executing"


@dataclass(frozen=True)
class Submission:
    unit_id: str
    version: int
    digest: str
    valid: bool


@dataclass
class WorkCase:
    case_id: str
    title: str
    state: WorkCaseState = WorkCaseState.DRAFT
    plan_digest: str | None = None
    submissions: list[Submission] = field(default_factory=list)

    @classmethod
    def draft(cls, *, case_id: str, title: str) -> WorkCase:
        return cls(case_id=case_id, title=title)

    def set_plan(self, plan_digest: str) -> None:
        if self.state is not WorkCaseState.DRAFT:
            raise ValueError("only draft cases accept a plan")
        self.plan_digest = plan_digest

    def confirm_plan(self, plan_digest: str) -> None:
        if self.plan_digest != plan_digest:
            raise ValueError("plan confirmation does not match the current plan")
        self.state = WorkCaseState.PLANNED

    def start_execution(self) -> None:
        if self.state is not WorkCaseState.PLANNED:
            raise ValueError("only planned cases can start execution")
        self.state = WorkCaseState.EXECUTING

    def record_submission(self, unit_id: str, digest: str, *, valid: bool) -> Submission:
        if self.state is not WorkCaseState.EXECUTING:
            raise ValueError("submissions require an executing case")
        version = 1 + sum(item.unit_id == unit_id for item in self.submissions)
        submission = Submission(unit_id=unit_id, version=version, digest=digest, valid=valid)
        self.submissions.append(submission)
        return submission

    def current_submission(self, unit_id: str) -> Submission | None:
        candidates = [item for item in self.submissions if item.unit_id == unit_id and item.valid]
        return candidates[-1] if candidates else None


def create_work_case_draft(omo_dir: Path, *, case_id: str, title: str, source_ref: str) -> dict[str, Any]:
    """Create a case draft through the canonical planned-task ingress."""
    from omo.omo_ingress_task_lifecycle import create_planned_task

    task_data: dict[str, Any] = {
        "id": case_id,
        "title": title,
        "status": "candidate",
        "task_type": "feature",
        "risk_level": "L2",
        "depends_on": [],
        "source_docs": [source_ref],
        "deliverables": ["confirmed work-case plan"],
        "evidence_required": [],
        "test_plan": [],
        "allowed_operation_level": "L0",
        "human_approval_required": True,
        "work_case": {"status": "draft", "source_ref": source_ref},
    }
    return create_planned_task(
        omo_dir,
        task_data=task_data,
        ingress_plane="projects/omo:work_case",
        source_ref=source_ref,
    )


def request_work_case_plan_confirmation(omo_dir: Path, *, case_id: str, plan_digest: str) -> dict[str, Any]:
    """Record a plan-confirmation request through OMO's contract broker."""
    from omo.omo_ingress_task_contract import record_task_contract_request

    request_ref = f".omo/workers/runs/{case_id}-work-case-plan-request.yaml"
    return record_task_contract_request(
        omo_dir,
        task_id=case_id,
        actor="projects/omo:work_case",
        request_ref=request_ref,
        request_record={"request_id": f"{case_id}-work-case-plan", "task_id": case_id, "plan_digest": plan_digest},
        source_ref=f"work-case:{case_id}:plan",
    )


@dataclass
class ExternalAction:
    action_id: str
    case_id: str
    action_type: str
    recipients: tuple[str, ...]
    content_digest: str
    attachment_digests: tuple[str, ...]
    approval_digest: str
    state: ExternalActionState = ExternalActionState.PROPOSED

    @classmethod
    def propose(
        cls,
        *,
        case_id: str,
        action_type: str,
        recipients: list[str],
        content_digest: str,
        attachment_digests: list[str],
    ) -> ExternalAction:
        snapshot = {
            "case_id": case_id,
            "action_type": action_type,
            "recipients": sorted(recipients),
            "content_digest": content_digest,
            "attachment_digests": sorted(attachment_digests),
        }
        approval_digest = (
            "sha256:"
            + sha256(dumps(snapshot, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()).hexdigest()
        )
        return cls(
            action_id=f"action-{uuid4().hex}",
            case_id=case_id,
            action_type=action_type,
            recipients=tuple(snapshot["recipients"]),
            content_digest=content_digest,
            attachment_digests=tuple(snapshot["attachment_digests"]),
            approval_digest=approval_digest,
        )

    def confirm(self, approval_digest: str) -> None:
        if approval_digest != self.approval_digest:
            raise ValueError("approval snapshot does not match the proposed external action")
        self.state = ExternalActionState.CONFIRMED

    def can_execute(self, approval_digest: str) -> bool:
        return self.state is ExternalActionState.CONFIRMED and approval_digest == self.approval_digest
