"""Work-case domain primitives for controlled external actions.

This module deliberately contains no channel client.  Adapters may execute an
action only after OMO has matched the operator-approved immutable snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from json import dumps
from uuid import uuid4


class ExternalActionState(StrEnum):
    PROPOSED = "proposed"
    CONFIRMED = "confirmed"


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
