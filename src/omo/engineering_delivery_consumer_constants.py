"""Shared constants for engineering delivery consumer."""

from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path

CONSUMPTION_SCHEMA = "engineering-delivery-consumption/v1"
REVIEW_SCHEMA = "engineering-delivery-review/v1"
REVIEW_QUEUE_SCHEMA = "engineering-delivery-review-queue/v1"
QUALIFIED_DECISION_OUTCOME_SCHEMA = "qualified-decision-outcome/v1"
SHADOW_OBSERVER_SCHEMA = "engineering-delivery-shadow-observer/v1"
QUALIFIED_DECISION_OUTCOME_LOG = Path("_knowledge/workflow-mesh/engineering-delivery-decision-outcomes.jsonl")
MOS_PROJECTION_RECEIPT_LOG = Path("_knowledge/workflow-mesh/engineering-delivery-mos-projections.jsonl")
_SHADOW_OBSERVER_INPUT_MAX_BYTES = 64 * 1024 * 1024
_SHADOW_OBSERVER_TOTAL_MAX_BYTES = 128 * 1024 * 1024

SCENE_BINDING = {
    "scene_id": "engineering-delivery",
    "journey_id": "intent-to-evidence",
    "outcome_metric": "verified_delivery_lead_time",
}
SCENE_POLICY = {
    "scene_id": "engineering-delivery",
    "tier": "shadow",
    "value_indicator_policy": False,
}
CONTROLS = {
    "proposal_only": True,
    "activation": "forbidden",
    "workflow_run_creation": False,
    "provider_invocation": False,
    "automatic_promotion": False,
    "personal_value_attribution": False,
}

_DELIVERY_FIELDS = frozenset(
    {
        "delivery_id",
        "repository_ref",
        "pr_url",
        "merge_sha",
        "requested_at",
        "merged_at",
        "evidence_refs",
    }
)
_REVIEW_FIELDS = frozenset({"delivery_id", "decision", "evidence_refs"})
_DECISIONS = frozenset({"reviewed", "adopted", "rejected"})
_FORBIDDEN_KEY_PARTS = frozenset(
    {
        "content",
        "credential",
        "document",
        "input",
        "output",
        "password",
        "path",
        "raw",
        "reviewdecision",
        "secret",
        "token",
        "verdict",
    }
)
_OPAQUE_REF_SCHEMES = frozenset({"evidence", "github", "ci", "workflow", "receipt"})
_HUMAN_ACTOR_SCHEMES = frozenset({"human", "operator", "principal"})
_NON_HUMAN_EVIDENCE = re.compile(
    r"(?:^|[/_.:-])(test|synthetic|user[-_]?provided)(?:$|[/_.:-])",
    re.IGNORECASE,
)
_PRINCIPAL_ASSERTION_SCHEMA = "cockpit-human-principal-assertion/v2"
_ENGINEERING_REVIEW_SIGNING_KEY_ENV = "COCKPIT_ENGINEERING_REVIEW_SIGNING_KEY"
_ENGINEERING_REVIEW_SIGNING_KEY_FILE = "_knowledge/workflow-mesh/engineering-review-signing.key"
_PRINCIPAL_ASSERTION_MAX_AGE = timedelta(minutes=5)
