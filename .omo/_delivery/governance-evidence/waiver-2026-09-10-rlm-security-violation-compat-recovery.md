---
schema_version: governance-waiver-evidence/v1
status: active
owner: human-principal
lifecycle: history
created: 2026-09-09
last-reviewed: 2026-09-09
value_indicator_policy: false
title: RLM SecurityViolation public identity compatibility recovery
type: doc
---

# RLM SecurityViolation Compatibility Recovery Waiver

## Temporary principal delegation

Verbatim authority:

> 我要去休息了，针对上述这种精细化的授权，我估计暂时不能给你处理，直到明天上午10点之前，所有相关授权，你来自主处理，做好备案即可。

The delegation is interpreted in Asia/Shanghai and expires at
`2026-09-10T10:00:00+08:00`. It authorizes this reversible child-repository
baseline repair because the defect blocks the already accepted A2 delivery.
It does not authorize force/history rewrite, Ledger/BET or completion/value
mutation, root gitlink change, host/runtime action, or a false green claim.

Frontmatter dates use the UTC calendar date of creation and review. The
filename uses the contemporaneous Asia/Shanghai date; these are the same
event, not an earlier review of a later artifact.

`AGCP_REQUIREMENT_ITERATION_GATE=0` was used only as the process-local prefix
for fresh unbound workflow start
`20260909T174410Z-project-code-change-38f67156`. Claims, tests, verification,
Git, CI and closeout use default policy.

## Trigger and diagnosis

A2 child PR #150 had an exact four-path implementation and passed its focused
RED/GREEN, unit, Ruff, workflow and independent reviews. Required child CI run
`34383802691` failed both `test` and `test-cov` during collection, before A2
tests executed:

- `tests/test_rlm_governance.py` imported `SecurityViolation`;
- `src/omo/resident/rlm_governance.py` defined and raised only
  `SecurityViolationError`.

The same ImportError was reproduced from clean child main
`db7217913dc95f727f26d66b9f7df5675404077b`. Neither failing path belongs to
the A2 WorkPacket, so #150 was preserved and closed rather than expanding its
scope. Its branch, source commit `dfc5921d84a1dea62dbd132bda1757cd6fd1a153`,
annotated tag, CI logs and review evidence remain intact.

History shows that a formatting change renamed only the production exception
while leaving the public test/API spelling unchanged. A test-only rename would
hide the compatibility regression; renaming the class back would remove the
newer spelling. The minimum compatible repair keeps the current class and
raise site and binds the old public name to the same exception identity:

```python
SecurityViolation = SecurityViolationError
```

## Exact scope

1. `src/omo/resident/rlm_governance.py`
2. `tests/test_rlm_governance.py`
3. this waiver

No other child or root path is authorized.

## Clone and workflow identity

- Root frozen base: `0941c21d5eca3580af928d6a930afded8645f676`
- Child frozen base: `db7217913dc95f727f26d66b9f7df5675404077b`
- Full successor:
  `/Users/xiamingxing/agents/codex-agent-os-recovery/attempts/a2-rlm-baseline-recovery-20260910-02/ws`
- Root branch:
  `agent/codex-agent-os-recovery--a2-rlm-baseline-recovery-20260910-02`
- Provenance: `ready /
  e10fcbc1070ea72ff9675f6ffd8b5365f528b7f66daf04ecfa7a2ac0ade11d2d`
- Readiness: `ready /
  a7e90c76b30e31dd26fcbd6c9fc523e7f34394a09e80a9351736a3994c8009f8`
- Affected-graph receipt:
  `73df86a20e66b537d851414a3350c24d1b71cd68727353642b2d3659fbaeab98`

The preceding custom-profile attempt
`a2-rlm-baseline-recovery-20260910-01` was intentionally denied writer
admission. It has no workflow, claim, edit, commit, tag, push or PR. Standard
`abort-unready` was attempted but failed closed on tracked active-workflow
history. The clean clone is retained as explicit evidence; it is not reused as
a writer and does not block this full successor.

## TDD and verification evidence

The public identity contract was added before production code. On the exact
child baseline it failed at collection with the expected missing
`SecurityViolation` import. After the one-line alias:

- focused RLM governance: `39 passed`;
- governance/kernel/subagent regression with `pytest-asyncio`: `108 passed`;
- child-CI-equivalent Python 3.13 suite outcome: `2300 passed, 202 skipped,
  1 deselected`;
- Ruff check and format: PASS.

An intermediate regression command intentionally disabled plugin autoload and
therefore produced 19 unsupported-async failures. That result is invalid as a
product verdict; rerunning with the child CI async plugin contract produced the
108-pass result above.

The full suite also appended timestamped test fixtures to four tracked
`.omo/state/agent-cell*` files. The diffs were inspected, attributed to test
data, and restored exactly to child HEAD before publication; none is staged or
included in this recovery. That write-through behavior remains separate test
purity debt and is not silently folded into this scope.

It also created the ignored runtime-like test artifact
`.omo/state/agent-cell-memory/conflicts.jsonl` through
`MemoryPipeline.detect_conflicts()`: four JSONL records, SHA-256
`ff2d308c6b9db3074b7cb4ab97e47cb0102967997c2366c9a6adb2412da35aba`,
timestamped `2026-09-09T17:48:09Z`. After inspection and attribution, that one
generated file was deleted. A post-cleanup ignored-state scan found no
remaining ignored artifact under `.omo/state`.

Ignored Python/Ruff/pytest cache artifacts remain while verification is still
active. They are not tracked or part of the PR. Their targeted cleanup and a
zero-cache recount are explicit preconditions for controlled clone retirement,
not evidence claimed at publication time.

Child CI remains authoritative and must independently reproduce the green
result before merge. A successful baseline PR does not complete A2, change its
candidate/evaluating state, prove operational/value outcomes, or authorize the
later root pointer transaction.

## Stop and rollback

Stop without publication if the diff exceeds three paths, the alias is not
object-identical, the existing raise behavior changes, root or child main
drifts before publication, another writer overlaps these paths, a default hook
or CI gate rejects the repair, or any Ledger/BET/value/root-pointer claim would
be inferred. Rollback is the child PR revert of the alias and contract test;
no data or runtime migration exists.
