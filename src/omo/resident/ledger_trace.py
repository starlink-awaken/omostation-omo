"""resident-ledger-trace — 从 workflow-mesh 事件流确定性提取五问骨架 (BET-Y1Q3-T10-17).

读取统一事件流 `.omo/_knowledge/workflow-mesh/events.jsonl`, 按 ``workflow_run_id``
聚合为有序事件序列, 确定性提取 sediment 五问中「可确定」的部分:

- 计划 vs 实际: ``WorkflowRequested.payload.objective`` (计划) + step 事件序列 (实际)
- 结果与证据: 终态事件 (Succeeded/Closed/Failed) 的 ``ok``/``status``/``evidence_count``
- 失败根因: ``StepFailed``/``WorkflowFailed`` 的 ``error`` + ``step_name``
- 指标: 事件序列长度 + 首末事件时间差

语义项 (关键发现 / 交接建议 / 净增减) 不做自动撰写, 留给运营 agent/人工兜底。
不引入 LLM, 不编造内容。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

# 事件类型常量 (与 sediment.py 对齐)
REQUESTED_EVENTS = frozenset({"WorkflowRequested"})
# step 序列 (实际执行) — StepDispatched/StepStarted 也可能携带终态快照, 但仅取 step_name
STEP_EVENTS = frozenset({"StepStarted", "StepDispatched", "StepFailed"})
# 失败根因来源
FAILURE_EVENTS = frozenset({"StepFailed", "WorkflowFailed"})
# 终态事件 (结果与证据)
TERMINAL_EVENTS = frozenset({"WorkflowSucceeded", "WorkflowClosed", "WorkflowFailed"})


def _event_type(event: dict[str, Any]) -> str:
    return str(event.get("event_type") or "")


def _payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def iter_run_sequences(events_path: str | Path) -> dict[str, list[dict[str, Any]]]:
    """逐行读 events.jsonl, 按 workflow_run_id 分组为有序事件序列.

    保持文件序 (occurred_at 升序)。缺 workflow_run_id/trace_id 的事件直接跳过
    (无法归属 run, 不产出骨架)。返回 ``{run_id: [event, ...]}``。
    """
    sequences: dict[str, list[dict[str, Any]]] = {}
    with open(events_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue  # 容忍单行坏数据, 不中断整个索引
            if not isinstance(event, dict):
                continue
            run_id = str(event.get("workflow_run_id") or event.get("trace_id") or "")
            if not run_id:
                continue
            sequences.setdefault(run_id, []).append(event)
    return sequences


def extract_deterministic_five_q(sequence: list[dict[str, Any]]) -> dict[str, Any]:
    """从单个 run 的有序事件序列确定性提取五问骨架 dict.

    - ``objective``/``workflow_id``: 取首个 WorkflowRequested 的 payload (计划)
    - ``steps``: step 事件出现的 step_name 去重序列 (实际)
    - ``outcome``: 首个终态事件的 ok/status/evidence_count (结果与证据)
    - ``failure``: 首个 StepFailed/WorkflowFailed 的 step_name+error (失败根因)
    - ``metrics``: event_count + duration_s (首末 occurred_at 差, 秒)
    """
    run_id = ""
    workflow_id = None
    objective = None
    steps: list[str] = []
    outcome: dict[str, Any] | None = None
    failure: dict[str, Any] | None = None

    for event in sequence:
        if not run_id:
            run_id = str(event.get("workflow_run_id") or event.get("trace_id") or "")
        et = _event_type(event)
        payload = _payload(event)
        if et in REQUESTED_EVENTS:
            if not workflow_id:
                workflow_id = payload.get("workflow_id") or None
            if not objective:
                objective = payload.get("objective") or None
        elif et in STEP_EVENTS:
            step_name = payload.get("step_name")
            if step_name and str(step_name) not in steps:
                steps.append(str(step_name))
            if et in FAILURE_EVENTS and failure is None:
                failure = {
                    "step_name": str(step_name) if step_name else None,
                    "error": payload.get("error") or None,
                }
        elif et in TERMINAL_EVENTS and outcome is None:
            outcome = {
                "ok": payload.get("ok"),
                "status": payload.get("status") or None,
                "evidence_count": payload.get("evidence_count"),
            }

    timestamps = [_parse_ts(event.get("occurred_at")) for event in sequence]
    present = [ts for ts in timestamps if ts is not None]
    if len(present) >= 2:
        duration_s = round((present[-1] - present[0]).total_seconds(), 3)
    elif len(present) == 1:
        duration_s = 0.0
    else:
        duration_s = None

    return {
        "run_id": run_id,
        "workflow_id": workflow_id,
        "objective": objective,
        "steps": steps,
        "outcome": outcome,
        "failure": failure,
        "metrics": {"event_count": len(sequence), "duration_s": duration_s},
    }


def load_run_skeletons(events_path: str | Path) -> dict[str, dict[str, Any]]:
    """全量索引 events.jsonl → ``{run_id: 五问骨架}`` (promote 调用一次, 内存缓存)."""
    sequences = iter_run_sequences(events_path)
    return {run_id: extract_deterministic_five_q(seq) for run_id, seq in sequences.items()}
