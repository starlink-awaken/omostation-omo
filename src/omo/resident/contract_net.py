"""resident/contract_net.py — 合同网协议 (CNP) 任务竞标 (BET-Y1Q4-T7-06).

多 Agent 任务自主竞标: 经理广播任务 → 合格 agent 投标 → 确定性评分
→ 中标/兜底。零模型调用, 全部为内存计算。

守 ledger 约束:
- 资质门槛: skills 未覆盖全部 required_skills 的投标一律拒绝
  (不允许无资质盲目投标)。
- Token 预算 (circuit breaker): 评估消耗累计 > 500 token → 强制截断,
  转默认 Archetype 兜底指派。
- 超时兜底: bid_window 内无中标者 → Fallback——兜底率 100%
  (不存在无超时的无限等待)。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

SCHEMA = "omo.resident.contract_net.v1"

# circuit breaker: 评估消耗累计上限 (token)
TOKEN_BUDGET = 500
# 超时兜底窗口 (秒)
DEFAULT_BID_WINDOW_S = 3.0
# 单次评估耗时上限 (秒, done_when)
EVAL_BUDGET_S = 0.300

# 评分权重: 覆盖率主导, 置信次之, 时长/成本为惩罚项
_W_COVERAGE = 2.0
_W_CONFIDENCE = 1.5
_W_DURATION = 1.0
_W_TOKEN = 0.5

FALLBACK_ARCHETYPE = "archetype-default"


class ContractNetError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class TaskAnnouncement:
    """经理广播的任务公告."""

    task_id: str
    required_skills: list[str]
    bid_window_s: float = DEFAULT_BID_WINDOW_S
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class Bid:
    """agent 投标."""

    agent_id: str
    skills: list[str]
    token_cost: float = 10.0
    confidence: float = 0.5
    estimated_duration_s: float = 60.0


@dataclass
class Award:
    """中标结果."""

    task_id: str
    agent_id: str
    score: float
    token_spent: float
    fallback: bool = False


def eligibility(bid: Bid, announcement: TaskAnnouncement) -> tuple[bool, str]:
    """资质门槛: skills 覆盖全部 required_skills 才可投标."""
    missing = [s for s in announcement.required_skills if s not in bid.skills]
    if missing:
        return False, f"missing skills: {missing}"
    return True, ""


def score_bid(bid: Bid, announcement: TaskAnnouncement) -> float:
    """确定性评分: 覆盖率 + 置信度 − 时长/成本惩罚."""
    required = announcement.required_skills or []
    coverage = (len([s for s in required if s in bid.skills]) / len(required)) if required else 1.0
    token_norm = min(1.0, bid.token_cost / TOKEN_BUDGET)
    duration_norm = bid.estimated_duration_s / (1.0 + bid.estimated_duration_s)
    return round(
        _W_COVERAGE * coverage
        + _W_CONFIDENCE * bid.confidence
        + _W_DURATION * (1.0 - duration_norm)
        - _W_TOKEN * token_norm,
        4,
    )


@dataclass
class EvaluationTrace:
    """单次评估轨迹 (审计用)."""

    rejected: list[tuple[str, str]] = field(default_factory=list)  # (agent_id, reason)
    evaluated: list[tuple[str, float]] = field(default_factory=list)
    truncated: bool = False
    elapsed_s: float = 0.0


def evaluate(
    announcement: TaskAnnouncement,
    bids: list[Bid],
    *,
    elapsed_s: float | None = None,
    token_budget: float = TOKEN_BUDGET,
) -> Award | None:
    """评分并产生中标者; 无合格中标者返回 None (调用方走超时兜底).

    - 资质门槛先拒绝; 评估消耗累计超 token_budget → 截断。
    - elapsed_s ≥ bid_window_s 时同样返回 None (超时, 由调用方兜底)。
    """
    started = time.perf_counter()
    trace = EvaluationTrace()
    spent = 0.0
    best: tuple[float, Bid] | None = None
    window = announcement.bid_window_s
    for bid in sorted(bids, key=lambda b: b.agent_id):
        ok, reason = eligibility(bid, announcement)
        if not ok:
            trace.rejected.append((bid.agent_id, reason))
            continue
        if elapsed_s is not None and elapsed_s >= window:
            break  # 窗口已关, 停止评估 (超时兜底由调用方执行)
        if spent + bid.token_cost > token_budget:
            trace.truncated = True
            break  # circuit breaker: 强制截断 → 兜底
        spent += bid.token_cost
        s = score_bid(bid, announcement)
        trace.evaluated.append((bid.agent_id, s))
        if best is None or s > best[0]:
            best = (s, bid)
    trace.elapsed_s = time.perf_counter() - started
    if best is None:
        return None
    return Award(
        task_id=announcement.task_id,
        agent_id=best[1].agent_id,
        score=best[0],
        token_spent=round(spent, 2),
    )


def evaluate_with_fallback(
    announcement: TaskAnnouncement,
    bids: list[Bid],
    *,
    elapsed_s: float | None = None,
    fallback_agent: str = FALLBACK_ARCHETYPE,
) -> Award:
    """评估 + 兜底: 无中标者 (含截断/超时) 时按默认 Archetype 指派 — 兜底率 100%."""
    award = evaluate(announcement, bids, elapsed_s=elapsed_s)
    if award is not None:
        return award
    return Award(
        task_id=announcement.task_id,
        agent_id=fallback_agent,
        score=0.0,
        token_spent=0.0,
        fallback=True,
    )


class ContractNetManager:
    """经理侧: 广播 → 收标 → 评标 → 授标/兜底."""

    def __init__(self, fallback_agent: str = FALLBACK_ARCHETYPE) -> None:
        self.fallback_agent = fallback_agent
        self.awards: dict[str, Award] = {}

    def announce_and_award(
        self,
        announcement: TaskAnnouncement,
        bids: list[Bid],
        *,
        elapsed_s: float | None = None,
    ) -> Award:
        award = evaluate_with_fallback(announcement, bids, elapsed_s=elapsed_s, fallback_agent=self.fallback_agent)
        self.awards[announcement.task_id] = award
        return award
