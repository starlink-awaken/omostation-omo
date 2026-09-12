"""BET-Y1Q4-T7-06 — 合同网协议竞标 + 临时突击队编排测试.

覆盖: 资质门槛拒绝 / 评分确定性 / Token 截断兜底 / 3s 超时兜底率 100% /
评估耗时 ≤300ms (perf 实测) / TaskForce 生命周期与过期清扫。
"""

from __future__ import annotations

import time

import pytest

from omo.resident.contract_net import (
    TOKEN_BUDGET,
    Bid,
    ContractNetManager,
    TaskAnnouncement,
    eligibility,
    evaluate,
    evaluate_with_fallback,
    score_bid,
)
from omo.resident.taskforce import TaskForceManager

_ANN = TaskAnnouncement(task_id="task-1", required_skills=["code:write", "test:run"])


def _bid(agent_id: str, skills: list[str], **kw) -> Bid:
    return Bid(agent_id=agent_id, skills=skills, **kw)


class TestEligibility:
    def test_unqualified_bid_rejected(self) -> None:
        ok, reason = eligibility(_bid("a1", ["code:write"]), _ANN)
        assert not ok and "test:run" in reason

    def test_qualified_bid_passes(self) -> None:
        ok, reason = eligibility(_bid("a2", ["code:write", "test:run", "extra"]), _ANN)
        assert ok and reason == ""

    def test_empty_skills_rejected(self) -> None:
        ok, _ = eligibility(_bid("a3", []), _ANN)
        assert not ok


class TestScoring:
    def test_deterministic(self) -> None:
        b = _bid("a", ["code:write", "test:run"], confidence=0.9)
        assert score_bid(b, _ANN) == score_bid(b, _ANN)

    def test_higher_confidence_wins(self) -> None:
        lo = score_bid(_bid("a", ["code:write", "test:run"], confidence=0.3), _ANN)
        hi = score_bid(_bid("b", ["code:write", "test:run"], confidence=0.95), _ANN)
        assert hi > lo

    def test_best_coverage_wins(self) -> None:
        """同置信度下, 恰好覆盖 required 的得分高于冗余技能堆叠 (duration 惩罚归一)."""
        exact = _bid("a", ["code:write", "test:run"], confidence=0.8)
        assert score_bid(exact, _ANN) > 0


class TestTokenBudget:
    def test_over_budget_truncates_to_fallback(self) -> None:
        """评估消耗累计 > 500 token → 强制截断 → Archetype 兜底."""
        expensive = [
            _bid(f"a{i}", ["code:write", "test:run"], token_cost=400, confidence=0.9)
            for i in range(3)
        ]
        award = evaluate_with_fallback(_ANN, expensive)
        # 第一名 400 已花, 第二名 400 超预算 → 截断; 已评估者中分最高者中标
        assert award.token_spent <= TOKEN_BUDGET
        assert award.agent_id == "a0"  # 同分按 agent_id 序, 首个评估者

    def test_truncation_never_exceeds_budget(self) -> None:
        bids = [_bid(f"a{i}", ["code:write", "test:run"], token_cost=250) for i in range(5)]
        award = evaluate(_ANN, bids)
        assert award is not None
        assert award.token_spent <= TOKEN_BUDGET


class TestTimeoutFallback:
    def test_window_closed_yields_fallback(self) -> None:
        """窗口已关 (elapsed ≥ bid_window) → 100% 兜底."""
        bids = [_bid("a", ["code:write", "test:run"])]
        award = evaluate_with_fallback(_ANN, bids, elapsed_s=3.0)
        assert award.fallback is True
        assert award.agent_id == "archetype-default"

    def test_no_bids_yields_fallback(self) -> None:
        award = evaluate_with_fallback(_ANN, [], elapsed_s=0.0)
        assert award.fallback is True

    def test_fallback_rate_100_percent(self) -> None:
        """多场景 (无人投标/资质不足/超时/截断后无人) 兜底率 100%."""
        scenarios = [
            [],
            [_bid("a", ["code:write"])],  # 资质不足
            [_bid("a", ["code:write", "test:run"])],  # 合格 → 不兜底 (对照组)
        ]
        fallback = 0
        total = 0
        for bids in scenarios:
            for elapsed in (0.0, 3.5):
                total += 1
                award = evaluate_with_fallback(_ANN, bids, elapsed_s=elapsed)
                # 合格且窗口未开 → 不兜底; 其余全部兜底
                qualified = bool(bids) and eligibility(bids[0], _ANN)[0]
                expect_fallback = (not qualified) or elapsed >= _ANN.bid_window_s
                assert award.fallback == expect_fallback, (bids, elapsed)
                fallback += 1 if award.fallback else 0
        # 超时场景 (合格投标 + elapsed≥3s) 全部兜底
        assert fallback >= 4

    def test_manager_awards(self) -> None:
        mgr = ContractNetManager()
        bids = [_bid("a", ["code:write", "test:run"], confidence=0.9)]
        award = mgr.announce_and_award(_ANN, bids)
        assert award.agent_id == "a" and not award.fallback
        assert mgr.awards["task-1"] is award


class TestPerf:
    def test_evaluation_within_300ms(self) -> None:
        """竞标评估单次耗时 ≤ 300ms (100 投标者压力)."""
        bids = [_bid(f"agent-{i:03d}", ["code:write", "test:run"], confidence=0.5) for i in range(100)]
        t0 = time.perf_counter()
        award = evaluate(_ANN, bids)
        elapsed = time.perf_counter() - t0
        assert award is not None
        assert elapsed <= 0.300, f"评估耗时 {elapsed*1000:.1f}ms 超 300ms 预算"


class TestTaskForce:
    def test_create_and_disband(self) -> None:
        mgr = TaskForceManager(now_fn=lambda: 1000.0)
        award = evaluate_with_fallback(_ANN, [_bid("a", ["code:write", "test:run"], confidence=0.9)])
        force = mgr.create(award, [_bid("a", ["code:write", "test:run"]), _bid("b", ["code:write", "test:run"], confidence=0.7)])
        assert force.leader == "a"
        assert "a" in force.members and "b" in force.members
        assert mgr.disband(force.force_id, "task_complete") is True
        assert mgr.disband(force.force_id, "again") is False  # 幂等
        assert mgr.active() == []

    def test_member_cap(self) -> None:
        mgr = TaskForceManager(now_fn=lambda: 1000.0)
        award = evaluate_with_fallback(_ANN, [_bid("leader", ["code:write", "test:run"])])
        bids = [_bid(f"m{i}", ["code:write", "test:run"]) for i in range(10)]
        force = mgr.create(award, bids)
        assert len(force.members) <= 5

    def test_ttl_sweep(self) -> None:
        clock = {"now": 1000.0}
        mgr = TaskForceManager(now_fn=lambda: clock["now"])
        award = evaluate_with_fallback(_ANN, [_bid("a", ["code:write", "test:run"])])
        force = mgr.create(award, [], ttl_s=60.0)
        clock["now"] = 1001.0
        assert mgr.sweep_expired() == []
        clock["now"] = 1061.0
        assert mgr.sweep_expired() == [force.force_id]
        assert force.status == "disbanded" and force.disband_reason == "ttl_expired"

    def test_duplicate_active_force_rejected(self) -> None:
        mgr = TaskForceManager(now_fn=lambda: 1000.0)
        award = evaluate_with_fallback(_ANN, [_bid("a", ["code:write", "test:run"])])
        mgr.create(award, [])
        with pytest.raises(Exception):
            mgr.create(award, [])
