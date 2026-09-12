"""resident/taskforce.py — 临时突击队动态自组织编排 (BET-Y1Q4-T7-06).

任务中标后按需组队 (leader = 中标者, members = 合格投标人择优),
任务完成/超时/主动释放即解散; 解散幂等, 过期清扫无副作用。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from omo.resident.contract_net import Award, Bid, ContractNetError

SCHEMA = "omo.resident.taskforce.v1"

DEFAULT_TTL_S = 3600.0
MAX_MEMBERS = 5


@dataclass
class TaskForce:
    """临时突击队."""

    force_id: str
    task_id: str
    leader: str
    members: list[str]
    created_at: float
    ttl_s: float = DEFAULT_TTL_S
    status: str = "active"  # active | disbanded
    disband_reason: str = ""
    meta: dict[str, Any] = field(default_factory=dict)

    def expired(self, now: float) -> bool:
        return self.status == "active" and (now - self.created_at) >= self.ttl_s


class TaskForceManager:
    """突击队生命周期: 组建 / 解散 / 过期清扫 (全部幂等)."""

    def __init__(self, *, now_fn=None, max_members: int = MAX_MEMBERS) -> None:
        import time as _time

        self._now = now_fn or _time.time
        self._max_members = max_members
        self._forces: dict[str, TaskForce] = {}
        self._seq = 0

    def create(self, award: Award, bids: list[Bid], *, ttl_s: float = DEFAULT_TTL_S) -> TaskForce:
        """按中标结果组建: leader = 中标者, members = 合格投标人按评分择优."""
        if self._forces.get(f"force-{award.task_id}") and self._forces[f"force-{award.task_id}"].status == "active":
            raise ContractNetError("force_exists", f"active taskforce already exists for {award.task_id}")
        scored = sorted(
            ((b.agent_id, b) for b in bids if b.agent_id != award.agent_id),
            key=lambda ab: (-ab[1].confidence, ab[0]),
        )
        members = [award.agent_id] + [agent_id for agent_id, _ in scored[: self._max_members - 1]]
        self._seq += 1
        force = TaskForce(
            force_id=f"force-{award.task_id}",
            task_id=award.task_id,
            leader=award.agent_id,
            members=members,
            created_at=self._now(),
            ttl_s=ttl_s,
            meta={"award_score": award.score, "fallback": award.fallback},
        )
        self._forces[force.force_id] = force
        return force

    def get(self, force_id: str) -> TaskForce | None:
        return self._forces.get(force_id)

    def active(self) -> list[TaskForce]:
        return [f for f in self._forces.values() if f.status == "active"]

    def disband(self, force_id: str, reason: str = "task_complete") -> bool:
        """解散 (幂等): 已解散返回 False."""
        force = self._forces.get(force_id)
        if force is None or force.status != "active":
            return False
        force.status = "disbanded"
        force.disband_reason = reason
        return True

    def sweep_expired(self) -> list[str]:
        """清扫过期突击队 (ttl 到期自动解散); 返回本轮解散的 force_id."""
        now = self._now()
        swept: list[str] = []
        for force in self._forces.values():
            if force.expired(now):
                force.status = "disbanded"
                force.disband_reason = "ttl_expired"
                swept.append(force.force_id)
        return swept
