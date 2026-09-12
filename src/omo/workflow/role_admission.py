"""role_admission.py — Role 准入状态注册（BET-Y1Q4-T10-165）。

在 sovereignty Role 之上叠加准入状态机（不动 sovereignty 核心模型）：
observer → r0_canary → as0 → admitted。铁律执行：未过门 Role 的
adapter 零写入、零自治、零扩并发——can_act() 是所有写路径的前置门卫。

状态文件：.omo/_truth/registry/role-admission.yaml（根仓注册表，
本模块只读校验 + 判定；登记走治理流程）。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

VALID_STATES = ("observer", "r0_canary", "as0", "admitted")
# 状态机：只能前进
_TRANSITIONS = {
    "observer": {"r0_canary"},
    "r0_canary": {"as0", "observer"},  # 失败可退回观察
    "as0": {"admitted", "observer"},
    "admitted": set(),
}


class AdmissionError(ValueError):
    """准入状态非法。"""


@dataclass
class RoleAdmission:
    role_id: str
    state: str
    adapter: str
    evidence_ref: str  # receipt/digest，未过门可为空串

    @property
    def can_write(self) -> bool:
        return self.state == "admitted"

    @property
    def can_autonomous(self) -> bool:
        return self.state == "admitted"

    @property
    def can_scale_concurrency(self) -> bool:
        return self.state == "admitted"


def load_registry(path: Path) -> dict[str, RoleAdmission]:
    if not path.is_file():
        return {}
    doc = yaml.safe_load(path.read_text()) or {}
    out = {}
    for item in doc.get("roles", []):
        ra = RoleAdmission(
            role_id=str(item["role_id"]),
            state=str(item["state"]),
            adapter=str(item.get("adapter", "")),
            evidence_ref=str(item.get("evidence_ref", "")),
        )
        if ra.state not in VALID_STATES:
            raise AdmissionError(f"{ra.role_id}: illegal state {ra.state}")
        out[ra.role_id] = ra
    return out


def can_act(registry: dict[str, RoleAdmission], role_id: str, action: str) -> bool:
    """写路径门卫：未过门零写入/零自治/零扩并发。

    action ∈ {write, autonomous, scale}。未知 role 一律拒绝（fail-closed）。
    """
    ra = registry.get(role_id)
    if ra is None:
        return False
    return {
        "write": ra.can_write,
        "autonomous": ra.can_autonomous,
        "scale": ra.can_scale_concurrency,
    }.get(action, False)


def check_transition(current: str, target: str) -> None:
    if target not in _TRANSITIONS.get(current, set()):
        raise AdmissionError(f"illegal transition: {current} -> {target}")


def render_gate_report(registry: dict[str, RoleAdmission]) -> dict[str, Any]:
    """供 ASD agents 面板/驾驶舱呈现：每 role 可见、未过门原因可见。"""
    return {
        "schema": "role-admission-report/v1",
        "roles": [
            {
                "role_id": ra.role_id,
                "state": ra.state,
                "adapter": ra.adapter,
                "can_write": ra.can_write,
                "blocked_reason": None if ra.can_write else f"state={ra.state}（未过门：零写入/零自治/零扩并发）",
                "evidence_ref": ra.evidence_ref or None,
            }
            for ra in sorted(registry.values(), key=lambda r: r.role_id)
        ],
    }
