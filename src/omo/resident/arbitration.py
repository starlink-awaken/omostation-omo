"""arbitration.py — 多 Agent 先例仲裁引擎 (BET-Y1Q4-T5-05).

确定性算法, 零模型调用:
  - 按冲突签名相似度检索先例
  - 多数一致性 + 先例置信度合成裁决
  - circuit breaker: confidence < 0.85 → escalate_hitl (严禁低置信度盲目合并)

设计原则:
  - 确定性: 相同输入产生相同输出
  - 可审计: 每个裁决记录 basis (引用的先例 ID)
  - 安全: 低置信度自动升级人类审批
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from omo.resident import WORKSPACE

# ── 常量 ──────────────────────────────────────────────

PRECEDENTS_DIR = WORKSPACE / ".omo" / "state" / "decision-graph" / "precedents.jsonl"

CONFIDENCE_THRESHOLD = 0.85  # circuit breaker 阈值
SIMILARITY_THRESHOLD = 0.6   # 签名相似度最低阈值


# ── 数据模型 ──────────────────────────────────────────


@dataclass
class Precedent:
    """历史判例 — 冲突签名 + 裁决结果.

    Attributes:
        precedent_id: 唯一标识符
        signature: 冲突签名 (用于相似度匹配)
        resolution: 裁决结果
        confidence: 先例置信度 [0.0, 1.0]
        outcome: 实际结果 (用于评估)
        created_at: 创建时间
        context: 附加上下文
    """

    signature: str
    resolution: str
    confidence: float = 0.9
    outcome: str = ""
    precedent_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    created_at: str = ""
    context: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence must be in [0.0, 1.0], got {self.confidence}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "precedent_id": self.precedent_id,
            "signature": self.signature,
            "resolution": self.resolution,
            "confidence": self.confidence,
            "outcome": self.outcome,
            "created_at": self.created_at,
            "context": self.context,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Precedent:
        return cls(
            signature=d.get("signature", ""),
            resolution=d.get("resolution", ""),
            confidence=d.get("confidence", 0.9),
            outcome=d.get("outcome", ""),
            precedent_id=d.get("precedent_id"),
            created_at=d.get("created_at", ""),
            context=d.get("context", {}),
        )


@dataclass
class Conflict:
    """冲突描述 — 多 Agent 分歧场景.

    Attributes:
        conflict_id: 唯一标识符
        signature: 冲突签名 (用于匹配先例)
        agents: 参与仲裁的 agent 列表
        positions: agent → position 映射
        context: 附加上下文 (resource, action 等)
    """

    signature: str
    agents: list[str] = field(default_factory=list)
    positions: dict[str, str] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)
    conflict_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    def to_dict(self) -> dict[str, Any]:
        return {
            "conflict_id": self.conflict_id,
            "signature": self.signature,
            "agents": self.agents,
            "positions": self.positions,
            "context": self.context,
        }


@dataclass
class ArbitrationResult:
    """仲裁裁决结果.

    Attributes:
        resolution: 裁决结果
        confidence: 合成置信度 [0.0, 1.0]
        basis: 引用的先例 ID 列表
        escalated: 是否触发 circuit breaker 升级
        escalation_reason: 升级原因 (仅当 escalated=True)
    """

    resolution: str
    confidence: float
    basis: list[str] = field(default_factory=list)
    escalated: bool = False
    escalation_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "resolution": self.resolution,
            "confidence": self.confidence,
            "basis": self.basis,
            "escalated": self.escalated,
            "escalation_reason": self.escalation_reason,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ArbitrationResult:
        return cls(
            resolution=d.get("resolution", ""),
            confidence=d.get("confidence", 0.0),
            basis=d.get("basis", []),
            escalated=d.get("escalated", False),
            escalation_reason=d.get("escalation_reason", ""),
        )


# ── 仲裁引擎 ──────────────────────────────────────────


class PrecedentArbiter:
    """多 Agent 先例仲裁器.

    确定性算法:
      1. 按签名相似度检索匹配先例
      2. 多数一致性投票 (权重 = 先例置信度)
      3. 合成裁决置信度
      4. circuit breaker: confidence < 0.85 → escalate_hitl

    严禁低置信度盲目合并。
    """

    def __init__(
        self,
        precedents_file: Path | None = None,
        confidence_threshold: float = CONFIDENCE_THRESHOLD,
        similarity_threshold: float = SIMILARITY_THRESHOLD,
    ) -> None:
        self._path = Path(precedents_file) if precedents_file else PRECEDENTS_DIR
        self._threshold = confidence_threshold
        self._sim_threshold = similarity_threshold
        self._precedents: list[Precedent] = []
        self._load_precedents()

    # ── 先例管理 ────────────────────────────────────

    def _load_precedents(self) -> None:
        """从 precedents.jsonl 加载历史先例."""
        if not self._path.exists():
            return
        with open(self._path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    self._precedents.append(Precedent.from_dict(d))
                except (json.JSONDecodeError, ValueError):
                    continue

    def _save_precedent(self, precedent: Precedent) -> None:
        """追加先例到文件."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(json.dumps(precedent.to_dict(), ensure_ascii=False) + "\n")

    def add_precedent(
        self,
        signature: str,
        resolution: str,
        confidence: float = 0.9,
        outcome: str = "",
        context: dict[str, Any] | None = None,
        precedent_id: str | None = None,
    ) -> Precedent:
        """添加历史先例.

        Args:
            signature: 冲突签名
            resolution: 裁决结果
            confidence: 先例置信度
            outcome: 实际结果
            context: 附加上下文
            precedent_id: 可选显式 ID

        Returns:
            创建的 Precedent
        """
        precedent = Precedent(
            signature=signature,
            resolution=resolution,
            confidence=confidence,
            outcome=outcome,
            precedent_id=precedent_id,
            context=context or {},
        )
        self._precedents.append(precedent)
        self._save_precedent(precedent)
        return precedent

    def load_precedents(self, file: Path | None = None) -> int:
        """从文件加载先例 (可追加).

        Returns:
            加载的先例数量
        """
        target = file or self._path
        if not target.exists():
            return 0
        loaded = 0
        with open(target, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    self._precedents.append(Precedent.from_dict(d))
                    loaded += 1
                except (json.JSONDecodeError, ValueError):
                    continue
        return loaded

    # ── 仲裁核心 ────────────────────────────────────

    def arbitrate(self, conflict: Conflict, precedents: list[Precedent] | None = None) -> ArbitrationResult:
        """执行先例仲裁.

        算法:
          1. 检索与冲突签名相似度 >= threshold 的先例
          2. 按 resolution 分组, 计算加权多数 (权重 = 先例置信度)
          3. 合成置信度 = 加权多数比例
          4. circuit breaker: 合成置信度 < threshold → escalate_hitl

        Args:
            conflict: 冲突描述
            precedents: 可选的先例列表 (默认使用内部存储)

        Returns:
            ArbitrationResult
        """
        pool = precedents if precedents is not None else self._precedents

        # 1. 检索匹配先例
        matched = self._retrieve_matchers(conflict.signature, pool)

        if not matched:
            # 无匹配先例 → 低置信度, 升级
            return ArbitrationResult(
                resolution="unknown",
                confidence=0.0,
                basis=[],
                escalated=True,
                escalation_reason="no_matching_precedents",
            )

        # 2. 加权多数投票
        resolution_scores: dict[str, float] = {}
        for p in matched:
            resolution_scores[p.resolution] = resolution_scores.get(p.resolution, 0.0) + p.confidence

        # 选出最高分的 resolution
        if not resolution_scores:
            return ArbitrationResult(
                resolution="unknown",
                confidence=0.0,
                basis=[],
                escalated=True,
                escalation_reason="no_valid_scores",
            )

        best_resolution = max(resolution_scores, key=resolution_scores.get)
        best_score = resolution_scores[best_resolution]
        total_score = sum(resolution_scores.values())
        synthetic_confidence = best_score / total_score if total_score > 0 else 0.0

        # 3. 合成置信度调整 (考虑匹配先例的平均置信度)
        avg_confidence = sum(p.confidence for p in matched) / len(matched)
        final_confidence = (synthetic_confidence * 0.7 + avg_confidence * 0.3)

        # 4. Circuit breaker 检查
        basis = [p.precedent_id for p in matched if p.resolution == best_resolution]

        if final_confidence < self._threshold:
            return ArbitrationResult(
                resolution=best_resolution,
                confidence=final_confidence,
                basis=basis,
                escalated=True,
                escalation_reason=f"confidence_below_threshold:{final_confidence:.2f}<{self._threshold}",
            )

        return ArbitrationResult(
            resolution=best_resolution,
            confidence=final_confidence,
            basis=basis,
            escalated=False,
        )

    def _retrieve_matchers(self, signature: str, precedents: list[Precedent]) -> list[Precedent]:
        """按签名相似度检索先例.

        相似度计算: Jaccard 系数 (基于 token 集合)
        阈值: similarity_threshold (默认 0.6)
        """
        sig_tokens = self._tokenize(signature)
        if not sig_tokens:
            return []

        matched: list[Precedent] = []
        for p in precedents:
            pred_tokens = self._tokenize(p.signature)
            if not pred_tokens:
                continue
            similarity = self._jaccard(sig_tokens, pred_tokens)
            if similarity >= self._sim_threshold:
                # 将相似度作为附加置信度调整
                adjusted = Precedent(
                    signature=p.signature,
                    resolution=p.resolution,
                    confidence=p.confidence * (0.5 + 0.5 * similarity),  # 相似度加权
                    outcome=p.outcome,
                    precedent_id=p.precedent_id,
                    created_at=p.created_at,
                    context=p.context,
                )
                matched.append(adjusted)

        return matched

    @staticmethod
    def _tokenize(text: str) -> set[str]:
        """将签名文本分词为 token 集合."""
        if not text:
            return set()
        # 简单分词: 按非字母数字字符分割, 小写化
        import re
        return set(re.findall(r"[a-z0-9]+", text.lower()))

    @staticmethod
    def _jaccard(set_a: set[str], set_b: set[str]) -> float:
        """计算两个集合的 Jaccard 相似度."""
        if not set_a or not set_b:
            return 0.0
        intersection = set_a & set_b
        union = set_a | set_b
        return len(intersection) / len(union)

    # ── HITL 升级 ───────────────────────────────────

    def escalate_hitl(
        self,
        conflict: Conflict,
        result: ArbitrationResult,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """将低置信度裁决升级至人类审批.

        Args:
            conflict: 原始冲突
            result: 仲裁结果 (escalated=True)
            context: 附加上下文

        Returns:
            升级待办记录 (dict), 包含冲突描述、裁决建议、升级原因
        """
        return {
            "escalation_id": uuid.uuid4().hex[:12],
            "conflict_id": conflict.conflict_id,
            "conflict_signature": conflict.signature,
            "suggested_resolution": result.resolution,
            "suggested_confidence": result.confidence,
            "escalation_reason": result.escalation_reason,
            "basis_precedents": result.basis,
            "agents": conflict.agents,
            "positions": conflict.positions,
            "context": context or {},
            "status": "pending_human_review",
            "created_at": _now_iso(),
        }

    # ── 便捷方法 ────────────────────────────────────

    @property
    def precedents_count(self) -> int:
        return len(self._precedents)

    @property
    def confidence_threshold(self) -> float:
        return self._threshold

    def summary(self) -> dict[str, Any]:
        """返回仲裁器状态摘要."""
        resolutions: dict[str, int] = {}
        for p in self._precedents:
            resolutions[p.resolution] = resolutions.get(p.resolution, 0) + 1
        return {
            "total_precedents": len(self._precedents),
            "threshold": self._threshold,
            "similarity_threshold": self._sim_threshold,
            "by_resolution": resolutions,
        }


# ── 仿真评测 ──────────────────────────────────────────


@dataclass
class SimulationScenario:
    """仿真评测场景 — 已知正确解的冲突.

    Attributes:
        name: 场景名称
        conflict: 冲突描述
        expected_resolution: 期望的裁决结果
        precedents: 先例列表
    """

    name: str
    conflict: Conflict
    expected_resolution: str
    precedents: list[Precedent] = field(default_factory=list)


def create_simulation_scenarios() -> list[SimulationScenario]:
    """构造已知正确解的仿真冲突场景集.

    每个场景包含:
      - 明确的冲突签名
      - 支持期望解的先例 (高置信度)
      - 干扰先例 (低置信度或不同 resolution)
    """
    scenarios: list[SimulationScenario] = []

    # 场景 1: 配置冲突 — 多 agent 对 config 值分歧
    scenarios.append(
        SimulationScenario(
            name="config_value_conflict",
            conflict=Conflict(
                signature="config_value:database.timeout:conflict",
                agents=["agent-a", "agent-b", "agent-c"],
                positions={
                    "agent-a": "timeout=30s",
                    "agent-b": "timeout=60s",
                    "agent-c": "timeout=30s",
                },
                context={"resource": "database.config", "action": "update"},
            ),
            expected_resolution="timeout=30s",  # 多数 + 高置信先例
            precedents=[
                Precedent(signature="config_value:database.timeout", resolution="timeout=30s", confidence=0.95),
                Precedent(signature="config_value:database.timeout", resolution="timeout=30s", confidence=0.90),
                Precedent(signature="config_value:database.timeout", resolution="timeout=60s", confidence=0.50),
            ],
        )
    )

    # 场景 2: 部署顺序冲突
    scenarios.append(
        SimulationScenario(
            name="deploy_order_conflict",
            conflict=Conflict(
                signature="deploy_order:service-a-before-b:conflict",
                agents=["deploy-agent-1", "deploy-agent-2"],
                positions={
                    "deploy-agent-1": "deploy_a_first",
                    "deploy-agent-2": "deploy_b_first",
                },
                context={"resource": "k8s/deployment", "action": "deploy"},
            ),
            expected_resolution="deploy_a_first",
            precedents=[
                Precedent(signature="deploy_order:service-a-before-b", resolution="deploy_a_first", confidence=0.92),
                Precedent(signature="deploy_order:service-a-before-b", resolution="deploy_a_first", confidence=0.88),
                Precedent(signature="deploy_order:service-b-before-a", resolution="deploy_b_first", confidence=0.60),
            ],
        )
    )

    # 场景 3: 权限升级冲突
    scenarios.append(
        SimulationScenario(
            name="permission_escalation_conflict",
            conflict=Conflict(
                signature="permission:admin:escalation:conflict",
                agents=["security-agent", "dev-agent"],
                positions={
                    "security-agent": "deny_escalation",
                    "dev-agent": "allow_escalation",
                },
                context={"resource": "iam/policy", "action": "grant"},
            ),
            expected_resolution="deny_escalation",
            precedents=[
                Precedent(signature="permission:admin:escalation", resolution="deny_escalation", confidence=0.98),
                Precedent(signature="permission:admin:escalation", resolution="deny_escalation", confidence=0.95),
                Precedent(signature="permission:user:escalation", resolution="allow_escalation", confidence=0.70),
            ],
        )
    )

    # 场景 4: 无匹配先例 (应升级 HITL)
    scenarios.append(
        SimulationScenario(
            name="no_precedent_conflict",
            conflict=Conflict(
                signature="unique:conflict:never_seen",
                agents=["agent-x"],
                positions={"agent-x": "position_a"},
                context={"resource": "unknown"},
            ),
            expected_resolution="unknown",  # 无先例 → 升级
            precedents=[],
        )
    )

    # 场景 5: 低置信度 (应升级 HITL)
    scenarios.append(
        SimulationScenario(
            name="low_confidence_conflict",
            conflict=Conflict(
                signature="ambiguous:decision:conflict",
                agents=["agent-m", "agent-n"],
                positions={"agent-m": "option_a", "agent-n": "option_b"},
                context={"resource": "ambiguous"},
            ),
            expected_resolution="option_a",  # 但置信度低 → 升级
            precedents=[
                Precedent(signature="ambiguous:decision", resolution="option_a", confidence=0.40),
                Precedent(signature="ambiguous:decision", resolution="option_b", confidence=0.45),
            ],
        )
    )

    return scenarios


def run_simulation(arbiter: PrecedentArbiter) -> dict[str, Any]:
    """执行仿真评测, 返回命中率与详情.

    Args:
        arbiter: 仲裁器实例

    Returns:
        Dict with keys: hit_rate, total, passed, failed, details
    """
    scenarios = create_simulation_scenarios()
    total = len(scenarios)
    passed = 0
    failed = 0
    details: list[dict[str, Any]] = []

    for scenario in scenarios:
        result = arbiter.arbitrate(scenario.conflict, precedents=scenario.precedents)

        # 判断是否命中:
        # - 有期望解: resolution 匹配 或 (escalated 且 expected 是 "unknown")
        # - 低置信度场景: 应 escalated=True
        is_hit = False
        reason = ""

        if scenario.expected_resolution == "unknown" and result.escalated:
            is_hit = True
            reason = "correctly_escalated_no_precedent"
        elif scenario.name == "low_confidence_conflict" and result.escalated:
            is_hit = True
            reason = "correctly_escalated_low_confidence"
        elif result.resolution == scenario.expected_resolution and not result.escalated:
            is_hit = True
            reason = "correct_resolution"

        if is_hit:
            passed += 1
        else:
            failed += 1

        details.append(
            {
                "scenario": scenario.name,
                "expected": scenario.expected_resolution,
                "got": result.resolution,
                "escalated": result.escalated,
                "confidence": result.confidence,
                "hit": is_hit,
                "reason": reason,
            }
        )

    hit_rate = passed / total if total > 0 else 0.0

    return {
        "hit_rate": hit_rate,
        "total": total,
        "passed": passed,
        "failed": failed,
        "details": details,
    }


# ── 便捷函数 ──────────────────────────────────────────


def _now_iso() -> str:
    """返回当前 UTC 时间 ISO 格式."""
    from datetime import UTC, datetime
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_arbiter(precedents_file: Path | None = None) -> PrecedentArbiter:
    """加载先例仲裁器 (便捷工厂函数)."""
    return PrecedentArbiter(precedents_file=precedents_file)
