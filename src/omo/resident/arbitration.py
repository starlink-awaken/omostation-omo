"""
arbitration.py — 多 Agent 先例仲裁引擎 (T5-05)

当多 Agent 对同一冲突各执先例时，基于签名相似度检索历史先例，
合成裁决置信度。置信度低于 0.85 时自动升级至人类待办 (HITL)，
严禁低置信度盲目合并。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

CONFIDENCE_THRESHOLD = 0.85


class ArbitrationError(Exception):
    """仲裁层错误。"""


@dataclass
class Precedent:
    """历史先例。"""

    precedent_id: str
    signature: str
    resolution: str
    confidence: float
    outcome: str
    ts: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "precedent_id": self.precedent_id,
            "signature": self.signature,
            "resolution": self.resolution,
            "confidence": self.confidence,
            "outcome": self.outcome,
            "ts": self.ts,
        }


@dataclass
class ArbitrationResult:
    """仲裁结果。"""

    resolution: str
    confidence: float
    basis: list[str]
    escalated: bool = False


class PrecedentArbiter:
    """先例仲裁器：检索 + 置信度合成 + HITL 升级。"""

    def __init__(self, store_path: str | Path | None = None):
        self.store_path = Path(store_path) if store_path else None
        self._precedents: list[Precedent] = []
        if self.store_path and self.store_path.exists():
            self._load()

    def _load(self) -> None:
        self._precedents.clear()
        with self.store_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    self._precedents.append(Precedent(**json.loads(line)))

    def _save(self, p: Precedent) -> None:
        if not self.store_path:
            return
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        with self.store_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(p.to_dict(), ensure_ascii=False) + "\n")

    def register(self, signature: str, resolution: str, confidence: float, outcome: str) -> Precedent:
        """注册先例。"""
        p = Precedent(
            precedent_id=f"prec-{hashlib.sha256(signature.encode()).hexdigest()[:12]}",
            signature=signature,
            resolution=resolution,
            confidence=confidence,
            outcome=outcome,
        )
        self._precedents.append(p)
        self._save(p)
        return p

    def _similarity(self, a: str, b: str) -> float:
        """简单 Jaccard 词组相似度 (确定性, 零模型调用)。"""
        ta, tb = set(a.lower().split()), set(b.lower().split())
        if not ta and not tb:
            return 1.0
        if not ta or not tb:
            return 0.0
        return len(ta & tb) / len(ta | tb)

    def arbitrate(self, conflict: str, precedents: list[Precedent] | None = None) -> ArbitrationResult:
        """对冲突进行先例仲裁。

        1. 检索先例：按签名相似度排序取 top-k
        2. 多数一致性 + 相似度加权置信度合成
        3. 低于 0.85 → 升级 HITL
        """
        pool = precedents if precedents is not None else self._precedents
        if not pool:
            return ArbitrationResult(
                resolution="",
                confidence=0.0,
                basis=[],
                escalated=True,
            )

        # 按相似度排序
        scored = [(p, self._similarity(conflict, p.signature)) for p in pool]
        scored.sort(key=lambda x: x[1], reverse=True)
        # 只保留高相似度先例 (>= 0.7)
        top = [(p, s) for p, s in scored if s >= 0.7][:5]
        if not top:
            return ArbitrationResult(
                resolution="",
                confidence=0.0,
                basis=[],
                escalated=True,
            )

        # 多数一致性：按 resolution 分组
        by_resolution: dict[str, list[tuple[Precedent, float]]] = {}
        for p, s in top:
            by_resolution.setdefault(p.resolution, []).append((p, s))

        best_res = max(
            by_resolution,
            key=lambda r: (
                len(by_resolution[r]),
                sum(p.confidence * s for p, s in by_resolution[r]),
            ),
        )
        group = by_resolution[best_res]
        # 相似度加权置信度
        total_weight = sum(s for _, s in group)
        confidence = sum(p.confidence * s for p, s in group) / total_weight if total_weight > 0 else 0.0
        confidence = round(min(confidence, 1.0), 4)

        basis = [p.precedent_id for p, _ in group]
        escalated = confidence < CONFIDENCE_THRESHOLD

        return ArbitrationResult(
            resolution=best_res,
            confidence=confidence,
            basis=basis,
            escalated=escalated,
        )

    def escalate_hitl(self, conflict: str, result: ArbitrationResult) -> dict[str, Any]:
        """构造 HITL 升级待办。"""
        return {
            "type": "escalation",
            "reason": "low_confidence_arbitration",
            "conflict": conflict,
            "proposed_resolution": result.resolution,
            "confidence": result.confidence,
            "threshold": CONFIDENCE_THRESHOLD,
            "basis": result.basis,
            "ts": datetime.now(UTC).isoformat(),
        }
