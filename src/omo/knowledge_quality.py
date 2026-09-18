"""knowledge_quality.py — 知识质量评分函数 (BET-Y3H1 业务落地).

为 knowledge-capture-pipeline 的 quality_gate 提供评分能力.
评分维度:
  - 完整性 (completeness): 是否包含关键要素 (what/why/how)
  - 特异性 (specificity): 是否具体可执行, 非泛泛而谈
  - 可复用性 (reusability): 是否可被其他场景检索复用
  - 来源可信度 (source_trust): 来源场景的 calibration

输出: 0.0 ~ 1.0 的加权评分.

设计决策:
  - 轻量级规则评分 (无需 LLM, 快速)
  - 可扩展 (后续可接入 LLM 评分)
  - 与 MOSBeliefManager calibration 联动
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# 评分权重 (总和 = 1.0)
WEIGHTS = {
    "completeness": 0.30,
    "specificity": 0.25,
    "reusability": 0.25,
    "source_trust": 0.20,
}

# 完整性关键词 (命中加分)
COMPLETENESS_HINTS = [
    r"因为|原因|导致|造成",  # 因果
    r"方法|步骤|流程|如何",  # 方法论
    r"结果|效果|影响|产出",  # 结果
    r"注意|警告|风险|避免",  # 警示
]

# 特异性指标 (具体 > 泛泛)
SPECIFICITY_PATTERNS = [
    r"\d+",  # 包含数字
    r"[A-Za-z_]{3,}",  # 技术术语
    r"`[^`]+`",  # 代码引用
    r'"[^"]+"',  # 引用
]


def score_knowledge(fragment: dict[str, Any]) -> dict[str, Any]:
    """对知识片段进行质量评分.

    Args:
        fragment: {
            "content": str,  # 知识内容
            "type": str,     # code_review / meeting_insight / research_finding / doc_experience
            "source_scene": str,  # 来源场景
            "source_calibration": float,  # 来源场景校准度 (可选)
        }

    Returns:
        {
            "score": float,  # 0.0 ~ 1.0
            "breakdown": {dimension: score},
            "grade": str,  # A/B/C/D
            "recommendation": str,
        }
    """
    content = fragment.get("content", "")
    source_calibration = fragment.get("source_calibration")

    # 空内容直接返回 0
    if not content or not content.strip():
        return {
            "score": 0.0,
            "breakdown": {k: 0.0 for k in WEIGHTS},
            "grade": "D",
            "recommendation": "内容为空, 无法评分",
        }

    completeness = _score_completeness(content)
    specificity = _score_specificity(content)
    reusability = _score_reusability(content, fragment.get("type", ""))
    source_trust = _score_source_trust(source_calibration)

    breakdown = {
        "completeness": round(completeness, 2),
        "specificity": round(specificity, 2),
        "reusability": round(reusability, 2),
        "source_trust": round(source_trust, 2),
    }

    score = sum(WEIGHTS[k] * breakdown[k] for k in WEIGHTS)
    score = round(score, 2)

    grade = _to_grade(score)
    recommendation = _recommendation(score, breakdown)

    return {
        "score": score,
        "breakdown": breakdown,
        "grade": grade,
        "recommendation": recommendation,
    }


def _score_completeness(content: str) -> float:
    """完整性: 命中关键词越多越完整."""
    if not content:
        return 0.0
    hits = sum(1 for p in COMPLETENESS_HINTS if re.search(p, content))
    # 4 个维度中命中几个 / 4
    base = hits / len(COMPLETENESS_HINTS)
    # 长度加成 (内容越长越可能完整, 上限 200 字)
    length_bonus = min(len(content) / 200, 1.0) * 0.3
    return min(1.0, base * 0.7 + length_bonus)


def _score_specificity(content: str) -> float:
    """特异性: 包含具体数字/术语/代码."""
    if not content:
        return 0.0
    hits = sum(1 for p in SPECIFICITY_PATTERNS if re.search(p, content))
    return min(1.0, hits / len(SPECIFICITY_PATTERNS))


def _score_reusability(content: str, frag_type: str) -> float:
    """可复用性: 类型匹配 + 结构化程度."""
    if not content:
        return 0.0
    # 类型基础分
    type_scores = {
        "code_review": 0.8,
        "meeting_insight": 0.6,
        "research_finding": 0.9,
        "doc_experience": 0.7,
    }
    base = type_scores.get(frag_type, 0.5)
    # 结构化加分 (包含列表/分段)
    structure_bonus = 0.0
    if re.search(r"^[-*]\s", content, re.MULTILINE):
        structure_bonus += 0.1
    if re.search(r"\n\n", content):
        structure_bonus += 0.05
    return min(1.0, base + structure_bonus)


def _score_source_trust(source_calibration: float | None) -> float:
    """来源可信度: 基于来源场景 calibration."""
    if source_calibration is None:
        return 0.5  # 未知来源给中等分
    return max(0.0, min(1.0, source_calibration))


def _to_grade(score: float) -> str:
    if score >= 0.8:
        return "A"
    if score >= 0.6:
        return "B"
    if score >= 0.4:
        return "C"
    return "D"


def _recommendation(score: float, breakdown: dict[str, float]) -> str:
    if score >= 0.8:
        return "高质量, 建议直接入库"
    if score >= 0.6:
        return "质量合格, 建议入库"
    # 找出最弱维度
    weakest = min(breakdown, key=lambda k: breakdown[k])
    suggestions = {
        "completeness": "补充因果/方法/结果描述",
        "specificity": "增加具体数字/术语/代码引用",
        "reusability": "结构化整理, 添加类型标签",
        "source_trust": "验证来源场景可信度",
    }
    return f"需改进 ({weakest}: {suggestions.get(weakest, '补充内容')})"


__all__ = ["score_knowledge", "WEIGHTS"]
