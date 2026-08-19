"""Tests for knowledge quality scoring (BET-Y3H1 business landing)."""

from __future__ import annotations

from pathlib import Path

import pytest

from omo.knowledge_quality import score_knowledge


class TestKnowledgeQualityScoring:
    def test_high_quality_fragment(self):
        """高质量知识片段."""
        fragment = {
            "content": "代码审查发现: 因为缺少空指针检查导致崩溃。方法: 在 `user_input` 前添加 `if user_input is None: return`。结果: 崩溃率从 5% 降至 0%。注意: 所有外部输入都需要验证。",
            "type": "code_review",
            "source_scene": "document-review",
            "source_calibration": 0.85,
        }
        result = score_knowledge(fragment)
        assert result["score"] >= 0.6
        assert result["grade"] in ("A", "B")
        assert "breakdown" in result
        assert len(result["breakdown"]) == 4

    def test_low_quality_fragment(self):
        """低质量知识片段 (泛泛而谈)."""
        fragment = {
            "content": "要注意代码质量",
            "type": "doc_experience",
            "source_scene": "unknown",
        }
        result = score_knowledge(fragment)
        assert result["score"] < 0.5
        assert result["grade"] in ("C", "D")

    def test_empty_content(self):
        """空内容返回 0."""
        result = score_knowledge({"content": "", "type": "code_review"})
        assert result["score"] == 0.0
        assert result["grade"] == "D"

    def test_specificity_with_code(self):
        """包含代码引用提高特异性."""
        with_code = score_knowledge({
            "content": "使用 `validate_input()` 函数检查输入, 返回 0 表示成功, -1 表示错误",
            "type": "code_review",
        })
        without_code = score_knowledge({
            "content": "需要验证输入是否正确",
            "type": "code_review",
        })
        assert with_code["breakdown"]["specificity"] > without_code["breakdown"]["specificity"]

    def test_source_trust_calibration(self):
        """来源 calibration 影响可信度."""
        high_trust = score_knowledge({
            "content": "测试内容",
            "source_calibration": 0.9,
        })
        low_trust = score_knowledge({
            "content": "测试内容",
            "source_calibration": 0.2,
        })
        assert high_trust["breakdown"]["source_trust"] > low_trust["breakdown"]["source_trust"]

    def test_source_trust_none_defaults(self):
        """无 calibration 时默认 0.5."""
        result = score_knowledge({"content": "测试", "source_calibration": None})
        assert result["breakdown"]["source_trust"] == 0.5

    def test_reusability_by_type(self):
        """不同来源类型的可复用性基础分."""
        research = score_knowledge({"content": "研究发现", "type": "research_finding"})
        meeting = score_knowledge({"content": "会议记录", "type": "meeting_insight"})
        assert research["breakdown"]["reusability"] >= meeting["breakdown"]["reusability"]

    def test_recommendation_for_low_score(self):
        """低分给出改进建议."""
        result = score_knowledge({"content": "泛泛", "type": "unknown"})
        assert "需改进" in result["recommendation"]

    def test_completeness_keywords(self):
        """命中完整性关键词加分."""
        complete = score_knowledge({
            "content": "因为设计缺陷导致系统崩溃。方法是增加重试机制。结果: 稳定性提升。注意: 避免无限重试。",
            "type": "code_review",
        })
        incomplete = score_knowledge({
            "content": "系统出问题了",
            "type": "code_review",
        })
        assert complete["breakdown"]["completeness"] > incomplete["breakdown"]["completeness"]
