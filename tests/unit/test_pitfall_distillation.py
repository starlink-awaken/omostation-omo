"""test_pitfall_distillation — 验证踩坑记忆动态自蒸馏与守卫自适应拦截 (Phase 7)."""

import shutil
from pathlib import Path

from omo.resident.pitfall_rag import (
    DYNAMIC_PITFALLS_DIR,
    distill_pitfall_from_diff,
    get_high_priority_pitfalls,
    load_dynamic_pitfalls,
    save_distilled_pitfall,
    verify_code_against_pitfalls,
)


def test_distill_npm_bun_mismatch():
    """验证从 npm edgesOut 错误与 bun 修复补丁中蒸馏出高置信度确定性规则."""
    error_log = "npm error Cannot read properties of null (reading 'edgesOut')"
    diff_text = """
- cd projects/cockpit-ui && npm install
+ cd projects/cockpit-ui && bun install
"""
    rule = distill_pitfall_from_diff(error_log, diff_text, context="cockpit-ui-ci.yml")
    assert rule["id"] == "pitfall-distilled-npm-bun-lock-mismatch"
    assert rule["is_deterministic"] is True
    assert rule["status"] == "active"
    assert "bun" in rule["remedy"]


def test_distill_shadow_rule():
    """验证模糊异常提炼为 SHADOW 影子模式不强阻断."""
    error_log = "RuntimeWarning: coroutine was never awaited"
    rule = distill_pitfall_from_diff(error_log, context="async worker")
    assert rule["is_deterministic"] is False
    assert rule["status"] == "shadow"


def test_save_and_hot_reload_dynamic_pitfall(tmp_path):
    """验证动态规则持久化落盘与实时热加载、守卫阻断闭环."""
    test_rule = {
        "id": "pitfall-test-forbidden-token",
        "title": "测试禁用特殊字面量",
        "domain": "test",
        "severity": "HIGH",
        "pattern": "FORBIDDEN_TEST_MAGIC",
        "validation_regex": r"FORBIDDEN_TEST_MAGIC",
        "remedy": "改用 SAFE_TEST_MAGIC",
        "is_deterministic": True,
        "status": "active",
    }
    saved_path = save_distilled_pitfall(test_rule)
    assert saved_path.exists()

    try:
        # 热加载测试
        dynamic_rules = load_dynamic_pitfalls()
        ids = [r["id"] for r in dynamic_rules]
        assert "pitfall-test-forbidden-token" in ids

        all_rules = get_high_priority_pitfalls()
        assert any(r["id"] == "pitfall-test-forbidden-token" for r in all_rules)

        # 守卫验证拦截
        bad_diff = """
+ const x = FORBIDDEN_TEST_MAGIC + 1;
"""
        violations = verify_code_against_pitfalls(bad_diff)
        assert len(violations) > 0
        assert any("pitfall-test-forbidden-token" in v for v in violations)

        # 干净代码不拦截
        clean_diff = """
+ const x = SAFE_TEST_MAGIC + 1;
"""
        violations_clean = verify_code_against_pitfalls(clean_diff)
        assert not any("pitfall-test-forbidden-token" in v for v in violations_clean)
    finally:
        # 清理测试产物
        if saved_path.exists():
            saved_path.unlink()
