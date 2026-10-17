#!/usr/bin/env python3
"""projects/omo/tests/unit/test_resident_pitfall_rag.py — 逆向反哺引擎与后验守卫单元测试."""

import pytest

from omo.resident.pitfall_rag import (
    CANONICAL_PITFALLS,
    extract_relevant_pitfalls,
    load_memory_topic_pitfalls,
    verify_code_against_pitfalls,
)


def test_canonical_pitfalls_structure():
    """验证内置主权硬纪律的完整性与键结构."""
    assert len(CANONICAL_PITFALLS) >= 5
    for item in CANONICAL_PITFALLS:
        assert "id" in item
        assert "title" in item
        assert "rule" in item
        assert "source" in item
        assert item["rule"].startswith("【")


def test_extract_pitfalls_zsh_intent():
    """验证当意图涉及 shell 脚本循环时，精准命中 zsh 切词规则."""
    pitfalls = extract_relevant_pitfalls(
        objective="优化 bin/gac/gac-worktree.sh 中的 zsh 遍历循环与性能",
        target_paths=["bin/gac/gac-worktree.sh"],
        max_rules=3,
    )
    assert len(pitfalls) <= 3
    rule_ids = [p["id"] for p in pitfalls]
    assert "pitfall-zsh-word-split" in rule_ids


def test_extract_pitfalls_menubar_intent():
    """验证当意图涉及 SwiftBar 状态栏时，精准命中保留 emoji 规则."""
    pitfalls = extract_relevant_pitfalls(
        objective="更新 dotfiles/menubar/hub.30s.sh 增加蜂群任务状态展示",
        target_paths=["dotfiles/menubar/hub.30s.sh"],
        max_rules=2,
    )
    rule_ids = [p["id"] for p in pitfalls]
    assert "pitfall-swiftbar-domain-emoji" in rule_ids


def test_extract_pitfalls_fallback():
    """验证当意图完全模糊时，自动兜底提供隔离工作树只读铁律."""
    pitfalls = extract_relevant_pitfalls(
        objective="随便做点模糊的改动",
        target_paths=[],
        max_rules=2,
    )
    assert len(pitfalls) >= 1
    rule_ids = [p["id"] for p in pitfalls]
    assert "pitfall-worktree-isolation" in rule_ids


def test_verify_code_zsh_split_detection():
    """验证后验校验器能识别 zsh 裸变量展开切词缺陷."""
    bad_diff = """
--- a/test.sh
+++ b/test.sh
@@ -10,3 +10,5 @@
+VAR="a b c"
+for x in $VAR; do
+    echo "$x"
+done
"""
    violations = verify_code_against_pitfalls(bad_diff, file_paths=["test.sh"])
    assert len(violations) >= 1
    assert "zsh 参数切词纪律" in violations[0]

    # 正确写法不应报错
    good_diff = """
--- a/test.sh
+++ b/test.sh
@@ -10,3 +10,5 @@
+for x in a b c; do
+    echo "$x"
+done
"""
    good_violations = verify_code_against_pitfalls(good_diff, file_paths=["test.sh"])
    assert len(good_violations) == 0


def test_verify_code_hardcoded_workspace_detection():
    """验证后验校验器能识别硬编码 Workspace 路径违规."""
    bad_diff = """
--- a/src/test.py
+++ b/src/test.py
@@ -5,2 +5,3 @@
+ROOT_DIR = "/Users/xiamingxing/Workspace"
+print(ROOT_DIR)
"""
    violations = verify_code_against_pitfalls(bad_diff, file_paths=["src/test.py"])
    assert len(violations) >= 1
    assert "ADR-0456" in violations[0]


def test_verify_code_swiftbar_emoji_detection():
    """验证后验校验器能识别 SwiftBar 保留 emoji 违规."""
    bad_diff = """
--- a/dotfiles/menubar/hub.30s.sh
+++ b/dotfiles/menubar/hub.30s.sh
@@ -50,2 +50,3 @@
+echo "🤖 待办任务: 3个"
"""
    violations = verify_code_against_pitfalls(bad_diff, file_paths=["dotfiles/menubar/hub.30s.sh"])
    assert len(violations) >= 1
    assert "SwiftBar 域正则" in violations[0]
