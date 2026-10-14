"""system.yaml 的内核侧解析: 写跟 STATE_ROOT, 显式异根 omo_dir 不被劫持.

契约: `../../.omo/_knowledge/decisions/0456-dev-runtime-profile-root.md` (D1/D3)
BET: BET-Y2Q4-T10-220

`system_yaml_for()` 只在 `omo_dir` 就是内核 OMO_ROOT 时才改道; 传别的目录 (测试
fixture、`--omo-dir`) 必须原样尊重 —— 否则"在隔离目录上验证写到哪"这件事不可断言。
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

from omo import omo_ingress, omo_paths, omo_state

CHECKOUT_SNAPSHOT = "state/system.yaml"


@pytest.fixture
def split_roots(tmp_path, monkeypatch):
    """检出根与 state 根分开的内核视图。"""
    checkout_omo = tmp_path / "checkout" / ".omo"
    state_omo = tmp_path / "state" / ".omo"
    (checkout_omo / "state").mkdir(parents=True)
    (state_omo / "state").mkdir(parents=True)
    (checkout_omo / "state" / "system.yaml").write_text(yaml.dump({"health_score": 46}), encoding="utf-8")
    (state_omo / "state" / "system.yaml").write_text(yaml.dump({"health_score": 88}), encoding="utf-8")
    monkeypatch.setattr(omo_paths, "OMO_ROOT", checkout_omo)
    monkeypatch.setattr(omo_paths, "STATE_SYSTEM_YAML", state_omo / "state" / "system.yaml")
    return checkout_omo, state_omo


def test_write_target_is_the_state_root_copy(split_roots):
    checkout_omo, state_omo = split_roots
    assert omo_paths.system_yaml_for() == state_omo / "state" / "system.yaml"
    assert omo_paths.system_yaml_for(None) == state_omo / "state" / "system.yaml"


def test_explicit_omo_root_redirects_to_state_root(split_roots):
    """显式传内核 OMO_ROOT 与不传等价 —— 都改道, 不会留在检出。"""
    checkout_omo, state_omo = split_roots
    assert omo_paths.system_yaml_for(checkout_omo) == state_omo / "state" / "system.yaml"


def test_foreign_omo_dir_is_never_hijacked(split_roots, tmp_path):
    """--omo-dir / fixture 的目录原样尊重, 否则隔离验证无从谈起。"""
    foreign = tmp_path / "fixture-omo"
    (foreign / "state").mkdir(parents=True)
    assert omo_paths.system_yaml_for(foreign) == foreign / "state" / "system.yaml"


def test_read_prefers_state_root_then_checkout_snapshot(split_roots, tmp_path):
    checkout_omo, state_omo = split_roots
    assert omo_paths.system_yaml_read() == state_omo / "state" / "system.yaml"

    (state_omo / "state" / "system.yaml").unlink()
    assert omo_paths.system_yaml_read() == checkout_omo / "state" / "system.yaml"


def test_read_of_absent_foreign_dir_does_not_fall_back_to_the_host(split_roots, tmp_path):
    """异根目录不存在时仍返回异根路径 —— 兜底只适用于内核自己那对根。"""
    foreign = tmp_path / "fixture-omo"
    assert omo_paths.system_yaml_read(foreign) == foreign / "state" / "system.yaml"


def test_state_root_and_checkout_are_distinct_in_this_fixture(split_roots):
    """判据不是"检出不含某值", 而是同一相对路径上两根的值不同。"""
    checkout_omo, state_omo = split_roots
    assert omo_paths.system_yaml_read() != checkout_omo / "state" / "system.yaml"
    assert yaml.safe_load(omo_paths.system_yaml_read().read_text())["health_score"] == 88
    assert yaml.safe_load((checkout_omo / "state" / "system.yaml").read_text())["health_score"] == 46


def _segments(node: ast.AST) -> list[ast.AST]:
    """按**源码顺序**摊平 `/` 链 —— 用栈弹序拼会拼出 "system.yaml/state"，判据静默失效。"""
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        return _segments(node.left) + _segments(node.right)
    return [node]


def _checkout_pinned_lines(source: str) -> list[int]:
    tree = ast.parse(source)
    offenders: list[int] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div)):
            continue
        segs = _segments(node)
        tail = "/".join(str(s.value) for s in segs if isinstance(s, ast.Constant) and isinstance(s.value, str))
        names = {s.id for s in segs if isinstance(s, ast.Name)}
        if tail.replace(" ", "").strip("/").endswith(CHECKOUT_SNAPSHOT) and "OMO_ROOT" in names:
            offenders.append(node.lineno)
    return offenders


def test_detector_itself_catches_a_known_offender():
    """判据必须是活的: 先在合成源码上命中，再拿它扫真实模块。

    上一版用栈弹序拼 tail，`OMO_ROOT / "state" / "system.yaml"` 拼成
    "system.yaml/state"，扫不出任何东西 —— 测试对任何代码都绿。
    """
    assert _checkout_pinned_lines('PATH = OMO_ROOT / "state" / "system.yaml"\n') == [1]
    assert _checkout_pinned_lines('PATH = OMO_ROOT / ".omo" / "state" / "system.yaml"\n') == [1]
    assert _checkout_pinned_lines("PATH = STATE_SYSTEM_YAML\n") == []


@pytest.mark.parametrize(
    "module",
    [omo_state, omo_ingress],
    ids=["omo_state", "omo_ingress"],
)
def test_kernel_writers_use_the_resolver_not_a_hardcoded_path(module):
    """写点不得再出现 `OMO_ROOT / "state" / "system.yaml"` 这类检出根拼法。"""
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert _checkout_pinned_lines(source) == [], module.__name__
