"""omo.resident — resident agent runtime (multi-常驻 agent 动态成长体系).

Migrated from bin/ssot resident scripts (WP-I). Hosts event ingest, the
subscription→execute daemon, knowledge sediment, memory sync, personal-signals,
alert forwarding, decision agent and execution adapters.

Invoked via ``omo resident <subcommand>`` (see cli.py).
"""

from __future__ import annotations

# workspace root from omo/resident/__init__.py:
#   /Workspace/projects/omo/src/omo/resident/__init__.py → parents[5]
from pathlib import Path

from omo.omo_paths import LEDGER_DB_ENV, event_ledger_path, state_root

WORKSPACE = Path(__file__).resolve().parents[5]


def write_path(frozen: Path) -> Path:
    """写面路径的**调用时刻**解析器: 把仍挂在检出根上的取值改挂到声明的 state 根。

    这个包的所有写面常量都以 `WORKSPACE` 为锚在 import 时求值, 而 `WORKSPACE` 既不读
    `OMOSTATION_STATE_ROOT` 也不等于 `omo_paths.STATE_ROOT` —— 所以晚声明的 profile 对
    它们完全无效, 把常量改名成函数也不会改变这一点 (BET-Y2Q4-T10-233 spec §2)。
    缺陷的形状因此不是「冻结」而是「没有 state 根概念」, 修在**读用点**而不是定义点。

    不动定义点的另一个理由是既有的测试缝: 这个包外有 89 处
    `monkeypatch.setattr(module, "EVENTS_JSONL", tmp_path / …)` 直接把常量指向 tmp,
    那些绝对路径不在 `WORKSPACE` 下 → 原样返回, 缝保持有效。若把常量改成函数, 这 89 处
    会当场断 (BET-Y2Q4-T10-232 在 root 侧改窄了 4 处, 本轮的 11 个测试文件在 packet 之外)。

    未声明 profile 时 `state_root() == WORKSPACE`, 返回值与传入值逐字节相同 —— 这是本轮
    不碰 launchd/crontab、现役服务落点不变的原因 (spec §3)。台账 db 不走这里, 走
    `event_ledger_path()`: `OMO_EVENT_LEDGER_DB` 可以指向检出外的任意位置, 重新锚定会吞掉它。
    """
    try:
        relative = frozen.relative_to(WORKSPACE)
    except ValueError:
        return frozen
    return state_root() / relative


__all__ = ("LEDGER_DB_ENV", "WORKSPACE", "event_ledger_path", "state_root", "write_path")
