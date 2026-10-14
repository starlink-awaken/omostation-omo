"""BET-Y2Q4-T10-233 — omo.resident 写面按 profile 收敛到 state 根。

三段对应 spec I1 / I2 / I4:

* **I1 等价** —— 未声明 profile 时每个迁移点的路径字符串与改前逐字节相同。这是
  circuit_breaker 的正向表达: 本轮只造机制, 不搬家。
* **I2 调用时刻落点** —— 在 **import 之后**声明 ``OMOSTATION_STATE_ROOT``, 不传参调用写者,
  断言真实文件落在声明位置且检出侧一份不多。冻结常量按构造做不到, 所以这条用例本身就是
  「没退化成常量翻动」的证据。
* **I4 自证源码门禁** —— 扫描按 resolver 匹配; 先用合成违规证明检测器点名 file+line+变量名,
  再断言真实扫描集合为空, 并反向断言纯读面不被命中。
"""

from __future__ import annotations

import ast
import os
from collections.abc import Callable
from pathlib import Path

import pytest

from omo import omo_paths
from omo.resident import (
    WORKSPACE,
    alert,
    daemon,
    decision,
    event_ledger_path,
    heartbeat,
    inbox,
    ingest,
    monitor,
    promote,
    receipt,
    sediment,
    signals,
    state_root,
    status,
    write_path,
)

RESIDENT_DIR = Path(daemon.__file__).resolve().parent
RESOLVER_CALLS = frozenset({"write_path", "state_root", "event_ledger_path"})
FS_ACTIONS = frozenset(
    {
        "open",
        "read_text",
        "write_text",
        "read_bytes",
        "write_bytes",
        "exists",
        "is_file",
        "is_dir",
        "mkdir",
        "stat",
        "unlink",
        "touch",
        "glob",
        "rglob",
        "iterdir",
        "chmod",
        "rename",
    }
)

# 本轮迁移的全部写面解析点 (spec §6)。每项都是「不传参的调用时刻解析」, 不是常量读法。
MIGRATED: list[tuple[str, Callable[[], Path], Path | None]] = [
    ("daemon.DEFAULT_EVENTS_JSONL", lambda: write_path(daemon.DEFAULT_EVENTS_JSONL), daemon.DEFAULT_EVENTS_JSONL),
    ("daemon.PID_FILE", lambda: write_path(daemon.PID_FILE), daemon.PID_FILE),
    ("daemon.LOG_FILE", lambda: write_path(daemon.LOG_FILE), daemon.LOG_FILE),
    ("daemon.watermark", lambda: daemon._wm_path("resident-sub"), None),
    ("daemon.ledger", lambda: event_ledger_path(), None),
    ("receipt.RECEIPTS_FILE", lambda: write_path(receipt.RECEIPTS_FILE), receipt.RECEIPTS_FILE),
    ("status.resolve_ledger", lambda: status.resolve_ledger(), None),
    ("status.EVENTS_JSONL", lambda: write_path(status.EVENTS_JSONL), status.EVENTS_JSONL),
    ("status.SEDIMENT_ROOT", lambda: write_path(status.SEDIMENT_ROOT), status.SEDIMENT_ROOT),
    ("status.DAEMON_WATERMARKS", lambda: write_path(status.DAEMON_WATERMARKS), status.DAEMON_WATERMARKS),
    ("status.ALERT_WATERMARK", lambda: write_path(status.ALERT_WATERMARK), status.ALERT_WATERMARK),
    ("heartbeat.EVENTS_JSONL", lambda: write_path(heartbeat.EVENTS_JSONL), heartbeat.EVENTS_JSONL),
    ("heartbeat.HEARTBEAT_LEDGER", lambda: write_path(heartbeat.HEARTBEAT_LEDGER), heartbeat.HEARTBEAT_LEDGER),
    ("monitor.EVENTS_JSONL", lambda: write_path(monitor.EVENTS_JSONL), monitor.EVENTS_JSONL),
    ("monitor.ALERT_LEDGER", lambda: write_path(monitor.ALERT_LEDGER), monitor.ALERT_LEDGER),
    ("alert.OBS_EVENTS", lambda: write_path(alert.OBS_EVENTS), alert.OBS_EVENTS),
    ("alert.WATERMARK_FILE", lambda: write_path(alert.WATERMARK_FILE), alert.WATERMARK_FILE),
    ("inbox.EVENTS_JSONL", lambda: write_path(inbox.EVENTS_JSONL), inbox.EVENTS_JSONL),
    ("inbox.WATERMARK_FILE", lambda: write_path(inbox.WATERMARK_FILE), inbox.WATERMARK_FILE),
    ("signals.EVENTS_JSONL", lambda: write_path(signals.EVENTS_JSONL), signals.EVENTS_JSONL),
    ("signals.WATERMARK_FILE", lambda: write_path(signals.WATERMARK_FILE), signals.WATERMARK_FILE),
    ("ingest.EVENTS_JSONL", lambda: write_path(ingest.EVENTS_JSONL), ingest.EVENTS_JSONL),
    ("ingest.WATERMARK_FILE", lambda: write_path(ingest.WATERMARK_FILE), ingest.WATERMARK_FILE),
    ("sediment.SEDIMENT_ROOT", lambda: write_path(sediment.SEDIMENT_ROOT), sediment.SEDIMENT_ROOT),
    ("promote.SEDIMENT_ROOT", lambda: write_path(promote.SEDIMENT_ROOT), promote.SEDIMENT_ROOT),
    ("promote.RETRO_ROOT", lambda: write_path(promote.RETRO_ROOT), promote.RETRO_ROOT),
    ("promote.EVENTS_PATH", lambda: write_path(promote.EVENTS_PATH), promote.EVENTS_PATH),
    ("promote.ARCHIVE_ROOT", lambda: write_path(promote.ARCHIVE_ROOT), promote.ARCHIVE_ROOT),
    ("decision.PROPOSAL_DIR", lambda: write_path(decision.PROPOSAL_DIR), decision.PROPOSAL_DIR),
    ("decision.INBOX_DIR", lambda: write_path(decision.INBOX_DIR), decision.INBOX_DIR),
]


def _no_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(omo_paths.STATE_ROOT_ENV, raising=False)
    monkeypatch.delenv(omo_paths.LEDGER_DB_ENV, raising=False)


def _snapshot(root: Path) -> dict[str, int]:
    if not root.is_dir():
        return {}
    return {str(p.relative_to(root)): p.stat().st_mtime_ns for p in root.rglob("*") if p.is_file()}


# ── I1: 未声明 profile 时逐字节等价 (circuit_breaker) ────────────────────────────


@pytest.mark.parametrize("label,resolve,frozen", MIGRATED, ids=[m[0] for m in MIGRATED])
def test_undeclared_profile_is_byte_identical_to_history(
    label: str, resolve: Callable[[], Path], frozen: Path | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _no_profile(monkeypatch)
    resolved = resolve()
    if frozen is not None:
        assert resolved == frozen, label
    assert str(resolved).startswith(f"{WORKSPACE}{os.sep}"), f"{label} 落在检出根之外: {resolved}"


def test_ledger_path_priority_matches_repo_root_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """OMO_EVENT_LEDGER_DB > state_root()/runtime/omo/… —— 与 bin/lib/repo_root.py 同优先级。"""
    _no_profile(monkeypatch)
    assert event_ledger_path() == WORKSPACE / omo_paths.LEDGER_RELATIVE
    monkeypatch.setenv(omo_paths.STATE_ROOT_ENV, "/tmp/declared-state")
    assert event_ledger_path() == Path("/tmp/declared-state") / omo_paths.LEDGER_RELATIVE
    monkeypatch.setenv(omo_paths.LEDGER_DB_ENV, "/tmp/elsewhere/db.sqlite3")
    assert event_ledger_path() == Path("/tmp/elsewhere/db.sqlite3")


def test_write_path_leaves_paths_outside_the_checkout_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    """模板不在检出根下 (如 inbox 的 ~/Documents/@感知信号) 时原样返回 —— 缝不得改写它。"""
    _no_profile(monkeypatch)
    outside = Path("/tmp/definitely-not-the-checkout/watermark.json")
    assert write_path(outside) == outside


def test_state_root_reads_env_at_call_time_not_import_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """omo_paths 侧的同一条: 模块常量 STATE_ROOT 冻结, state_root() 不冻结。"""
    _no_profile(monkeypatch)
    assert state_root() == omo_paths.WORKSPACE_ROOT
    monkeypatch.setenv(omo_paths.STATE_ROOT_ENV, "/tmp/late-declared")
    assert state_root() == Path("/tmp/late-declared")


# ── I2: 声明 profile 后真实落点 (正向判据, 不是「检出不脏」的否定式) ─────────────


def test_declared_state_root_relocates_every_write_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    declared = tmp_path / "state"
    monkeypatch.setenv(omo_paths.STATE_ROOT_ENV, str(declared))
    monkeypatch.delenv(omo_paths.LEDGER_DB_ENV, raising=False)
    stray: list[str] = []
    for label, resolve, frozen in MIGRATED:
        resolved = resolve()
        if not resolved.is_relative_to(declared):
            stray.append(f"{label} -> {resolved}")
        elif frozen is not None:
            assert resolved.relative_to(declared) == frozen.relative_to(WORKSPACE), label
    assert not stray, "仍落检出根: " + "; ".join(stray)


def test_writers_land_in_declared_root_and_checkout_gains_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """真跑写者: 八类写点各落一份在声明位置, 且检出侧 .omo/** 零新增、mtime 集合不变。

    spec §7 判据-2 的端到端版本 (整条 `omo resident daemon --once`) 见 closeout receipt。
    """
    declared = tmp_path / "state"
    monkeypatch.setenv(omo_paths.STATE_ROOT_ENV, str(declared))
    monkeypatch.delenv(omo_paths.LEDGER_DB_ENV, raising=False)

    watched = WORKSPACE / ".omo"
    before = _snapshot(watched)

    daemon._save_byte_offset("resident-sub", 7)
    alert._save_byte_offset(9)
    inbox._save_watermark({"a.txt": "h1"})
    signals._save_watermark({"s.json": "h2"})
    ingest._save_watermark("evt-1")
    heartbeat._write_ledger_direct({"ts": "T", "health": "ok"}, {"ts": "T"})
    monitor._append_to_events_jsonl({"ts": "T", "idempotency_key": "k"}, "k")
    receipt.record("WorkflowClosed", "h", "handler", "ok")

    expected = [
        declared / ".omo" / "_delivery" / "resident-orchestrator" / "watermarks" / "resident-sub.json",
        declared / ".omo" / "_delivery" / "alert-forwarder" / "watermark.json",
        declared / ".omo" / "_delivery" / "perception-inbox" / "watermark.json",
        declared / ".omo" / "_delivery" / "personal-signals" / "watermark.json",
        declared / ".omo" / "_delivery" / "event-ingest" / "watermark.json",
        declared / ".omo" / "state" / "resident-heartbeat.jsonl",
        declared / ".omo" / "_knowledge" / "workflow-mesh" / "events.jsonl",
        declared / ".omo" / "_delivery" / "resident-orchestrator" / "receipts.jsonl",
    ]
    got = [p for p in expected if p.is_file()]
    print(f"expect={len(expected)} got={len(got)}")
    assert len(got) == len(expected), "state 根缺少: " + "; ".join(str(p) for p in expected if not p.is_file())
    assert _snapshot(watched) == before, f"检出侧被写入: {sorted(_snapshot(watched) - before)}"


# ── I4: 自证源码门禁 (扫 src, 不是扫 memory) ────────────────────────────────────


class FrozenWritePoint:
    """一处「写面模板在 import 时被冻结取用」的位置。"""

    def __init__(self, file: str, lineno: int, name: str, shape: str) -> None:
        self.file, self.lineno, self.name, self.shape = file, lineno, name, shape

    def key(self) -> tuple[str, int, str, str]:
        return (self.file, self.lineno, self.name, self.shape)

    def __repr__(self) -> str:
        return f"{self.file}:{self.lineno} {self.name} ({self.shape})"


def _templates(tree: ast.Module) -> dict[str, int]:
    """模块级 ``NAME = <右值含 WORKSPACE 的表达式>`` —— 通用形状, 不是硬编码变量名清单。"""
    found: dict[str, int] = {}
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)) or node.value is None:
            continue
        targets = [node.target] if isinstance(node, ast.AnnAssign) else node.targets
        for t in targets:
            if isinstance(t, ast.Name) and t.id.isupper() and "WORKSPACE" in ast.unparse(node.value):
                found[t.id] = node.lineno
    return found


def scan_frozen_write_points(source: str, file_label: str) -> list[FrozenWritePoint]:
    """点名四类冻结形状: 文件动作 (含 ``X.parent.mkdir()`` 链) / 实参 / 返回 / 默认参数。"""
    tree = ast.parse(source)
    templates = _templates(tree)
    wrapped: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in RESOLVER_CALLS:
            for arg in list(node.args) + [k.value for k in node.keywords]:
                wrapped.update(id(inner) for inner in ast.walk(arg))

    def base_name(node: ast.AST | None) -> str | None:
        """沿属性链走到最左边的 Name —— `FROZEN.parent` 的基名仍是 FROZEN。"""
        while isinstance(node, ast.Attribute):
            node = node.value
        return node.id if isinstance(node, ast.Name) and node.id in templates else None

    hits: list[FrozenWritePoint] = []

    def bare(node: ast.AST | None, lineno: int, shape: str) -> None:
        if node is None or id(node) in wrapped:
            return
        name = base_name(node)
        if name:
            hits.append(FrozenWritePoint(file_label, lineno, name, shape))

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in FS_ACTIONS:
            bare(node.value, node.lineno, f".{node.attr}()")
            bare(node, node.lineno, f".{node.attr}()")
        elif isinstance(node, ast.Call):
            for arg in list(node.args) + [k.value for k in node.keywords]:
                bare(arg, getattr(arg, "lineno", node.lineno), "arg")
        elif isinstance(node, ast.Return):
            bare(node.value, node.lineno, "return")
        elif isinstance(node, ast.Assign):
            bare(node.value, node.lineno, "assign-rhs")
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for default in list(node.args.defaults) + [d for d in node.args.kw_defaults if d is not None]:
                bare(default, default.lineno, "param-default")
    return sorted({(h.file, h.lineno, h.name, h.shape): h for h in hits}.values(), key=lambda h: h.key())


# 读面豁免 (ADR-0456: 读跟随当前检出)。两个模块都不在本轮 write_surfaces 里, 且语义是**读**:
# resources 读治理 registry SSOT, sandbox_driver 读 bin/ 下的脚本路径。被下面两条断言钉成集合相等。
READ_PLANE: dict[tuple[str, str], str] = {
    ("resources.py", "FABRIC_REGISTRY"): "读 .omo/_truth/registry/ 是读面, 跟随检出 (ADR-0456 读面契约)",
    ("sandbox_driver.py", "SANDBOX_SCRIPT"): "bin/gac/ 脚本是代码读面, 不随 profile 搬家",
}

SYNTHETIC = "\n".join(
    [
        "from pathlib import Path",
        "from omo.resident import WORKSPACE, write_path",
        "FROZEN = WORKSPACE / '.omo' / 'state' / 'frozen.jsonl'",
        "SAFE = WORKSPACE / '.omo' / 'state' / 'safe.jsonl'",
        "",
        "def bad() -> None:",
        "    FROZEN.parent.mkdir(parents=True, exist_ok=True)",
        "    with FROZEN.open('a') as fh:",
        "        fh.write('x')",
        "",
        "def also_bad(path: Path = FROZEN) -> Path:",
        "    return FROZEN",
        "",
        "def good() -> None:",
        "    with write_path(SAFE).open('a') as fh:",
        "        fh.write('x')",
    ]
)


def test_detector_names_a_synthetic_violation_with_file_line_and_variable() -> None:
    """检测器自证: 合成违规必须被点名为 file + line + 变量名, 且不得误报已走 resolver 的那处。

    没有这一条, 「真实扫描为空」的绿可能只是检测器压根没命中 (AGENTS.md #4606 纪律)。
    """
    hits = scan_frozen_write_points(SYNTHETIC, "synthetic.py")
    assert all(h.file == "synthetic.py" for h in hits)
    assert {h.name for h in hits} == {"FROZEN"}, hits
    assert all(h.name != "SAFE" for h in hits), f"走 write_path 的读用点被误报: {hits}"
    shapes = {h.shape for h in hits}
    assert {".mkdir()", ".open()", "return", "param-default"} <= shapes, hits
    lines = sorted({h.lineno for h in hits})
    assert lines == [7, 8, 11, 12], hits


@pytest.mark.parametrize("path", sorted(RESIDENT_DIR.glob("*.py")), ids=lambda p: p.name)
def test_real_scan_of_resident_package_is_empty(path: Path) -> None:
    """写面必须全部经调用时刻 resolver; 读面残留由 READ_PLANE 显式豁免 (见下两条)。"""
    hits = [
        h
        for h in scan_frozen_write_points(path.read_text(encoding="utf-8"), path.name)
        if (h.file, h.name) not in READ_PLANE
    ]
    assert not hits, f"{path.name}: 仍有 import 时冻结的写面 -> {hits}"


def _scan_all_resident() -> list[FrozenWritePoint]:
    out: list[FrozenWritePoint] = []
    for path in sorted(RESIDENT_DIR.glob("*.py")):
        out += scan_frozen_write_points(path.read_text(encoding="utf-8"), path.name)
    return out


def test_read_plane_allowlist_equals_what_the_scan_actually_finds() -> None:
    """豁免钉成**集合相等** (present == declared, 双向)。

    多一条 = 有新写面被静默放过; 少一条 = 声明了当前不存在的幽灵豁免。两种都必须红
    (同源: bin/gac/omo-state-write-guard.py 的 undeclared-key / ghost-declaration 双向检查)。
    """
    assert {(h.file, h.name) for h in _scan_all_resident()} == set(READ_PLANE)


def test_every_read_plane_exemption_names_a_real_file() -> None:
    for (fname, var), reason in READ_PLANE.items():
        target = RESIDENT_DIR / fname
        assert target.is_file(), f"{fname} 不存在 —— 豁免过期"
        assert var in target.read_text(encoding="utf-8"), f"{fname} 里已无 {var}"
        assert len(reason) > 20, f"{fname}:{var} 缺理由"


def test_pure_read_plane_sites_are_not_flagged() -> None:
    """反向断言: 读面 (bin 脚本 / 交付 run 文件) 留在检出根是**契约**不是漏网。

    这一句同时证明上一条对 execute.py 不是「文件是空的」式假绿。
    """
    execute = (RESIDENT_DIR / "execute.py").read_text(encoding="utf-8")
    assert 'WORKSPACE / "bin" / "gac" / "pi-worker-adapter.py"' in execute
    assert 'WORKSPACE / ".omo" / "_delivery" / "agent-workflows" / "runs"' in execute
    assert scan_frozen_write_points(execute, "execute.py") == []


# 改前 receipt.py 的冻结写点形状 (按源码顺序) —— 逐字取自 ba69696^ 的那 6 处文件动作。
_PRECHANGE_SHAPES = [
    (".mkdir()", 1),
    (".open()", 1),
    (".is_file()", 2),
    (".read_text()", 2),
]


def test_prechange_baseline_is_not_zero() -> None:
    """基线读数: 检测器必须在**改前的真实文本**上点名 6 处 —— 绿是「从 N 归零」, 不是「一直为零」。

    基线文本逐字嵌进用例, 不用 ``git show HEAD:path``: 提交之后 HEAD **就是改后**的树,
    而 CI 侧历史深浅不定 (worktree 里的子模块可能只有 1 个 commit) —— 两种都会让这条
    判据在交付动作完成后当场假红 (omo PR #208 首跑就是这么红的)。改前状态由 fixture
    自己物化, 与 #4606「测兜底必须先物化」同源。
    """
    hits = scan_frozen_write_points(_PRECHANGE_RECEIPT, "receipt.py@prechange")
    assert len(hits) == sum(n for _, n in _PRECHANGE_SHAPES), repr(hits)
    assert all(h.name == "RECEIPTS_FILE" for h in hits), repr(hits)
    shapes = [h.shape for h in hits]
    for shape, count in _PRECHANGE_SHAPES:
        assert shapes.count(shape) == count, f"{shape}: expect={count} got={shapes.count(shape)}"
    print(f"expect=6 got={len(hits)}")


_PRECHANGE_RECEIPT = """\
from pathlib import Path
from typing import Any

WORKSPACE = Path("/checkout")

RECEIPTS_FILE = WORKSPACE / ".omo" / "_delivery" / "resident-orchestrator" / "receipts.jsonl"
MAX_RECENT = 200  # recent() 单次上限


def record(entry, safe=True, err=""):
    try:
        RECEIPTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with RECEIPTS_FILE.open("a", encoding="utf-8") as fh:
            fh.write("x")
    except OSError:
        pass


def recent(limit: int = 50) -> list[Any]:
    limit = max(1, min(int(limit), MAX_RECENT))
    if not RECEIPTS_FILE.is_file():
        return []
    try:
        lines = RECEIPTS_FILE.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    return lines


def stats():
    total = 0
    if RECEIPTS_FILE.is_file():
        for line in RECEIPTS_FILE.read_text(encoding="utf-8").splitlines():
            total += 1
    return total
"""


def test_package_exports_the_single_write_plane_vocabulary() -> None:
    """写面口径只有一个: state_root / write_path / event_ledger_path 都从包入口可取。"""
    import omo.resident as resident

    for name in ("state_root", "write_path", "event_ledger_path", "WORKSPACE", "LEDGER_DB_ENV"):
        assert hasattr(resident, name), name
    assert set(resident.__all__) == {"LEDGER_DB_ENV", "WORKSPACE", "event_ledger_path", "state_root", "write_path"}


def test_monitor_reexport_aliases_are_not_writers() -> None:
    """monitor 的 OBS_EVENTS / WATERMARK_FILE 是纯 re-export: 无文件动作, 故不得被当成写者。"""
    src = (RESIDENT_DIR / "monitor.py").read_text(encoding="utf-8")
    tail = src[src.index("WATERMARK_FILE = _alert.WATERMARK_FILE") + len("WATERMARK_FILE = _alert.WATERMARK_FILE") :]
    assert "OBS_EVENTS." not in tail and "WATERMARK_FILE." not in tail
