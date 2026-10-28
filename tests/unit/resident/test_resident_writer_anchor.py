"""BET-Y2Q4-T10-277 — resident / pipeline_supervisor 写者锚点收敛到 state 根。

三面，两两不共享失效模式：

* **A 锚点判据（尺自证）** —— 这四处原本把仓根锚成 ``Path(__file__).resolve().parents[3]``
  （= ``projects/omo``）、或 ``Path.cwd()`` 兜底、或 CWD 相对字面量。检测器按 **AST 调用形状**
  命中（不看文本，所以本文件与 docstring 里的说明不会自击），先在合成源码上自证会点名
  file+line+形状，再断言真实扫描为空。T10-233 那份扫描按「冻结常量有没有过 write_path」匹配，
  对「锚本身数错 parents」是盲的 —— 这正是本条要补的尺。
* **B 落点（真实写）** —— 在 **import 之后**声明 ``OMOSTATION_STATE_ROOT``，不传参、
  并把 CWD 换成无关目录跑一次真实写者；断言文件落在声明根、代码树里一份不多。
* **C 等价（不搬家）** —— 未声明 profile 时定义锚与解析结果逐字节相同，且落在 workspace 根的
  ``.omo/state`` 下，**不在** omo 包自己的树里。
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from omo import omo_paths
from omo import pipeline_supervisor
from omo.resident import WORKSPACE, memory_pipeline, refine_rollback, replay, write_path

OMO_PKG_DIR = Path(omo_paths.__file__).resolve().parent
CODE_ROOT = omo_paths.code_root()
STATE_ROOT_ENV = omo_paths.STATE_ROOT_ENV

WRITERS = {
    "memory_pipeline.MEMORY_DIR": memory_pipeline.MEMORY_DIR,
    "memory_pipeline.CANDIDATE_FILE": memory_pipeline.CANDIDATE_FILE,
    "memory_pipeline.MEMORY_FILE": memory_pipeline.MEMORY_FILE,
    "memory_pipeline.CONFLICT_FILE": memory_pipeline.CONFLICT_FILE,
    "replay.REPLAY_DIR": replay.REPLAY_DIR,
    "replay.EPISODE_LOG": replay.EPISODE_LOG,
    "replay.EVAL_LOG": replay.EVAL_LOG,
    "refine_rollback.store": WORKSPACE / ".omo/state/refinements",
    "refine_rollback.snapshot": WORKSPACE / ".omo/state/refinement-snapshots",
}

WRITER_MODULES = (memory_pipeline, replay, refine_rollback, pipeline_supervisor)

# 三种锚点缺陷形状（AST 级），修法依次是 WORKSPACE 锚 / code_root() / 注入根。
BAD_SHAPES = ("file_anchored_root", "cwd_fallback", "cwd_relative_literal")


def _shape_of(call: ast.Call) -> str | None:
    """识别一个 ``Path(...)`` / ``X.cwd()`` 调用属于哪种缺陷形状。"""
    func = call.func
    # Path(__file__) —— 由模块自身位置反推仓根
    if isinstance(func, ast.Name) and func.id == "Path" and call.args:
        a0 = call.args[0]
        if isinstance(a0, ast.Name) and a0.id == "__file__":
            return "file_anchored_root"
        # Path(".omo/…") —— CWD 相对字面量
        if isinstance(a0, ast.Constant) and isinstance(a0.value, str) and a0.value.startswith(".omo"):
            return "cwd_relative_literal"
        return None
    # Path.cwd() —— CWD 兜底
    if isinstance(func, ast.Attribute) and func.attr == "cwd" and isinstance(func.value, ast.Name) and func.value.id == "Path":
        return "cwd_fallback"
    return None


def scan_anchor_defects(source: str, label: str) -> list[str]:
    hits: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call):
            shape = _shape_of(node)
            if shape in BAD_SHAPES:
                hits.append(f"{label}:{node.lineno}: {shape}")
    return hits


# ── A 尺自证 ───────────────────────────────────────────────────────────
def test_anchor_detector_names_a_synthetic_violation() -> None:
    synthetic = "\n".join(
        [
            "def _sample(store_dir=None):",
            "    ROOT = Path(__file__).resolve().parents[3]",
            '    MEMORY_DIR = ROOT / ".omo/state/agent-cell-memory"',
            "    self_dir = store_dir or Path(\".omo/state/refinements\")",
            "    return Path.cwd()",
        ]
    )
    hits = scan_anchor_defects(synthetic, "synthetic.py")
    assert len(hits) == 3, hits
    assert any("file_anchored_root" in h for h in hits)
    assert any("cwd_fallback" in h for h in hits)
    assert any("cwd_relative_literal" in h for h in hits)


def test_anchor_detector_does_not_fire_on_the_seam_or_on_prose() -> None:
    """反面：正确的缝写法 + docstring 里提到旧形状，都不得命中。"""
    ok = "\n".join(
        [
            '"""旧的 marker 上溯与 `Path.cwd()` 兜底已移除。"""',
            "from omo.resident import WORKSPACE, write_path",
            "",
            'MEMORY_DIR = WORKSPACE / ".omo/state/agent-cell-memory"',
            "",
            "def _sample():",
            "    write_path(MEMORY_DIR).mkdir(parents=True, exist_ok=True)",
            "    return code_root()",
        ]
    )
    assert scan_anchor_defects(ok, "ok.py") == []


def test_no_writer_module_anchors_the_state_plane_on_its_own_tree() -> None:
    """改后为 0，与上面「合成样本为 3」由同一把尺产出 —— 只断言 0 是空绿。"""
    for mod in WRITER_MODULES:
        path = Path(mod.__file__)
        hits = scan_anchor_defects(path.read_text(encoding="utf-8"), path.name)
        assert hits == [], "\n".join(hits)


# ── C 未声明 profile：逐字节复现历史落点，且落点在 workspace 根 ────────────
@pytest.mark.parametrize("name", sorted(WRITERS), ids=sorted(WRITERS))
def test_undeclared_profile_lands_on_workspace_root_not_the_code_tree(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)
    frozen = WRITERS[name]
    assert write_path(frozen) == frozen, name
    assert frozen.is_relative_to(WORKSPACE), name
    assert not frozen.is_relative_to(OMO_PKG_DIR), f"{name} 仍挂在代码树内部"
    assert str(frozen).startswith(f"{WORKSPACE / '.omo' / 'state'}"), name


def test_pipeline_supervisor_state_path_anchored_on_state_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)
    assert pipeline_supervisor._state_path() == WORKSPACE / ".omo/state/pipeline-supervisor-state.json"
    monkeypatch.setenv(STATE_ROOT_ENV, "/tmp/t277-declared")
    assert pipeline_supervisor._state_path() == Path("/tmp/t277-declared/.omo/state/pipeline-supervisor-state.json")


# ── B 声明 profile：真实写者落声明根，代码树不涨，且与 CWD 无关 ────────────
def test_declared_profile_writers_land_in_state_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    declared = tmp_path / "declared-state"
    unrelated_cwd = tmp_path / "elsewhere"
    unrelated_cwd.mkdir()
    monkeypatch.setenv(STATE_ROOT_ENV, str(declared))
    monkeypatch.delenv(omo_paths.LEDGER_DB_ENV, raising=False)
    monkeypatch.chdir(unrelated_cwd)

    pipe = memory_pipeline.MemoryPipeline()
    episode = {"episode_id": "ep-landing", "results": [{"ok": True, "action": "read_file", "output": "x" * 120}]}
    assert pipe.generate_candidates(episode), "candidate was not generated"
    candidates = write_path(memory_pipeline.CANDIDATE_FILE)
    assert candidates == declared / ".omo/state/agent-cell-memory/candidates.jsonl"
    assert candidates.is_file(), candidates
    assert pipe._load_file(candidates) and pipe._load_file(candidates)[0]["episode_id"] == "ep-landing"

    store = refine_rollback.RefinementStore()
    assert store._store_dir == declared / ".omo/state/refinements"
    assert store._store_dir.is_dir()

    snaps = refine_rollback.SnapshotManager()
    assert snaps._snapshot_dir == declared / ".omo/state/refinement-snapshots"
    assert snaps._snapshot_dir.is_dir()

    replay.ReplayFramework()
    assert write_path(replay.EPISODE_LOG) == declared / ".omo/state/agent-cell-replay/episodes.jsonl"
    assert (declared / ".omo/state/agent-cell-replay").is_dir()
    # 定义点仍锚 WORKSPACE —— 既有 monkeypatch.setattr 测试缝依赖这一点
    assert replay.REPLAY_DIR == WORKSPACE / ".omo/state/agent-cell-replay"

    pipeline_supervisor._save_state(
        {"schema": pipeline_supervisor.SCHEMA, "morning_runs": [], "inbound_runs": [], "alerts": []}
    )
    pipeline_supervisor._append_jsonl("pipeline-events.jsonl", {"topic": "landing"})
    assert (declared / ".omo/state/pipeline-supervisor-state.json").is_file()
    events = declared / ".omo/state/pipeline-events.jsonl"
    assert events.is_file()
    assert json.loads(events.read_text(encoding="utf-8"))["topic"] == "landing"
    # 代码面锚点与 CWD 无关（改前是 marker 上溯 + Path.cwd() 兜底）
    assert pipeline_supervisor._ws() == CODE_ROOT

    # 三面检查：声明根写成功之后，omo 包所在代码树与无关 CWD 都不长出同面目录
    for rel in ("agent-cell-memory", "agent-cell-replay", "refinements", "refinement-snapshots"):
        assert not (OMO_PKG_DIR / ".omo/state" / rel).exists(), OMO_PKG_DIR / ".omo/state" / rel
        assert not (unrelated_cwd / ".omo/state" / rel).exists(), unrelated_cwd / ".omo/state" / rel


def test_explicit_root_argument_still_wins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """契约 C2：注入根的调用点不被缝改道（显式参数优先于 profile）。"""
    injected = tmp_path / "injected"
    monkeypatch.setenv(STATE_ROOT_ENV, str(tmp_path / "declared"))
    assert refine_rollback.RefinementStore(store_dir=injected)._store_dir == injected
    assert refine_rollback.SnapshotManager(snapshot_dir=injected)._snapshot_dir == injected
