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

第四种形状是本轮（sema + pipeline_supervisor 腿）补的：那两处的锚本身没数错，缺陷形状是
**状态面由调用方给的根拼出来**（``self.ws / ".omo/state/…"``、``ws / ".omo/state/im-triage"``），
于是默认根一旦不是仓根，落点就跟着跑。尺按 AST 的「最外层 ``/`` 链 + 最左操作数不是缝」命中它
（``scan_state_plane_defects``），并用**内存里的源码变异**在同一把尺上自证 —— 摘掉缝必须红，
否则「改后为 0」仍可能只是检测器没命中 (V4)。
"""

from __future__ import annotations

import ast
import json
import types
from pathlib import Path

import pytest

from omo import omo_paths, pipeline_supervisor
from omo.resident import WORKSPACE, memory_pipeline, refine_rollback, replay, sema_crystallizer, write_path

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

WRITER_MODULES = (memory_pipeline, replay, refine_rollback, pipeline_supervisor, sema_crystallizer)

# 三种锚点缺陷形状（AST 级），修法依次是 WORKSPACE 锚 / code_root() / 注入根。
BAD_SHAPES = ("file_anchored_root", "cwd_fallback", "cwd_relative_literal")

# 第四种形状（本轮补）：状态面路径由**任意调用方根**拼出来，绕过了缝。
# `write_path` / `state_root` / `code_root` 的调用链，以及 `WORKSPACE` 锚的定义点（写者按
# 既有约定在**用点**过缝）都不算命中。用 `self.ws` / `ws` 这类调用方给的根拼 `.omo/state/**`
# 才命中 —— 这正是 sema_crystallizer 与 pipeline_supervisor im-triage 面的原形状。
STATE_SHAPE = "state_plane_from_arbitrary_root"
SEAM_CALLS = frozenset({"write_path", "state_root", "code_root", "event_ledger_path", "canonical_root"})
ANCHOR_NAMES = frozenset({"WORKSPACE"})

# sema_crystallizer 修复前（omo `a8709b9`）的真实文本，用作尺的正面对撞样本。
HISTORICAL_SEMA_STATE_SITES = (
    "class SemaCrystallizer:\n"
    "    def __init__(self, workspace_root, threshold=2):\n"
    "        self.ws = workspace_root\n"
    "\n"
    "    def ingest_rules(self, rules_path=None):\n"
    '        p = rules_path or (self.ws / ".omo/state/hard-negative-rules.jsonl")\n'
    "        return self.ledger.load_rules_as_events(p) if Path(p).exists() else 0\n"
    "\n"
    "    def hot_reload(self):\n"
    '        sentinel = self.ws / ".omo/state/sema-skills-manifest.json"\n'
    "        sentinel.parent.mkdir(parents=True, exist_ok=True)\n"
)


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
    if (
        isinstance(func, ast.Attribute)
        and func.attr == "cwd"
        and isinstance(func.value, ast.Name)
        and func.value.id == "Path"
    ):
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


def _contains_omo_literal(node: ast.AST) -> bool:
    return any(
        isinstance(s, ast.Constant) and isinstance(s.value, str) and s.value.startswith(".omo") for s in ast.walk(node)
    )


def _leftmost_leaf(node: ast.BinOp) -> ast.AST:
    cur: ast.AST = node
    while isinstance(cur, ast.BinOp) and isinstance(cur.op, ast.Div):
        cur = cur.left
    return cur


def _rooted_at_seam(node: ast.BinOp, parents: dict[ast.AST, ast.AST]) -> bool:
    """链的最左操作数是缝调用 / `WORKSPACE` 锚，或整条链在缝调用的实参里 —— 都不算命中。"""
    leaf = _leftmost_leaf(node)
    if isinstance(leaf, ast.Call) and isinstance(leaf.func, ast.Name) and leaf.func.id in SEAM_CALLS:
        return True
    if isinstance(leaf, ast.Name) and leaf.id in ANCHOR_NAMES:
        return True
    cur = parents.get(node)
    while cur is not None:
        if isinstance(cur, ast.Call) and isinstance(cur.func, ast.Name) and cur.func.id in SEAM_CALLS:
            return True
        cur = parents.get(cur)
    return False


def scan_state_plane_defects(source: str, label: str) -> list[str]:
    """命中「状态面由任意调用方根拼出、绕过缝」的形状（sema_crystallizer 原形状）。"""
    tree = ast.parse(source)
    parents: dict[ast.AST, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    hits: list[str] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div)):
            continue
        if isinstance(parents.get(node), ast.BinOp):  # 只判最长链的最外层
            continue
        if not _contains_omo_literal(node) or _rooted_at_seam(node, parents):
            continue
        hits.append(f"{label}:{node.lineno}: {STATE_SHAPE}")
    return hits


def scan_all_defects(source: str, label: str) -> list[str]:
    return scan_anchor_defects(source, label) + scan_state_plane_defects(source, label)


# ── A 尺自证 ───────────────────────────────────────────────────────────
def test_anchor_detector_names_a_synthetic_violation() -> None:
    synthetic = "\n".join(
        [
            "def _sample(store_dir=None):",
            "    ROOT = Path(__file__).resolve().parents[3]",
            '    MEMORY_DIR = ROOT / ".omo/state/agent-cell-memory"',
            '    self_dir = store_dir or Path(".omo/state/refinements")',
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


def test_state_plane_detector_names_the_historical_sema_sites() -> None:
    """尺的正面对撞：修复前（omo `a8709b9`）那两处真实写法必须被点名。

    「改后为 0」与这条「改前为 N」由同一个函数产出 —— 只断言 0 是空绿 (AGENTS.md §7⑤)。
    """
    hits = scan_state_plane_defects(HISTORICAL_SEMA_STATE_SITES, "sema_pre.py")
    assert len(hits) == 2, hits
    assert all(STATE_SHAPE in h for h in hits), hits


def test_state_plane_detector_does_not_fire_on_the_seam_forms() -> None:
    """反面：三种合法形状 —— `WORKSPACE` 锚定义点、`write_path(...)` 包裹、`state_root()` 链。"""
    ok = "\n".join(
        [
            "from omo.resident import WORKSPACE, write_path",
            "",
            'MEMORY_DIR = WORKSPACE / ".omo/state/agent-cell-memory"',
            "",
            "def _store(store_dir=None):",
            '    return store_dir or write_path(WORKSPACE / ".omo/state/refinements")',
            "",
            "def _state_dir(name):",
            '    return state_root() / ".omo" / "state" / name',
            "",
            "def _sentinel(ws):",
            '    return write_path(ws / ".omo" / "state" / "sema-skills-manifest.json")',
        ]
    )
    assert scan_state_plane_defects(ok, "ok2.py") == []


def test_no_writer_module_anchors_the_state_plane_on_its_own_tree() -> None:
    """改后为 0，与上面「合成样本为 3」「历史 sema 为 2」由同一把尺产出 —— 只断言 0 是空绿。"""
    for mod in WRITER_MODULES:
        path = Path(mod.__file__)
        hits = scan_all_defects(path.read_text(encoding="utf-8"), path.name)
        assert hits == [], "\n".join(hits)


# V4 变异对照：把真实源码里的缝摘掉，同一把尺必须在**真实文件**上点名它。
# 只对内存副本动手、不写盘。历史样本证「尺认得那个形状」，这条证「尺在这份文件上够得着那一行」——
# 少了这条，「改后为 0」仍可能来自该文件的行形状根本不在尺的射程里。
MUTATIONS = (
    (
        sema_crystallizer,
        'sentinel = self._state_path("sema-skills-manifest.json")',
        'sentinel = self.ws / ".omo/state/sema-skills-manifest.json"',
    ),
    (
        pipeline_supervisor,
        'card_dir = _state_dir("im-triage")',
        'card_dir = ws / ".omo/state/im-triage"',
    ),
)


@pytest.mark.parametrize("module,old,new", MUTATIONS, ids=[m.__name__ for m, _, _ in MUTATIONS])
def test_the_ruler_fires_on_the_real_module_once_the_seam_is_removed(
    module: types.ModuleType, old: str, new: str
) -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert old in source, f"{module.__name__}: 被变异的那一行已不在源码里，这条对照失效"
    hits = [h for h in scan_state_plane_defects(source.replace(old, new, 1), "mutated") if STATE_SHAPE in h]
    assert len(hits) == 1, hits
    assert scan_state_plane_defects(source, "real") == []


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


def test_pipeline_supervisor_state_subdirs_follow_the_state_root(monkeypatch: pytest.MonkeyPatch) -> None:
    """policy-radar 读面与 im-triage 写面：未声明时逐字节复现历史落点，声明后随 state 根。"""
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)
    for name in ("policy-radar", "im-triage"):
        resolved = pipeline_supervisor._state_dir(name)
        assert resolved == WORKSPACE / ".omo/state" / name, name
        assert not resolved.is_relative_to(OMO_PKG_DIR), f"{name} 仍挂在代码树内部"
    monkeypatch.setenv(STATE_ROOT_ENV, "/tmp/t277-declared")
    assert pipeline_supervisor._state_dir("im-triage") == Path("/tmp/t277-declared/.omo/state/im-triage")


def test_sema_state_paths_identity_without_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """C 面：默认根就是 `WORKSPACE`（改前是「无默认」→ 由调用方/CWD 决定）。"""
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)
    crystal = sema_crystallizer.SemaCrystallizer()
    assert crystal.ws == WORKSPACE
    for name in ("hard-negative-rules.jsonl", "sema-skills-manifest.json"):
        resolved = crystal._state_path(name)
        assert resolved == WORKSPACE / ".omo/state" / name, name
        assert not resolved.is_relative_to(OMO_PKG_DIR), f"{name} 仍挂在代码树内部"
    # 状态面搬家，代码面不搬：结晶产物是要进版本库的资产
    assert crystal.skills_root() == WORKSPACE / ".agents/skills" / sema_crystallizer.CRYSTALLIZED_DIRNAME
    assert crystal.index_path() == WORKSPACE / ".agents/skills" / "INDEX.md"


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
    # sema 半边：既有 tests/unit/resident/test_sema_crystallizer.py 用 tmp_path 注入根并断言
    # 产物落在那里，靠的是 write_path 对「不在 WORKSPACE 下的路径原样返回」这一支。
    crystal = sema_crystallizer.SemaCrystallizer(injected)
    assert crystal._state_path("sema-skills-manifest.json") == injected / ".omo/state/sema-skills-manifest.json"
    assert crystal.skills_root() == injected / ".agents/skills" / sema_crystallizer.CRYSTALLIZED_DIRNAME


def test_sema_default_root_writes_land_in_declared_state_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """done_when[1]（V2）的 sema 半边：不传参、cwd 换到无关目录，读写两面都落声明根。

    读侧用「显式物化在声明根、默认调用点必须读到」这条正向落点判据 —— 而不是「别处没有」。
    """
    declared = tmp_path / "declared-state"
    unrelated_cwd = tmp_path / "elsewhere"
    unrelated_cwd.mkdir()
    rules = declared / ".omo/state/hard-negative-rules.jsonl"
    rules.parent.mkdir(parents=True, exist_ok=True)
    rules.write_text(
        json.dumps({"rule_type": "banned_phrase", "pattern": "综上所述", "count": 2}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv(STATE_ROOT_ENV, str(declared))
    monkeypatch.delenv(omo_paths.LEDGER_DB_ENV, raising=False)
    monkeypatch.chdir(unrelated_cwd)

    crystal = sema_crystallizer.SemaCrystallizer()
    assert crystal.ingest_rules() == 2, "默认规则库读取点没落到声明根"
    assert crystal.ledger.ready_keys() == ["banned_phrase:综上所述"]

    result = crystal.hot_reload()
    assert result["ok"] is True
    manifest = declared / ".omo/state/sema-skills-manifest.json"
    assert manifest.is_file(), manifest
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert set(payload) == {"reloaded_at", "skills", "active_domains"}
    # 两面此时分叉：状态面跟 profile，代码面跟 self.ws（默认 = WORKSPACE，产物要进版本库）
    assert crystal.skills_root() == WORKSPACE / ".agents/skills" / sema_crystallizer.CRYSTALLIZED_DIRNAME

    for rel in ("im-triage", "policy-radar", "sema-skills-manifest.json"):
        assert not (unrelated_cwd / ".omo/state" / rel).exists(), unrelated_cwd / ".omo/state" / rel
        assert not (OMO_PKG_DIR / ".omo/state" / rel).exists(), OMO_PKG_DIR / ".omo/state" / rel

    card_dir = pipeline_supervisor._state_dir("im-triage")
    assert card_dir == declared / ".omo/state/im-triage"
    card_dir.mkdir(parents=True, exist_ok=True)
    (card_dir / "pipeline-probe.json").write_text("{}", encoding="utf-8")
    assert (declared / ".omo/state/im-triage/pipeline-probe.json").is_file()
