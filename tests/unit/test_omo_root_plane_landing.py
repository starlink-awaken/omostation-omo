"""BET-Y2Q4-T10-239 ISC-5 —— 声明 dev profile 后的**正向落点**断言。

「检出不含旧路径」是否定式验收，本身不证明写面真的改了根（AGENTS.md §7：每条
「移走某个写目标」的契约都要配一条正向落点断言）。这里声明
`OMOSTATION_STATE_ROOT=<tmp>`，然后逐站点断言它**落在 `<tmp>` 之下**，
并断言读侧站点**仍留在检出里** —— 两面同时成立才算收敛，只成立一面就是挂错根。

台账两面（`sovereignty/enforcement.py` 与 `event_ledger/surface.py`）历史上各拼各的
`runtime/omo/event-ledger.sqlite3`；这里断言二者与 `omo_paths.event_ledger_path()`
逐字节相等，即 B1 判据在 omo 侧的最后两个断点确实并进了单一口径。

最后一组用例守的是**这份判据自己的隔离面**：fixture 清掉哪些 env 名单，来自
`omo_paths.PROFILE_ENVS` 单一来源，且名单内容用 AST 从源码重新导出后对拍 ——
名单手抄过一次，抄漏的正是 `OMOSTATION_STATE_ROOT` 本身。
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from omo import omo_paths
from omo.omo_paths import PROFILE_ENVS, code_root, event_ledger_path, state_root

SRC_ROOT = omo_paths.OMO_SRC_PARENT / "src"

WRITE_PLANE = [
    "omo.omo_trail.default_trail_path",
    "omo.omo_event.default_event_log_path",
    "omo.omo_sync.default_sync_log_path",
    "omo.omo_alert.alert_log_path",
    "omo.omo_bos_metrics.default_metrics_path",
    "omo.omo_logs.knowledge_dir",
    "omo.omo_observability.knowledge_dir",
]

CODE_PLANE = [
    "omo.omo_alert.notify_script",
    "omo.omo_bos.kairon_packages_src",
    "omo.omo_bos.default_registry_path",
    "omo.omo_self_healing.omo_project_root",
]


def _resolve(dotted: str):
    module_name, _, attr = dotted.rpartition(".")
    return getattr(__import__(module_name, fromlist=[attr]), attr)


@pytest.fixture
def dev_state_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """声明一个 dev profile 的运行态根；其余 profile 名一律清空。"""
    state = tmp_path / "state-root"
    state.mkdir()
    for name in PROFILE_ENVS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OMOSTATION_STATE_ROOT", str(state))
    return state


def test_write_plane_resolvers_land_under_declared_state_root(dev_state_root: Path) -> None:
    """ISC-5 的正向半：七个写面站点全部解析到 `<tmp>` 之下。"""
    misplaced: dict[str, str] = {}
    for dotted in WRITE_PLANE:
        actual = Path(_resolve(dotted)())
        if not actual.is_relative_to(dev_state_root):
            misplaced[dotted] = str(actual)
    assert not misplaced, "声明 profile 后仍落在检出里的写面站点:\n" + "\n".join(
        f"  {k} -> {v}" for k, v in misplaced.items()
    )


def test_code_plane_resolvers_stay_in_the_checkout(dev_state_root: Path) -> None:
    """ISC-5 的另一半：读侧站点不得跟着 profile 走 —— 否则 dev profile 读到空目录。"""
    moved: dict[str, str] = {}
    for dotted in CODE_PLANE:
        actual = Path(_resolve(dotted)())
        if not actual.is_relative_to(code_root()) or actual.is_relative_to(dev_state_root):
            moved[dotted] = str(actual)
    assert not moved, "跟着 profile 跑掉的读侧站点:\n" + "\n".join(f"  {k} -> {v}" for k, v in moved.items())


def test_ledger_two_forked_sites_equal_the_single_seam(dev_state_root: Path) -> None:
    """两处手拼的台账默认路径并进 `event_ledger_path()`，且落在 state 根下。"""
    from omo.event_ledger import surface as ledger_surface
    from omo.sovereignty import enforcement

    expected = event_ledger_path().resolve()
    assert expected.is_relative_to(dev_state_root.resolve())
    assert enforcement._default_db_path() == expected
    assert ledger_surface._default_db_path() == expected
    assert expected == (dev_state_root / "runtime" / "omo" / "event-ledger.sqlite3").resolve()


def test_ledger_env_override_still_wins_over_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """优先级不变：`OMO_EVENT_LEDGER_DB` > profile 根下的默认路径（两仓同契约）。"""
    from omo.event_ledger import surface as ledger_surface
    from omo.sovereignty import enforcement

    for name in PROFILE_ENVS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OMOSTATION_STATE_ROOT", str(tmp_path / "state-root"))
    explicit = tmp_path / "explicit-ledger.sqlite3"
    monkeypatch.setenv("OMO_EVENT_LEDGER_DB", str(explicit))
    for fn in (enforcement._default_db_path, ledger_surface._default_db_path, event_ledger_path):
        assert Path(fn()).resolve() == explicit.resolve(), fn


def test_a2a_inbox_reads_the_state_root_copy_not_the_checkout_copy(tmp_path: Path, dev_state_root: Path) -> None:
    """混合面的行为证据：同一个 `workspace` 变量曾经同时喂 state 与 code，拆开后只有 state 侧被读写。"""
    from omo import omo_agent_host

    code_side = tmp_path / "code-side"  # 假装的检出侧，不该被碰
    (code_side / ".omo" / "state").mkdir(parents=True)
    (dev_state_root / ".omo" / "state").mkdir(parents=True)
    task = {"to": "probe", "type": "task", "ts": "t-1", "from": "governor", "payload": {}}
    payload = json.dumps(task) + "\n"
    for base in (code_side, dev_state_root):
        (base / ".omo" / "state" / "a2a-messages.jsonl").write_text(payload, encoding="utf-8")

    before = (code_side / ".omo" / "state" / "a2a-messages.jsonl").read_text(encoding="utf-8")
    inbox = state_root() / ".omo" / "state" / "a2a-messages.jsonl"
    assert inbox.is_relative_to(dev_state_root), f"a2a inbox 没落在声明的 state 根: {inbox}"

    found = omo_agent_host._check_a2a_inbox("probe", state_root())
    assert len(found) == 1, found
    after_state = inbox.read_text(encoding="utf-8")
    assert "reply" in after_state, "state 侧 inbox 应被写入 reply 标记"
    assert (code_side / ".omo" / "state" / "a2a-messages.jsonl").read_text(encoding="utf-8") == before


def test_state_root_resolves_relative_declarations_to_absolute(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """profile 值可以写成相对路径：缝必须 expanduser + absolute，否则写者各按各的 cwd 落点。"""
    for name in PROFILE_ENVS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OMOSTATION_STATE_ROOT", "relative-state")
    assert state_root() == (tmp_path / "relative-state").absolute()


def test_no_profile_means_both_roots_are_the_checkout(untouched: Path) -> None:
    """未声明 profile 时两根相等 —— 这是「本轮只交机制、对运行态无效应」的边界。"""
    assert state_root() == code_root() == untouched


@pytest.fixture
def untouched(monkeypatch: pytest.MonkeyPatch) -> Path:
    for name in PROFILE_ENVS:
        monkeypatch.delenv(name, raising=False)
    return omo_paths.WORKSPACE_ROOT


# ---------------------------------------------------------------------------
# 隔离名单自身的判据 —— 名单不得手抄，抄了就正是要防的那个缺陷
# ---------------------------------------------------------------------------

PATH_FUNCS = frozenset({"Path"})
PATH_METHODS = frozenset({"expanduser", "resolve", "absolute", "joinpath", "join", "is_dir", "is_file"})
ENV_READ_FUNCS = frozenset({"get", "getenv"})


def _env_consts(tree: ast.AST) -> dict[str, str]:
    """`FOO_ENV = "FOO"` → {"FOO_ENV": "FOO"}，把跨文件 import 的常量名解析回真实 env 名。"""
    out: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.endswith("_ENV"):
                    out[target.id] = node.value.value
    return out


def _env_read(node: ast.AST, consts: dict[str, str]) -> str | None:
    """这个节点是一次以字面量/常量为名的 env 读取吗？返回真实 env 名。"""
    if isinstance(node, ast.Call):
        func = node.func
        is_environ_get = (
            isinstance(func, ast.Attribute)
            and func.attr in ENV_READ_FUNCS
            and (
                (isinstance(func.value, ast.Attribute) and func.value.attr == "environ")
                or (isinstance(func.value, ast.Name) and func.value.id == "os")
            )
        )
        if not is_environ_get or not node.args:
            return None
        first = node.args[0]
    elif isinstance(node, ast.Subscript):
        base = node.value
        if not (isinstance(base, ast.Attribute) and base.attr == "environ"):
            return None
        first = node.slice
    else:
        return None
    if isinstance(first, ast.Constant) and isinstance(first.value, str):
        return first.value
    if isinstance(first, ast.Name):
        return consts.get(first.id)
    return None


def _flow_scope(node: ast.AST, parents: dict[int, ast.AST], module: ast.AST) -> ast.AST:
    """读取点所在的作用域：最近的 enclosing function，否则整个模块。"""
    current = parents.get(id(node))
    while current is not None and current is not module:
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current
        current = parents.get(id(current))
    return module


def _joins_a_path(node: ast.AST, parents: dict[int, ast.AST], scope: ast.AST) -> bool:
    """读取点本身是否在路径语境里（沿祖先链向上，直到作用域）。"""
    current = node
    while current is not None and current is not scope:
        if isinstance(current, ast.Call):
            func = current.func
            if isinstance(func, ast.Name) and func.id in PATH_FUNCS:
                return True
            if isinstance(func, ast.Attribute) and func.attr in PATH_METHODS:
                return True
        if isinstance(current, ast.BinOp) and isinstance(current.op, ast.Div):
            sides = (current.left, current.right)
            if any(isinstance(side, ast.Constant) and isinstance(side.value, str) for side in sides):
                return True
        current = parents.get(id(current))
    return False


def _assigned_names(node: ast.AST, scope: ast.AST) -> set[str]:
    """读取结果被赋给哪些名字（同作用域内的赋值左侧）。"""
    out: set[str] = set()
    for child in ast.walk(scope):
        if isinstance(child, ast.Assign):
            value, targets = child.value, list(child.targets)
        elif isinstance(child, (ast.AugAssign, ast.AnnAssign)):
            value, targets = child.value, [child.target]
        else:
            continue
        if value is None or not any(sub is node for sub in ast.walk(value)):
            continue
        out |= {target.id for target in targets if isinstance(target, ast.Name)}
    return out


def _path_consumed_names(scope: ast.AST) -> set[str]:
    """哪些名字被当作路径消费：`Path(name)` / `name / "seg"` / `name.resolve()` 等。"""
    used: set[str] = set()
    for node in ast.walk(scope):
        if isinstance(node, ast.Call):
            func = node.func
            hit = (isinstance(func, ast.Name) and func.id in PATH_FUNCS) or (
                isinstance(func, ast.Attribute) and func.attr in PATH_METHODS
            )
            if hit:
                used |= {arg.id for arg in node.args if isinstance(arg, ast.Name)}
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            sides = (node.left, node.right)
            if any(isinstance(side, ast.Constant) and isinstance(side.value, str) for side in sides):
                used |= {sub.id for sub in ast.walk(node) if isinstance(sub, ast.Name)}
    return used


def path_override_envs(root: Path, *, exclude_vendored: bool = True) -> dict[str, list[str]]:
    """`root/**.py` 里「env 值最终进路径」的 env 名 → 命中站点。

    命中面 = 读取点直接在 `Path(...)` / `.resolve()` / `/ "段"` 里，或读取结果先赋给一个
    名字、该名字在同作用域内被同样消费。f-string 与 `str.format` **不算**路径语境（URL 与
    消息模板会假报）。`_vendored/**` 是外部代码，其 env 名不由本仓契约管，默认排除。
    """
    files = [
        path
        for path in sorted(root.rglob("*.py"))
        if "__pycache__" not in path.parts and not (exclude_vendored and "_vendored" in path.parts)
    ]
    trees = {path: ast.parse(path.read_text(encoding="utf-8")) for path in files}
    consts: dict[str, str] = {}
    for tree in trees.values():
        consts.update(_env_consts(tree))

    hits: dict[str, list[str]] = {}
    for path, tree in trees.items():
        parents = {id(child): node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        for node in ast.walk(tree):
            name = _env_read(node, consts)
            if name is None:
                continue
            scope = _flow_scope(node, parents, tree)
            override = _joins_a_path(node, parents, scope) or bool(
                _assigned_names(node, scope) & _path_consumed_names(scope)
            )
            if override:
                hits.setdefault(name, []).append(f"{path.relative_to(root).as_posix()}:{node.lineno}")
    return hits


def test_profile_env_roster_equals_the_derived_path_override_face() -> None:
    """名单的两侧都对拍：源码里新增一个路径覆盖 env 而不进名单 → 红；名单里留一个死名 → 红。"""
    derived = path_override_envs(SRC_ROOT)
    diff = {
        "missing_from_roster": {name: derived[name] for name in sorted(set(derived) - set(PROFILE_ENVS))},
        "not_derived_from_src": sorted(set(PROFILE_ENVS) - set(derived)),
    }
    assert set(PROFILE_ENVS) == set(derived), json.dumps(diff, indent=2)
    assert len(PROFILE_ENVS) == len(set(PROFILE_ENVS)), f"名单含重名: {PROFILE_ENVS}"
    # 名单必须真的含 ADR-0456 的 profile 面本身 —— 手抄的那版缺的正是这两个里的第一个
    assert omo_paths.STATE_ROOT_ENV in PROFILE_ENVS
    assert omo_paths.LEDGER_DB_ENV in PROFILE_ENVS


def test_roster_detector_names_an_injected_override_and_skips_a_port(tmp_path: Path) -> None:
    """空绿反证：注入一条路径覆盖读取，尺必须点名它；端口/URL 读取不得被算进来。"""
    module = tmp_path / "injected.py"
    module.write_text(
        "\n".join(
            [
                "import os",
                "from pathlib import Path",
                "",
                'ROOT = Path(os.environ.get("INJECT_PATH_ENV", str(Path.cwd())))',
                'declared = os.environ.get("INDIRECT_PATH_ENV")',
                "CANDIDATE = Path(declared)",
                'PORT = int(os.environ.get("INJECT_PORT_ENV", "8080"))',
                'URL = os.environ.get("INJECT_URL_ENV", "http://localhost")',
                'FLAG = bool(os.environ.get("INJECT_FLAG_ENV"))',
                "",
            ]
        ),
        encoding="utf-8",
    )
    derived = path_override_envs(tmp_path)
    assert set(derived) == {"INJECT_PATH_ENV", "INDIRECT_PATH_ENV"}, derived
    for name in ("INJECT_PORT_ENV", "INJECT_URL_ENV", "INJECT_FLAG_ENV"):
        assert name not in derived, f"非路径名被算进隔离面: {name}"


def test_roster_detector_excludes_vendored_names_by_boundary(tmp_path: Path) -> None:
    """排除 `_vendored` 是边界，不是尺没吃到 —— 同一棵树在含/不含两种口径下读数不同。"""
    vendored = tmp_path / "_vendored" / "bridge.py"
    vendored.parent.mkdir(parents=True)
    vendored.write_text(
        'import os\nfrom pathlib import Path\n\nD = Path(os.environ.get("VENDORED_PATH_ENV", "."))\n',
        encoding="utf-8",
    )
    assert set(path_override_envs(tmp_path)) == set()
    assert set(path_override_envs(tmp_path, exclude_vendored=False)) == {"VENDORED_PATH_ENV"}


def test_vendored_env_names_are_outside_the_roster_in_live_src() -> None:
    """活源码里 `_vendored` 确实读了 env 路径名，且这些名字不在契约名单里（边界有对象）。"""
    vendored = path_override_envs(SRC_ROOT, exclude_vendored=False)
    live = set(path_override_envs(SRC_ROOT))
    outside = sorted(set(vendored) - live)
    assert outside, "_vendored 里没有路径覆盖 env 名 —— 该边界可能已失效"
    assert set(outside).isdisjoint(PROFILE_ENVS), f"外部代码的 env 名混进了本仓契约名单: {outside}"
