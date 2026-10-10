"""BET-Y2Q4-T10-239 ISC-1 / ISC-4 —— `code_root()` 的语义与「未声明 profile 时逐字节不变」。

ISC-1 关心**缝本身**：`omo.omo_paths.code_root()` 必须与根仓
`bin/lib/repo_root.py:code_root()` 同语义 —— 跟随当前检出、**不**读 `OMOSTATION_ROOT`
（读它的是 `canonical_root()`）。两仓同名的两个函数一旦分叉，「单一口径」就退回文档措辞。

ISC-4 关心**改后不换位置**：把 `WORKSPACE_ROOT` 钉回那 32 处历史上恒取的 host 字面量
（`Path.home() / "Workspace"`），每个被收敛的 resolver 必须逐字节复现改前的落点。
这是唯一能抓「挂错根」的判据 —— 把 `.omo/_knowledge/**` 的写者错挂到 code 根、
或把读检出源码的站点错挂到 state 根，绿测试都看不见（spec §2 ISC-4 的证伪条款）。
尾段取自 d8ca1a2 的基线语料（同目录 `omo_src_root_literal_baseline.json`），
所以期望值不是凭记忆写的。
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest

from omo import omo_paths
from omo.omo_paths import OMO_SRC_PARENT, PROFILE_ENVS

# 隔离名单只有一个来源（`omo_paths.PROFILE_ENVS`），内容由
# test_omo_root_plane_landing.py 的 AST 判据从源码对拍。本文件此前自带一份手抄名单，
# 抄漏了 `OMOSTATION_STATE_ROOT` 本身，于是「未声明 profile 两根相等」在已声明 profile
# 的 env 下当场红（dev profile 全量跑实测 1 failed / 3040 passed，BET-Y2Q4-T10-239）。

# (dotted resolver, 改前那行的尾段, 语料文件) —— 尾段用于回查基线语料
HISTORICAL_PATHS: list[tuple[str, str, tuple[str, ...]]] = [
    ("omo.omo_trail.default_trail_path", ".omo/_knowledge/omo-trail.jsonl", ()),
    ("omo.omo_event.default_event_log_path", ".omo/_knowledge/omo-events.jsonl", ()),
    ("omo.omo_sync.default_sync_log_path", ".omo/_knowledge/omo-sync.jsonl", ()),
    ("omo.omo_alert.alert_log_path", ".omo/_knowledge/omo-alerts.jsonl", ()),
    ("omo.omo_bos_metrics.default_metrics_path", ".omo/_knowledge/bos-metrics.jsonl", ()),
    ("omo.omo_logs.knowledge_dir", ".omo/_knowledge", ()),
    ("omo.omo_observability.knowledge_dir", ".omo/_knowledge", ()),
    # 读侧：检出内的脚本与源码
    (
        "omo.omo_alert.notify_script",
        "projects/runtime/scripts/notify-alerts.sh",
        ("src/omo/omo_alert.py", "notify-alerts.sh"),
    ),
    (
        "omo.omo_bos.kairon_packages_src",
        "projects/knowledge/kairon/packages/kos/src",
        ("src/omo/omo_bos.py", '"projects"'),
    ),
    (
        "omo.omo_bos.default_registry_path",
        ".omo/_knowledge/bos-registry.json",
        ("src/omo/omo_bos.py", "bos-registry.json"),
    ),
    (
        "omo.omo_self_healing.omo_project_root",
        "projects/omo",
        ("src/omo/omo_self_healing.py", "Workspace/projects/omo"),
    ),
    (
        "omo.omo_self_healing.healing_config_path",
        "projects/omo/.omo/self_healing_rules.yaml",
        # 改前那行是 `HEALING_CONFIG_PATH = OMO_ROOT / ".omo" / "self_healing_rules.yaml"`，
        # 而语料只记录命中行，所以这里回查的是它挂在的那个根（OMO_ROOT 的缺省值）。
        ("src/omo/omo_self_healing.py", "Workspace/projects/omo"),
    ),
    # 台账两面：改前各自手拼 runtime/omo/event-ledger.sqlite3
    ("omo.sovereignty.enforcement._default_db_path", "runtime/omo/event-ledger.sqlite3", ()),
    ("omo.event_ledger.surface._default_db_path", "runtime/omo/event-ledger.sqlite3", ()),
]


@pytest.fixture
def host_pinned_root(monkeypatch: pytest.MonkeyPatch) -> Path:
    """把仓根钉回 d8ca1a2 上那 32 处恒取的 host 字面量，并清掉所有 profile 声明。"""
    host = Path.home() / "Workspace"
    for name in PROFILE_ENVS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(omo_paths, "WORKSPACE_ROOT", host)
    return host


@pytest.fixture
def untouched_root(monkeypatch: pytest.MonkeyPatch) -> Path:
    """未声明 profile、也不改写常量：取本检出的真实取值。"""
    for name in PROFILE_ENVS:
        monkeypatch.delenv(name, raising=False)
    return omo_paths.WORKSPACE_ROOT


def _resolve(dotted: str):
    module_name, _, attr = dotted.rpartition(".")
    module = importlib.import_module(module_name)
    return getattr(module, attr)


def _load_root_repo_module() -> ModuleType:
    """按路径加载根仓 `bin/lib/repo_root.py`（纯 stdlib，可独立加载）。"""
    candidate = OMO_SRC_PARENT.parents[1] / "bin" / "lib" / "repo_root.py"
    if not candidate.is_file():
        pytest.skip(f"独立 omo 检出里没有根仓可比对: {candidate}")
    spec = importlib.util.spec_from_file_location("root_repo_under_test", candidate)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# ── ISC-1: 缝的语义 ────────────────────────────────────────────────


def test_code_root_ignores_omostation_root(untouched_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """证伪条款：让它读 OMOSTATION_ROOT 就与 bin/lib/repo_root.py 分叉，且违反 ADR-0456 读面契约。"""
    bogus = Path("/tmp/definitely-not-a-checkout-10239")
    monkeypatch.setenv("OMOSTATION_ROOT", str(bogus))
    assert omo_paths.code_root() == untouched_root
    assert omo_paths.code_root() != bogus


def test_code_root_equals_workspace_root_without_profile(untouched_root: Path) -> None:
    """未声明任何 profile env 时 `code_root() == WORKSPACE_ROOT`（spec §2 ISC-1 末句）。"""
    assert omo_paths.code_root() == untouched_root == omo_paths.WORKSPACE_ROOT


def test_code_root_follows_checkout_not_install_root(untouched_root: Path) -> None:
    """跟随当前检出：返回的是本模块所在检出往上四层，且该检出下确有 src/omo。"""
    assert omo_paths.code_root() == untouched_root
    assert (OMO_SRC_PARENT / "src" / "omo" / "omo_paths.py").is_file()


def test_two_repos_code_root_agree(untouched_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """两仓的 `code_root()` 同语义：同一检出、同样无视 OMOSTATION_ROOT。"""
    root_module = _load_root_repo_module()
    bogus = Path("/tmp/definitely-not-a-checkout-10239")
    monkeypatch.setenv("OMOSTATION_ROOT", str(bogus))
    assert root_module.code_root() == omo_paths.code_root() == untouched_root
    # 边界：读 OMOSTATION_ROOT 的是 canonical_root()，且它只认带 MARKER 的目录 —— 本 BET 不接它。
    # MARKER 的实际值是 `docs/project-registry.yaml`（不是字面的 MARKER 文件），
    # 所以这里必须用 root_module.MARKER 拼，写死 "MARKER" 会得到一个假失败。
    try:
        resolved = root_module.canonical_root()
    except RuntimeError:
        resolved = None
    assert resolved != bogus, "canonical_root() 不能把无 MARKER 的目录当规范检出"
    assert resolved is None or (resolved / root_module.MARKER).is_file()


def test_state_root_is_call_time_not_import_time(untouched_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """缝必须是调用时刻解析：晚声明的 profile 不能被 import 时求值的常量冻住。"""
    assert omo_paths.state_root() == untouched_root
    declared = untouched_root / ".profile-probe-10239"
    monkeypatch.setenv("OMOSTATION_STATE_ROOT", str(declared))
    assert omo_paths.state_root() == declared
    # 反面：模块级 STATE_ROOT 常量做不到（它停留在 import 时的取值）
    assert omo_paths.STATE_ROOT != omo_paths.state_root()


def test_event_ledger_env_takes_priority_over_state_root(monkeypatch: pytest.MonkeyPatch) -> None:
    """优先级 `OMO_EVENT_LEDGER_DB` > `state_root()/runtime/omo/...`，与根仓同名函数同契约。"""
    db = Path("/tmp/ledger-10239.sqlite3")
    monkeypatch.setenv("OMO_EVENT_LEDGER_DB", str(db))
    assert omo_paths.event_ledger_path() == db


# ── ISC-4: 未声明 profile 时的逐字节无效应 ─────────────────────────


def test_every_converged_site_reproduces_its_historical_path(host_pinned_root: Path) -> None:
    """14 个被收敛的落点逐一等于「host 字面量 + 改前尾段」，逐字节。"""
    wrong: dict[str, str] = {}
    for dotted, tail, _grounding in HISTORICAL_PATHS:
        expected = host_pinned_root / Path(tail)
        if dotted.endswith("_default_db_path"):
            expected = expected.resolve()
        actual = _resolve(dotted)()
        if Path(actual) != expected:
            wrong[dotted] = f"expected {expected} got {actual}"
    assert not wrong, "\n".join(f"{k}: {v}" for k, v in wrong.items())


def test_historical_tails_are_grounded_in_the_baseline_corpus() -> None:
    """期望尾段必须能在 d8ca1a2 的语料里找到，防止把改前路径凭记忆写错。"""
    corpus = json.loads(
        (Path(__file__).resolve().parent / "omo_src_root_literal_baseline.json").read_text(encoding="utf-8")
    )
    for _dotted, _tail, grounding in HISTORICAL_PATHS:
        if not grounding:
            continue
        rel, needle = grounding
        assert rel in corpus, f"语料里没有 {rel}"
        joined = "\n".join(item["text"] for item in corpus[rel])
        assert needle in joined, f"改前那行不含 {needle!r}: {joined[:200]}"


def test_planes_do_not_swap_under_the_canonical_layout(untouched_root: Path) -> None:
    """挂错根的第二种抓法：两面在未声明 profile 时同源，但函数各自仍按面解析。

    本机若在规范检出（仓根 == host 字面量）跑，就顺手验证写面文件确实还躺在老位置。
    """
    from omo import omo_trail

    assert omo_paths.state_root() == omo_paths.code_root()
    trail = omo_trail.default_trail_path()
    assert trail.parent == omo_paths.code_root() / ".omo" / "_knowledge"
    if omo_paths.code_root() == Path.home() / "Workspace":
        assert trail.is_file(), f"规范检出上改后落点应命中已在写的 {trail}"
