"""OMO 路径常量(从 kairon_governance.paths 迁移, 适配 omo 包布局).

路径推导:
  omo/src/omo/omo_paths.py
    parents[0] = omo/src/omo  (module dir)
    parents[1] = omo/src
    parents[2] = omo project (OMO_SRC_PARENT)
    parents[3] = projects
    parents[4] = Workspace   (WORKSPACE_ROOT)
    parents[5] = $HOME

ADR-0456 code/state 两根: WORKSPACE_ROOT 跟随当前检出(读侧, 治理 SSOT 从这里取),
STATE_ROOT 由 $OMOSTATION_STATE_ROOT 声明(写侧, 运行态与生成态从这里落位)。
未声明 profile 时 STATE_ROOT == WORKSPACE_ROOT, 逐字节复现历史路径。
"""

from __future__ import annotations

import os
from pathlib import Path

STATE_ROOT_ENV = "OMOSTATION_STATE_ROOT"

_MODULE_DIR = Path(__file__).resolve().parent
OMO_SRC_PARENT = _MODULE_DIR.parents[1]  # /Users/xiamingxing/Workspace/projects/omo
PROJECTS_DIR = _MODULE_DIR.parents[2]  # /Users/xiamingxing/Workspace/projects
WORKSPACE_ROOT = _MODULE_DIR.parents[3]  # /Users/xiamingxing/Workspace
HOME_DIR = _MODULE_DIR.parents[4]  # /Users/xiamingxing

# 运行态写入根; 与 bin/lib/repo_root.py:state_root() 同契约 (此处不跨仓边界导入)。
STATE_ROOT = (
    Path(os.environ[STATE_ROOT_ENV]).expanduser().absolute() if os.environ.get(STATE_ROOT_ENV) else WORKSPACE_ROOT
)

# 关键路径
OMO_ROOT = WORKSPACE_ROOT / ".omo"
KAIRON_DIR = PROJECTS_DIR / "knowledge" / "kairon"  # T6-01 内包后路径
KAIRON_PACKAGES = KAIRON_DIR / "packages"

# 运行时镜像根 (高 churn 的 self-healing/ingress/evolution 产物写这里, 不入仓)
RUNTIME_OMO_ROOT = STATE_ROOT / "runtime" / "omo"

# 治理子路径 (稳定 SSOT, 入仓)
TRUTH_DIR = OMO_ROOT / "_truth"
CONTROL_DIR = OMO_ROOT / "_control"
DELIVERY_DIR = OMO_ROOT / "_delivery"
ARCHIVE_DIR = OMO_ROOT / "_archive"
EVIDENCE_DIR = DELIVERY_DIR / "evidence"
EVIDENCE_LEGACY_DIR = DELIVERY_DIR / "evidence-legacy"
EVIDENCE_ALIAS_DIR = OMO_ROOT / "evidence"
KNOWLEDGE_DIR = OMO_ROOT / "_knowledge"
LOG_DIR = OMO_ROOT / "_log"
STANDARDS_DIR = OMO_ROOT / "standards"
CRON_DIR = OMO_ROOT / "cron"
GOALS_DIR = OMO_ROOT / "goals"
PITCHES_DIR = OMO_ROOT / "pitches"
TESTS_DIR = OMO_ROOT / "tests"
CAPABILITIES_DIR = OMO_ROOT / "capabilities"
CHANGE_LOG_DIR = OMO_ROOT / "change-log"
TASKS_DIR = OMO_ROOT / "tasks"
TASKS_PLANNED_DIR = OMO_ROOT / "tasks" / "planned"
TASKS_ACTIVE_DIR = OMO_ROOT / "tasks" / "active"
TASKS_DONE_DIR = OMO_ROOT / "tasks" / "done"
STATE_DIR = STATE_ROOT / ".omo" / "state"
WORKERS_DIR = OMO_ROOT / "workers"
DEBT_DIR = OMO_ROOT / "debt"
DECISIONS_DIR = KNOWLEDGE_DIR / "decisions"
DEBT_ITEMS_DIR = OMO_ROOT / "debt" / "items"
STATE_SYSTEM_YAML = STATE_DIR / "system.yaml"
PROJECTS_REGISTRY_YAML = OMO_ROOT / "PROJECTS.yaml"
ROOT_INDEX_MD = OMO_ROOT / "INDEX.md"
OMO_GOVERNANCE_SURFACES_STANDARD = STANDARDS_DIR / "omo-governance-surfaces.md"
OMO_GOVERNANCE_SURFACES_REGISTRY = TRUTH_DIR / "registry" / "omo-governance-surfaces.yaml"

# 运行时镜像子路径 (高 churn 产物)
RUNTIME_DELIVERY_DIR = RUNTIME_OMO_ROOT / "_delivery"
RUNTIME_CONTROL_DIR = RUNTIME_OMO_ROOT / "_control"
RUNTIME_CHANGE_LOG_DIR = RUNTIME_OMO_ROOT / "change-log"
RUNTIME_TASKS_DIR = RUNTIME_OMO_ROOT / "tasks"
RUNTIME_TRUTH_DIR = RUNTIME_OMO_ROOT / "_truth"


def runtime_omo_path(relative: str | Path) -> Path:
    """Return a runtime mirror path under RUNTIME_OMO_ROOT.

    Example:
        runtime_omo_path("_delivery/ingress/registry.yaml")
        -> /.../Workspace/runtime/omo/_delivery/ingress/registry.yaml
    """
    return RUNTIME_OMO_ROOT / Path(relative)


def ensure_runtime_omo_dir(relative: str | Path) -> Path:
    """Create the runtime mirror directory if missing and return it."""
    path = RUNTIME_OMO_ROOT / Path(relative)
    path.mkdir(parents=True, exist_ok=True)
    return path


# Runtime projection registry (ADR-0129)
RUNTIME_PROJECTIONS_REGISTRY = TRUTH_DIR / "registry" / "runtime-projections.yaml"

# Used only when the registry itself is not in this checkout (omo consumed as a
# library without the workspace `.omo/` plane). Same values the registry declares
# today; drift is pinned by tests/unit/test_projection_reader_resolution.py.
_PROJECTION_FALLBACK_RELS: dict[str, tuple[str, str]] = {
    "health": (".omo/state/runtime/health.yaml", ".omo/state/health.yaml"),
    "system_health": (".omo/state/runtime/system_health.yaml", ".omo/state/system_health.yaml"),
    "governance_data": (".omo/state/runtime/governance-data.json", ".omo/_control/governance-data.json"),
    "brief": (".omo/state/runtime/brief.md", "BRIEF.md"),
}


def projection_rels(name: str) -> tuple[str, str]:
    """已登记投影的 (canonical, legacy) **相对路径** —— 映射的唯一真源。

    路径是登记表的事, 锚点是 profile 的事: canonical 挂 STATE_ROOT (写入侧),
    legacy 挂 WORKSPACE_ROOT (当前检出侧)。把"读哪个文件"与"哪一层根"分开, 才不会出现
    每个消费者各抄一份映射 —— 那是 ADR-0129 Phase 2 之后 legacy 冻结事故的根因
    (读侧写死 legacy 路径的tools, 在生产机上量到一个不再被写的文件)。

    未登记的名字抛 KeyError: 静默回退会把"读错文件"伪装成"读到了"。
    """
    if RUNTIME_PROJECTIONS_REGISTRY.is_file():
        import yaml

        # The registry carries YAML frontmatter, so it is a multi-document stream;
        # the last document is the registry itself.
        docs = yaml.safe_load_all(RUNTIME_PROJECTIONS_REGISTRY.read_text(encoding="utf-8"))
        data = [d for d in docs if isinstance(d, dict)][-1]
        entry = (data.get("projections") or {}).get(name)
        if entry is None:
            raise KeyError(f"Unknown runtime projection: {name}")
        return entry["canonical"], entry["legacy"]

    fallback = _PROJECTION_FALLBACK_RELS.get(name)
    if fallback is None:
        raise KeyError(f"Unknown runtime projection: {name}")
    return fallback


def projection_path(name: str, *, prefer_canonical: bool = True) -> Path:
    """Resolve the path of a registered runtime projection.

    Consumers should use this instead of hard-coding paths like
    `.omo/state/health.yaml` or `BRIEF.md`.

    By default returns the canonical path if it exists, otherwise falls back
    to the legacy path. During ADR-0129 migration this lets readers find
    projections regardless of which phase the workspace is in.

    Canonical lands on STATE_ROOT (written state); legacy stays on
    WORKSPACE_ROOT so a dev profile can still read the committed file.

    返回的路径**不存在**就是两侧都不存在 —— 投影没生成过, 这不是错误状态, 读取方
    应当报 absent 而不是计为 fail/expired。
    """
    canonical_rel, legacy_rel = projection_rels(name)
    canonical = STATE_ROOT / canonical_rel
    legacy = WORKSPACE_ROOT / legacy_rel

    if prefer_canonical:
        if canonical.exists():
            return canonical
        if legacy.exists():
            return legacy
        return canonical  # return canonical for writers even if missing
    return legacy if legacy.exists() else canonical


def system_yaml_for(omo_dir: Path | None = None) -> Path:
    """system.yaml 的**写**目标 —— 运行态根那份 (ADR-0456 D1 / BET-Y2Q4-T10-220).

    `find_omo_dir()` 返回的是**检出**里的 `.omo`, 拿它拼 `state/system.yaml` 会让声明
    `OMOSTATION_STATE_ROOT` 的一次 sync 改写检出那份跟踪快照 —— 检出侧从此只是
    "最后提交的快照", 运行态只落 `<state_root>/.omo/state/system.yaml`。

    显式传入的其他目录(测试 fixture、`--omo-dir` 指到别处)按原样返回: 落点必须能在
    指定根上验证, 否则"写到哪"变成不可断言的事。未声明 profile 时 STATE_ROOT ==
    WORKSPACE_ROOT, 两条分支逐字节相同。
    """
    if omo_dir is not None and omo_dir.resolve() != OMO_ROOT.resolve():
        return omo_dir / "state" / "system.yaml"
    return STATE_SYSTEM_YAML


def system_yaml_read(omo_dir: Path | None = None) -> Path:
    """system.yaml 的读目标: 运行态根那份优先, 检出快照兜底。

    与 projection_path() 同语义, 但 system.yaml **不**注册成投影 —— 它是同一个文件的
    "快照 + 运行态镜像", 不是 canonical/legacy 两个名字(注册会让投影名映射把快照判成
    legacy 位)。显式目录只看它自己。
    """
    target = system_yaml_for(omo_dir)
    if target.is_file():
        return target
    if omo_dir is not None and omo_dir.resolve() != OMO_ROOT.resolve():
        return target
    return OMO_ROOT / "state" / "system.yaml"


# Agora 路由表 (P30 拆分后, agora 已迁出 kairon, 现位于 projects/agora)
# P31-W0-AGORA-ACTUAL-FIX: 修正路径指向
AGORA_ROUTES_PATH = PROJECTS_DIR / "agora" / "src" / "agora-routes.json"

# 治理历史 (kairon-governance 旧 JSONL 路径保持不变, 保证历史连续性)
GOVERNANCE_HISTORY_PATH = KNOWLEDGE_DIR / "governance-history.jsonl"

# Daemon 运行时
DAEMON_PID_FILE = Path("/tmp/omo-governance-daemon.pid")
DAEMON_LOG_FILE = DELIVERY_DIR / "daemon.log"


def find_omo_dir(start: Path | None = None) -> Path:
    """Resolve the authoritative workspace .omo directory.

    Runtime commands should prefer the workspace root declared by this package,
    instead of accidentally binding to legacy shadow `.omo` directories inside
    subrepositories such as `projects/omo/.omo`.
    """
    if OMO_ROOT.is_dir():
        return OMO_ROOT
    current = (start or Path.cwd()).resolve()
    while current != current.parent:
        candidate = current / ".omo"
        if candidate.is_dir():
            return candidate
        current = current.parent
    return (start or Path.cwd()) / ".omo"


__all__ = (
    "AGORA_ROUTES_PATH",
    "ARCHIVE_DIR",
    "CAPABILITIES_DIR",
    "CHANGE_LOG_DIR",
    "CONTROL_DIR",
    "CRON_DIR",
    "DAEMON_LOG_FILE",
    "DAEMON_PID_FILE",
    "DEBT_DIR",
    "DEBT_ITEMS_DIR",
    "DECISIONS_DIR",
    "DELIVERY_DIR",
    "EVIDENCE_ALIAS_DIR",
    "EVIDENCE_DIR",
    "EVIDENCE_LEGACY_DIR",
    "GOALS_DIR",
    "GOVERNANCE_HISTORY_PATH",
    "HOME_DIR",
    "KAIRON_DIR",
    "KAIRON_PACKAGES",
    "KNOWLEDGE_DIR",
    "LOG_DIR",
    "OMO_GOVERNANCE_SURFACES_REGISTRY",
    "OMO_GOVERNANCE_SURFACES_STANDARD",
    "OMO_ROOT",
    "OMO_SRC_PARENT",
    "PITCHES_DIR",
    "PROJECTS_DIR",
    "PROJECTS_REGISTRY_YAML",
    "ROOT_INDEX_MD",
    "RUNTIME_CHANGE_LOG_DIR",
    "RUNTIME_CONTROL_DIR",
    "RUNTIME_DELIVERY_DIR",
    "RUNTIME_OMO_ROOT",
    "RUNTIME_TASKS_DIR",
    "RUNTIME_TRUTH_DIR",
    "STANDARDS_DIR",
    "STATE_DIR",
    "STATE_ROOT",
    "STATE_ROOT_ENV",
    "STATE_SYSTEM_YAML",
    "TASKS_DIR",
    "TASKS_PLANNED_DIR",
    "TESTS_DIR",
    "TRUTH_DIR",
    "WORKERS_DIR",
    "WORKSPACE_ROOT",
    "ensure_runtime_omo_dir",
    "find_omo_dir",
    "runtime_omo_path",
    "system_yaml_for",
    "system_yaml_read",
)
