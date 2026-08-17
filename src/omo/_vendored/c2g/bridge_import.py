import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path

import yaml

from .bridge_depend import _resolve_depends_on
from .bridge_id import _generate_task_id, _infer_phase_wave
from .omo_client import create_planned_task_via_broker, validate_planned_task_data
from .task_builder import build_ecos_task, build_local_task


def _parse_pitch_frontmatter(content: str) -> tuple[str | None, str, str | None]:
    """Parse leading blockquote frontmatter for Upstream, Appetite, Scenario.

    Only the first contiguous blockquote block at the top of the document is
    inspected, so body sections like `## Boundaries & Appetites` cannot shadow
    the real frontmatter values.

    Scenario (BET-7, 2026-06-27): optional field from c2g ``brainstorm --scenario``,
    format ``> **Scenario**: <key> (<label>)``; only the key (e.g. "finance") is
    extracted and passed through to task metadata for routing/audit/filtering.
    """
    lines = content.splitlines()
    frontmatter_lines: list[str] = []
    in_frontmatter = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if in_frontmatter:
                break
            continue
        if stripped.startswith(">"):
            in_frontmatter = True
            frontmatter_lines.append(stripped)
        elif in_frontmatter:
            break
        # Skip leading title/comment lines before the frontmatter block.

    text = "\n".join(frontmatter_lines)
    upstream_match = re.search(r"^>\s*\*\*Upstream\*\*:\s*(.+)$", text, re.MULTILINE)
    appetite_match = re.search(r"^>\s*\*\*Appetite:\*\*\s*(.+)$", text, re.MULTILINE)
    scenario_match = re.search(r"^>\s*\*\*Scenario\*\*:\s*(\S+)", text, re.MULTILINE)
    upstream = upstream_match.group(1).strip() if upstream_match else None
    appetite = appetite_match.group(1).strip() if appetite_match else "Unknown"
    scenario = scenario_match.group(1).strip() if scenario_match else None
    return upstream, appetite, scenario


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _validate_ecos_task(task_data: dict) -> bool:
    try:
        errors = validate_planned_task_data(task_data)
        if errors:
            print("  ❌ M2 防腐层拦截 (Schema Validation Failed)")
            for err in errors:
                print(f"     - {err}")
            return False
        return True
    except ImportError:
        import json

        import httpx

        try:
            response = httpx.post("http://localhost:9190/omo/validate-task", json=task_data, timeout=10.0)
            response.raise_for_status()
            result = response.json()
            if not result.get("valid", False):
                errors = result.get("errors", [])
                print("  ❌ M2 防腐层拦截 (Schema Validation Failed)")
                for err in errors:
                    print(f"     - {err}")
                return False
            return True
        except httpx.HTTPError as e:
            print(f"  ❌ M2 防腐层拦截 (HTTP Error: {e})")
            return False
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  ❌ M2 防腐层拦截 (Response Parse Error: {e})")
            return False


def _write_ecos_task(omo_dir: Path, task_id: str, task_data: dict) -> None:
    create_planned_task_via_broker(
        omo_dir,
        task_data=task_data,
        source_ref=f"c2g:bridge-import:{task_id}",
    )


def _save_local_task(base_dir: Path, task_data: dict, adapter: str) -> bool:
    from .adapters import get_providers
    from .domain import TaskSchema

    gov, store = get_providers(str(base_dir), adapter)
    task = TaskSchema(**task_data)
    if not gov.validate_task(task):
        print("  ❌ 本地治理校验失败")
        return False
    store.save_task(task)
    return True


def _import_bmad(file_path: Path, omo_dir: Path, sequential: bool = False, adapter: str = "ecos"):
    print(f"🌉 正在将 BMAD / OpenSpec 规范转换为 OMO Planned Tasks: {file_path}")
    content = file_path.read_text(encoding="utf-8")
    tasks_created = 0

    planned_dir = omo_dir / "tasks" / "planned"
    planned_dir.mkdir(parents=True, exist_ok=True)

    # Pass 1: 解析 QA 质量保障模块 (Test Plan & Evidence Required)
    test_plan_parsed = []
    evidence_parsed = []
    in_test, in_evid = False, False
    for line in content.split("\n"):
        if line.startswith("### 7.1"):
            in_test, in_evid = True, False
            continue
        elif line.startswith("### 7.2"):
            in_test, in_evid = False, True
            continue
        elif line.startswith("#"):
            in_test, in_evid = False, False
            continue
        if in_test and line.strip().startswith("- "):
            test_plan_parsed.append(line.split("- ", 1)[1].strip())
        elif in_evid and line.strip().startswith("- "):
            evidence_parsed.append(line.split("- ", 1)[1].strip())

    if not test_plan_parsed:
        test_plan_parsed = ["[Fallback] Default test plan"]
    if not evidence_parsed:
        evidence_parsed = ["[Fallback] Default evidence"]

    # Pass 2: 收集所有 - [ ] 行的 title, 算好 title → IMPORTED id 映射.
    title_to_imported: dict[str, str] = {}
    parsed_tasks: list[tuple[str, list[str]]] = []

    for line in content.split("\n"):
        if "- [ ]" not in line:
            continue
        raw_title = line.split("- [ ]")[1].strip()

        depends_on_raw: list[str] = []
        if "(depends_on:" in raw_title:
            parts = raw_title.split("(depends_on:")
            task_title = parts[0].strip()
            deps_str = parts[1].split(")")[0].strip()
            depends_on_raw = [d.strip() for d in deps_str.split(",") if d.strip()]
        else:
            task_title = raw_title

        title_to_imported[task_title] = _generate_task_id(task_title)
        parsed_tasks.append((task_title, depends_on_raw))

    # Pass 3: 写文件, depends_on 用 _resolve_depends_on 替换为真实 IMPORTED id.
    last_task_id: str | None = None
    for idx, (task_title, depends_on_raw) in enumerate(parsed_tasks):
        task_id = title_to_imported[task_title]

        if depends_on_raw:
            depends_on = _resolve_depends_on(depends_on_raw, title_to_imported)
        elif sequential and last_task_id:
            depends_on = [last_task_id]
        else:
            depends_on = []

        phase, wave = _infer_phase_wave(task_title)

        # [DEVIL'S GATEKEEPER]: OMO Pre-Check Before Materialization
        if "TODO" in task_title or "TBD" in task_title:
            print(
                f"  ❌ 预检拦截 (Pre-check Failed): 任务 {task_id} 含有未决议项 ({task_title})，拒绝流入 OMO 稳态区。"
            )
            continue

        if adapter == "ecos":
            task_data = build_ecos_task(
                task_id,
                task_title,
                depends_on=depends_on,
                source_docs=[str(file_path.absolute())],
                evidence_required=evidence_parsed,
                test_plan=test_plan_parsed,
                context_uri=f"bos://memory/openspecs/{file_path.name}#{task_id}",
                extra={"phase": phase, "wave": wave} if phase or wave else None,
            )
            if not _validate_ecos_task(task_data):
                continue
            _write_ecos_task(omo_dir, task_id, task_data)
        else:
            task_data = build_local_task(
                task_id,
                task_title,
                description=f"Imported from {file_path.name}",
                depends_on=depends_on,
                source_docs=[str(file_path.absolute())],
                context_uri=f"bos://memory/openspecs/{file_path.name}#{task_id}",
                extra={"metadata": {"phase": phase, "wave": wave}},
            )
            if not _save_local_task(omo_dir, task_data, adapter):
                continue

        print(f"  -> 创建了任务: {task_id} (依赖: {depends_on}) [M2 Validated]")
        tasks_created += 1
        last_task_id = task_id

    print(f"✅ 完成转换，共生成且经过 M2 强校验了 {tasks_created} 个任务。")


def _import_fast_track(source_topic: Path, omo_dir: Path, adapter: str = "ecos"):
    """[C2G v2] 解法二: Fast-Track 免签降维."""
    import time

    print(f"🚀 正在触发 Fast-Track 免签降维: {source_topic.name}")
    planned_dir = omo_dir / "tasks" / "planned"
    planned_dir.mkdir(parents=True, exist_ok=True)

    task_id = f"FAST-{int(time.time())}"

    if adapter == "ecos":
        task_data = build_ecos_task(
            task_id,
            str(source_topic.name),
            context_uri=f"bos://memory/fast-track/{task_id}",
            source_docs=["bos://memory/fast-track/virtual-doc"],
            entry_gate=["FAST_TRACK_L0"],
            imported_via="fast_track_cli",
        )
        if not _validate_ecos_task(task_data):
            return
        _write_ecos_task(omo_dir, task_id, task_data)
    else:
        task_data = build_local_task(
            task_id,
            str(source_topic.name),
            description="Fast-track task",
            source_docs=["bos://memory/fast-track/virtual-doc"],
            context_uri=f"bos://memory/fast-track/{task_id}",
            imported_via="fast_track_cli",
        )
        if not _save_local_task(omo_dir, task_data, adapter):
            return

    print(f"✅ Fast-Track 成功: 已落盘为 OMO CARDS ({task_id}.yaml)")


def _import_pitch(source_file: Path, base_dir: Path, adapter: str = "ecos"):
    """[C2G v4] 将 Pitch (提案) 转换为 Bet 并在 OMO 中生成 Planned Task."""
    from .adapters import get_providers
    from .llm import extract_tasks_from_pitch

    _gov, store = get_providers(str(base_dir), adapter)
    print(f"🌉 [C2G v4] 正在将 Pitch 转化为 OMO Bet: {source_file.name}")
    content = source_file.read_text(encoding="utf-8")

    # [C2G v4] CR-STRATEGY-01 孤儿拦截约束
    # 用正则解析文档头部连续 blockquote frontmatter，避免被正文同名列表项覆盖。
    upstream, appetite, scenario = _parse_pitch_frontmatter(content)

    if not upstream:
        print(
            "  ❌ [CR-STRATEGY-01 孤儿拦截] Pitch 缺乏 Upstream 锚点，拒绝转化为 Bet。请在文档头部声明 `> **Upstream**: MS-XXX`。"
        )
        return

    # 创建 Bet (Goal)
    bet_id = f"BET-{hashlib.md5(source_file.name.encode()).hexdigest()[:4]}"
    desc = f"Bet: {source_file.stem} (Appetite: {appetite})"

    # [C2G v4] BET ID 重用检测 — graceful skip
    if adapter == "ecos":
        goals_file = base_dir / "goals" / "current.yaml"
        if goals_file.exists():
            from .bridge_utils import strip_frontmatter

            goals_data = next(yaml.safe_load_all(strip_frontmatter(goals_file.read_text())), {})
            existing_ids = [g.get("id") for g in goals_data.get("goals", [])]
            if bet_id in existing_ids:
                print(f"  ⏭  Bet {bet_id} already exists, skip.")
                print("✅ Bet 下注成功: 共创建了 0 个执行计划。")
                return

    from .domain import BetSchema

    try:
        store.save_bet(
            BetSchema(
                goal_id=bet_id,
                title=source_file.stem,
                description=desc,
                appetite=appetite,
                created_at=_utc_now(),
            )
        )
    except ValueError as e:
        if "already exists" in str(e):
            print(f"  ⏭  Bet {bet_id} already exists (payload changed), skip.")
            print("✅ Bet 下注成功: 共创建了 0 个执行计划。")
            return
        raise

    # 派生 Planned Task
    planned_dir = base_dir / "tasks" / "planned"
    planned_dir.mkdir(parents=True, exist_ok=True)

    print("  🧠 正在调用 LLM 结构化提取任务...")
    llm_tasks = extract_tasks_from_pitch(content)

    if not llm_tasks:
        llm_tasks = [
            {
                "title": f"执行 {bet_id}: {source_file.stem}",
                "description": f"从 Pitch转化而来的任务: {source_file.stem}",
                "task_type": "feature",
                "risk_level": "L0",
                "cognitive_cartridge": "",
                "deliverables": [f"达成 {bet_id}"],
                "evidence_required": ["回写 Pitch 并通过 Bet 验收"],
                "test_plan": ["依据 Pitch 验收"],
            }
        ]

    tasks_created = 0
    for idx, extracted in enumerate(llm_tasks):
        task_id = f"IMPORTED-{hashlib.md5((bet_id + str(idx)).encode()).hexdigest()[:6]}"

        if adapter == "ecos":
            task_data = build_ecos_task(
                task_id,
                extracted.get("title", f"Task {idx}"),
                task_type=extracted.get("task_type", "feature"),
                risk_level=extracted.get("risk_level", "L0"),
                source_docs=[str(source_file.absolute())],
                deliverables=extracted.get("deliverables", []),
                evidence_required=extracted.get("evidence_required", []),
                test_plan=extracted.get("test_plan", []),
                context_uri=f"bos://memory/sandbox/pitches/{source_file.name}",
                entry_gate=["BET_APPROVED"],
                imported_via="omo_bridge_pitch",
                extra={
                    "created_at": _utc_now(),
                    "updated_at": _utc_now(),
                    "metadata": {
                        "bet_id": bet_id,
                        "cognitive_cartridge": extracted.get("cognitive_cartridge", ""),
                        **({"scenario": scenario} if scenario else {}),
                    },
                },
            )
            if not _validate_ecos_task(task_data):
                print(f"  ❌ M2 防腐层拦截 (Schema Validation Failed for task {task_id})")
                continue
            _write_ecos_task(base_dir, task_id, task_data)
        else:
            task_data = build_local_task(
                task_id,
                extracted.get("title", f"Task {idx}"),
                description=extracted.get("description", "No description"),
                source_docs=[str(source_file.absolute())],
                context_uri=f"bos://memory/sandbox/pitches/{source_file.name}",
                imported_via="omo_bridge_pitch",
                extra={
                    "metadata": {
                        "task_type": extracted.get("task_type", "feature"),
                        "risk_level": extracted.get("risk_level", "L0"),
                        "deliverables": extracted.get("deliverables", []),
                        "evidence_required": extracted.get("evidence_required", []),
                        "test_plan": extracted.get("test_plan", []),
                        "wait_for_gate": ["BET_APPROVED"],
                        "bet_id": bet_id,
                        "cognitive_cartridge": extracted.get("cognitive_cartridge", ""),
                        **({"scenario": scenario} if scenario else {}),
                    },
                },
            )
            if not _save_local_task(base_dir, task_data, adapter):
                continue

        print(f"  ✅ 提取任务成功: {task_id} ({extracted.get('title', f'Task {idx}')})")
        tasks_created += 1

    print(f"✅ Bet 下注成功: 共创建了 {tasks_created} 个执行计划。")
