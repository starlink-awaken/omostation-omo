#!/usr/bin/env python3
"""omo.resident.interact — 常驻 Agent 统一交互引擎与能力中枢 (WP-Interact).

提供多模态、可嵌入的常驻 Agent 交互界面:
  - omo resident ask "<prompt>"      与常驻大脑单次问答 (支持 -d 异步派发)
  - omo resident chat                进入常驻 Agent 持续对话终端 (REPL 交互式)
  - omo resident tick [role|all]     推进事件驱动协同循环
  - omo resident distill             触发大模型经验卡片萃取
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import readline
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("omo.resident.interact")

from omo.omo_paths import code_root
from omo.resident import WORKSPACE, state_root, write_path

EVENTS_FILE = WORKSPACE / ".omo/_knowledge/workflow-mesh/events.jsonl"
SED_RUNS_DIR = WORKSPACE / ".omo/_knowledge/sediment/runs"
SED_FAIL_DIR = WORKSPACE / ".omo/_knowledge/sediment/failures"
PROPOSALS_DIR = WORKSPACE / ".omo/_knowledge/decision-proposals"
SIGNALS_DIR = Path.home() / ".codebuddy/personal-signals"
SKILLS_DIR = code_root() / ".agents/skills"
PID_FILE = WORKSPACE / ".omo/_delivery/resident-orchestrator/daemon.pid"

UV_BIN = "/opt/homebrew/bin/uv" if Path("/opt/homebrew/bin/uv").exists() else "uv"


MACMINI_TAILSCALE_IP = "100.64.110.118"


def _probe(port: int, host: str = "127.0.0.1") -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.8):
            return True
    except OSError:
        return False


def _resolve_aetherforge_host() -> tuple[str, str]:
    """探测 AetherForge 算力网关, 优先本机 127.0.0.1, 若本机离线则透明漫游至 MacMini 算力织网."""
    if _probe(4000, "127.0.0.1"):
        return ("127.0.0.1", "本机")
    if _probe(4000, MACMINI_TAILSCALE_IP):
        return (MACMINI_TAILSCALE_IP, "MacMini 算力织网")
    return ("127.0.0.1", "本机")


def _get_skills_summary() -> list[str]:
    """读取本仓可用 skills 摘要清单, 让常驻 Agent 具备完整的技能知晓能力."""
    if not SKILLS_DIR.exists():
        return []
    skills = []
    for item in sorted(SKILLS_DIR.iterdir()):
        if item.is_dir() and (item / "SKILL.md").exists():
            # 提取第一行或名字
            skills.append(item.name)
    return skills


def _get_context_summary() -> str:
    """提取系统当前的最新态势与运行快照，注入给常驻 Agent 思考."""
    parts = []
    try:
        from omo.resident.status import snapshot

        s = snapshot()
        comps = s.get("components", {})
        daemon = comps.get("daemon", {})
        events = comps.get("events", {})
        sed = comps.get("sediment", {})
        overall_health = s.get("health", "ok")
        parts.append(
            f"- 守护进程状态: health={overall_health}, detail={daemon.get('detail')}, tick_age={daemon.get('tick_age_seconds')}s\n"
            f"- 事件总线: 累计 {events.get('lines', 0)} 条事件 (idle {events.get('idle_seconds', 0)}s)\n"
            f"- 沉淀产物: 已处理 runs={sed.get('runs', 0)}, 失败={sed.get('failures', 0)}"
        )
    except Exception:
        pass

    p_dir = write_path(PROPOSALS_DIR)
    if p_dir.exists():
        proposals = sorted(p_dir.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
        if proposals:
            names = [p.name for p in proposals[:3]]
            parts.append(f"- 最新决策提案 ({len(proposals)}篇): {', '.join(names)}")

    return "\n".join(parts) if parts else "系统处于冷启动基线"


def _build_system_cognition_prompt() -> str:
    """构建常驻 Agent 的首席认知上下文，注水系统 Agent 阵容、可用工具、Skills 与架构边界."""
    skills = _get_skills_summary()
    skills_sample = ", ".join(skills[:25]) + f" ... (共 {len(skills)} 项技能)" if skills else "未挂载"

    return (
        "你是夏明星的个人主权操作系统 (eCOS) 常驻首席认知 Agent (Chief Cognitive Agent)。\n"
        "你常驻在系统内，拥有系统全域架构认知、多机算力拓扑与全工具调度能力。\n\n"
        "【双机硬件规格与定位】\n"
        "- MBP (128.0 GB 统一内存): 本地重推理主舰 (Heavy Compute Flagship)，承载 35B/70B 大模型 (LM Studio :1234 / AetherForge :4000)。\n"
        "- MacMini (24.0 GB 统一内存): 7×24h 哨兵与常驻基底 (Always-on Sentinel & Hub)，承载守护进程与常驻探活。\n\n"
        "【常驻 Agent 阵容与分工】\n"
        "- 首席认知 Agent (你): 全局态势研判、多 Agent 协调、与夏明星直接对话沟通。\n"
        "- HealthMonitorAgent: 24h 监控 system_health 快照与端口健康。\n"
        "- KnowledgeCuratorAgent: 消费事件流与决策产物，聚合跨场景经验到 MOS 信念库。\n"
        "- GovernorAgent: 审计系统偏离与高债务，分发 A2A 治理任务，发起演进提案。\n"
        "- AdvisorAgent: 评估系统行为与 TELOS 核心宪法原则的对齐度。\n"
        "- JourneyRunnerAgent: 推进受治理的长期旅程与 HITL 恢复。\n"
        "- Sediment & Decision: 自动沉淀 runs 知识并生成架构演进提案。\n\n"
        "【你可以自主调度的只读探针 (ReAct Tools)】\n"
        "当夏明星问询系统事实且你需要现场探查时，可以在回复中单独给出：\n"
        'Action: tool_name(param="value")\n'
        "可用探针：\n"
        "- Action: probe_system_health()                  # 获取系统健康度、守护进程水位与事件积压\n"
        "- Action: probe_active_alerts()                  # 检查是否有未解除的 P0/P1 治理拦截或端口故障\n"
        "- Action: probe_aictl_status()                   # 查询本地 AI 算力栈服务状态与在轨模型\n"
        "- Action: probe_hardware_compute()               # 探查双机(MBP 128G+Mini 24G)内存与大模型负载\n"
        "- Action: probe_agora_mesh()                     # 探查 Agora 蜂群网络中已注册的 Agent 阵容\n"
        "- Action: probe_bos_capabilities()               # 探查 BOS URI 注册的 42+ 服务能力\n"
        "- Action: probe_cockpit_commands()               # 探查 Cockpit 20+ 领域 CLI 工具集与用法建议\n"
        '- Action: dispatch_agora_task(objective="...", agent="codex") # 拆解派发任务给 Agora 专业 Agent 并拉起隔离 Worktree\n'
        '- Action: search_codebase(query="...", path=".") # 全局代码与文档快速检索\n'
        '- Action: read_codebase_file(path="...", start=1, end=50) # 读取指定代码或决策文件行范围\n'
        "- Action: read_decision_proposals(limit=3)       # 查看最新未决的系统演进提案\n\n"
        "【交互与执行准则】\n"
        "1. 表达专业、清晰、简洁，直击要害，使用中文回答。\n"
        "2. 若你需要事实支撑，请输出单个 Action: tool(...)。若信息已足够，直接输出最终答案，切勿伪造 Observation。\n"
        "3. 若涉及不可逆或破坏性操作，提示需要通过 Action Desk 署名批准。"
    )


def _probe_lm_studio_active_model(port: int = 1234, timeout: float = 0.8) -> str | None:
    """动态探测本地 LM Studio 当前实际驻留的活跃大模型 ID."""
    import urllib.request

    try:
        url = f"http://127.0.0.1:{port}/v1/models"
        req = urllib.request.Request(url, headers={"User-Agent": "omo-resident-probe"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read())
            models = data.get("data", [])
            if models and isinstance(models, list):
                return models[0].get("id")
    except Exception:
        pass
    return None


def resolve_compute_tier(
    prompt: str,
    context: str = "",
    requested_tier: str | None = None,
) -> tuple[str, str, int]:
    """根据任务意图、系统上下文与当前双机网络探活状态，自适应裁决最优算力通道.

    返回: (tier_description, model_name, target_port)
    三层动态分级:
      - fast: 本地常驻 AetherForge (0ms 首字, 适合即时研判、告警排查)
      - deep: MBP 128G 本地 LM Studio (动态识别已载模型, 适合架构审查、长上下文审计)
      - pro: 云端旗舰 Magpie (glm-5.3/qwen-max, 1M长文本与系统深度提炼)
    """
    req = (requested_tier or "").lower().strip()

    af_host, af_label = _resolve_aetherforge_host()
    fast_label = f"本地常驻 ({af_label} Fast Edge)" if af_label != "本机" else "本地常驻 (Fast Edge)"
    deep_label = f"本地深度 ({af_label} Deep Edge)" if af_label != "本机" else "本地深度 (Deep Edge)"

    active_lm_model = _probe_lm_studio_active_model(1234) if _probe(1234) else None
    mbp_label = f"MBP 重算力 ({active_lm_model or 'LM Studio'})"

    # 1. 显式覆盖优先
    if req in ("local", "fast"):
        return (fast_label, "fast", 4000)
    elif req in ("deep", "heavy", "mbp", "35b"):
        if active_lm_model:
            return (mbp_label, active_lm_model, 1234)
        elif _probe(1234):
            return (mbp_label, "swift-1.5-qwen3.8-27b-splash", 1234)
        return (deep_label, "deep", 4000)
    elif req in ("cloud-fast", "flash"):
        return ("云端轻量 (Cloud Fast)", "glm-4-flash", 3425)
    elif req in ("pro", "cloud-pro", "glm"):
        return ("云端旗舰 (Cloud Pro)", "glm-5.3", 3425)

    # 2. 场景模式自动识别 (Scenario-driven)
    # 重算力特征词: 架构审查、代码重构、复杂ReAct、长上下文走查
    heavy_triggers = ("架构审查", "复杂推理", "深度分析", "代码审计", "全景分析", "35b", "重模型")
    if any(t in prompt for t in heavy_triggers):
        if active_lm_model:
            return (mbp_label, active_lm_model, 1234)
        elif _probe(4000):
            return (deep_label, "deep", 4000)

    # 长文本、深度复盘、架构提案、全量事件归纳 → 升级至云端旗舰
    complex_triggers = ("萃取", "提炼", "复盘", "演进提案", "全面分析", "深入评估", "distill", "promote")
    if any(t in prompt for t in complex_triggers) or len(prompt) + len(context) > 1200:
        if _probe(3425):
            return ("云端旗舰 (Cloud Pro)", "glm-5.3", 3425)

    # 3. 默认优先策略: 本地/漫游 AetherForge 探活可用 → 本地优先 (Local-First Sovereignty)
    if _probe(4000, af_host):
        return (fast_label, "fast", 4000)

    # 本地不可用时回退到 Magpie 云端极速档
    if _probe(3425):
        return ("云端轻量 (Cloud Fast)", "glm-4-flash", 3425)

    # 终极保底
    return (fast_label, "fast", 4000)


def _get_aetherforge_key() -> str:
    key = os.environ.get("AETHERFORGE_API_KEY", "")
    if key:
        return key
    key_file = Path.home() / ".config/aetherforge/api_key"
    if key_file.exists():
        try:
            return key_file.read_text(encoding="utf-8").strip()
        except OSError:
            pass
    try:
        out = subprocess.run(
            ["security", "find-generic-password", "-s", "aetherforge-gateway", "-w"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        return out.stdout.strip() if out.returncode == 0 else ""
    except Exception:
        return ""


def _invoke_llm_endpoint(
    port: int,
    model: str,
    messages: list[dict[str, str]],
    timeout: int = 35,
    stream: bool = False,
    host: str | None = None,
) -> str | None:
    import urllib.request

    try:
        target_host = host or (_resolve_aetherforge_host()[0] if port == 4000 else "127.0.0.1")
        url = f"http://{target_host}:{port}/v1/chat/completions"
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": 0.3,
            "stream": stream,
        }
        headers = {"Content-Type": "application/json"}
        if port == 4000:
            key = _get_aetherforge_key()
            if key:
                headers["Authorization"] = f"Bearer {key}"
            payload["max_tokens"] = 1024
        elif port == 1234:
            payload["max_tokens"] = 2048

        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST")

        if not stream:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = json.loads(r.read())
                msg = data["choices"][0]["message"]
                # 兼容普通 content 与 reasoning_content
                return msg.get("content") or msg.get("reasoning_content") or ""
        else:
            collected = []
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                for line in resp:
                    line_str = line.decode("utf-8").strip()
                    if line_str.startswith("data: "):
                        data_str = line_str[6:].strip()
                        if data_str == "[DONE]":
                            break
                        try:
                            chunk = json.loads(data_str)
                            delta = chunk["choices"][0].get("delta", {})
                            token = delta.get("content") or delta.get("reasoning_content") or ""
                            if token:
                                sys.stdout.write(token)
                                sys.stdout.flush()
                                collected.append(token)
                        except Exception:
                            pass
            return "".join(collected) if collected else None
    except Exception:
        return None


def execute_react_tool(action_name: str, arg_str: str) -> str:
    """受控沙箱执行自主只读探针."""
    kwargs, args = {}, []
    if arg_str.strip():
        try:
            import ast

            tree = ast.parse(f"fn({arg_str})")
            call_node = tree.body[0].value
            args = [ast.literal_eval(a) for a in call_node.args]
            kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in call_node.keywords}
        except Exception:
            args = [arg_str.strip("'\" ")]

    if action_name == "probe_system_health":
        try:
            from omo.resident.status import snapshot

            s = snapshot()
            comps = s.get("components", {})
            daemon = comps.get("daemon", {})
            events = comps.get("events", {})
            sed = comps.get("sediment", {})
            return json.dumps(
                {
                    "health": s.get("health", "ok"),
                    "daemon": {
                        "ok": daemon.get("ok"),
                        "tick_age_s": daemon.get("tick_age_seconds"),
                        "detail": daemon.get("detail"),
                    },
                    "events": {"total": events.get("lines"), "idle_s": events.get("idle_seconds")},
                    "sediment": {"runs": sed.get("runs"), "failures": sed.get("failures")},
                },
                ensure_ascii=False,
            )
        except Exception as exc:
            return f"error: {exc}"

    elif action_name == "probe_active_alerts":
        try:
            from omo.resident.escalate import check_and_escalate

            report = check_and_escalate(dry_run=True)
            return json.dumps(
                {
                    "active_count": report.get("active_count"),
                    "alerts": report.get("active_alerts"),
                },
                ensure_ascii=False,
            )
        except Exception as exc:
            return f"error: {exc}"

    elif action_name == "probe_aictl_status":
        try:
            res = subprocess.run(["aictl", "status"], capture_output=True, text=True, timeout=4)
            return res.stdout.strip()[:1000] if res.returncode == 0 else f"error: {res.stderr}"
        except Exception as exc:
            return f"error: {exc}"

    elif action_name == "search_codebase":
        query = kwargs.get("query") or (args[0] if args else "")
        path = kwargs.get("path") or (args[1] if len(args) > 1 else ".")
        if not query:
            return "error: missing query"
        clean_path = (WORKSPACE / path).resolve()
        if not str(clean_path).startswith(str(WORKSPACE)):
            return "error: path outside workspace"
        try:
            res = subprocess.run(
                ["rg", "-n", "-m", "5", "--max-columns", "160", str(query), str(clean_path)],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            return res.stdout.strip()[:1500] if res.stdout.strip() else "未找到匹配内容"
        except Exception as exc:
            return f"error: {exc}"

    elif action_name == "read_codebase_file":
        path = kwargs.get("path") or (args[0] if args else "")
        start = int(kwargs.get("start") or (args[1] if len(args) > 1 else 1))
        end = int(kwargs.get("end") or (args[2] if len(args) > 2 else 50))
        if not path:
            return "error: missing path"
        target = (WORKSPACE / path).resolve()
        if not str(target).startswith(str(WORKSPACE)):
            return "error: path outside workspace"
        if not target.is_file():
            return f"error: file not found: {path}"
        try:
            lines = target.read_text(encoding="utf-8", errors="replace").splitlines()
            selected = lines[max(0, start - 1) : end]
            return "\n".join(f"{i + start}: {line_text}" for i, line_text in enumerate(selected))[:2000]
        except Exception as exc:
            return f"error: {exc}"

    elif action_name == "read_decision_proposals":
        limit = int(kwargs.get("limit") or (args[0] if args else 3))
        p_dir = write_path(PROPOSALS_DIR)
        if not p_dir.exists():
            return "暂无提案目录"
        try:
            files = sorted(p_dir.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]
            items = []
            for f in files:
                lines = f.read_text(encoding="utf-8", errors="replace").splitlines()[:10]
                items.append(f"【{f.name}】:\n" + "\n".join(lines))
            return "\n\n".join(items) if items else "暂无提案"
        except Exception as exc:
            return f"error: {exc}"

    elif action_name == "probe_hardware_compute":
        try:
            from omo.resident.swarm_mesh import get_swarm_mesh

            mesh = get_swarm_mesh()
            res = mesh.probe_mesh()
            return json.dumps(res, ensure_ascii=False)
        except Exception as exc:
            return f"error: {exc}"

    elif action_name == "probe_agora_mesh":
        try:
            prov_file = WORKSPACE / ".omo/_truth/registry/capability-providers.yaml"
            if prov_file.exists():
                import yaml

                data = yaml.safe_load(prov_file.read_text(encoding="utf-8"))
                providers = data.get("providers", [])
                mesh_agents = []
                for p in providers:
                    mesh_agents.append(
                        {
                            "id": p.get("id"),
                            "capabilities": p.get("capabilities", []),
                            "strengths": p.get("strengths", []),
                        }
                    )
                return json.dumps({"agora_mesh_agents": mesh_agents}, ensure_ascii=False)
            baseline_agents = [
                {"id": "claude-code", "capabilities": ["code", "architecture"]},
                {"id": "codex", "capabilities": ["code", "review"]},
            ]
            return json.dumps({"agora_mesh_agents": baseline_agents}, ensure_ascii=False)
        except Exception as exc:
            return f"error: {exc}"

    elif action_name == "probe_bos_capabilities":
        try:
            bos_uris = [
                "bos://compute/aetherforge/infer",
                "bos://memory/mos/recall",
                "bos://governance/gac/audit",
                "bos://agora/a2a/route",
                "bos://observability/telemetry/stream",
            ]
            return json.dumps(
                {
                    "bos_uri_count": len(bos_uris),
                    "core_services": bos_uris,
                    "protocol": "BOS/v2 (Binding Ontology Service)",
                },
                ensure_ascii=False,
            )
        except Exception as exc:
            return f"error: {exc}"

    elif action_name == "probe_cockpit_commands":
        cmds = {
            "resident": "常驻 Agent 状态研判、问答与决策提案 (cockpit resident ask / chat / status / decision)",
            "health": "全域健康雷达评估与 7 域检查 (cockpit health)",
            "gate": "GaC 治理门禁与本地预检 (cockpit gate)",
            "workflow": "ADR-0203 工作流闭环管理 (cockpit workflow start / claim / closeout)",
            "board": "B.D.S.K. 虚拟董事会审议 (cockpit board)",
            "memory": "MOS 记忆召回与知识检索 (cockpit memory query)",
            "compute": "本地大模型推理与显存水位评估 (cockpit compute)",
        }
        return json.dumps({"cockpit_commands": cmds}, ensure_ascii=False)

    elif action_name == "dispatch_agora_task":
        obj = kwargs.get("objective") or (args[0] if args else "")
        agent = kwargs.get("agent") or (args[1] if len(args) > 1 else None)
        if not obj:
            return "error: missing objective"
        try:
            from omo.resident.dispatch import dispatch_task

            res = dispatch_task(objective=obj, target_agent=agent, auto_worktree=True, dry_run=False)
            return json.dumps(
                {
                    "status": "dispatched",
                    "task_id": res["task_id"],
                    "target_agent": res["target_agent"],
                    "session": res["session_id"],
                    "worktree": res["worktree"]["message"],
                    "action_desk_card": "registered_pending_approval",
                },
                ensure_ascii=False,
            )
        except Exception as exc:
            return f"error: {exc}"

    return f"error: unknown tool '{action_name}'"


def _distill_session_epiphany(history: list[dict[str, str]]) -> None:
    """会话结束时，若存在多轮交互，调用轻量模型萃取 1 条候选经验或偏好卡片."""
    if len(history) < 2:
        return

    turns = []
    for m in history[-6:]:
        role = "夏明星" if m.get("role") == "user" else "常驻Agent"
        turns.append(f"{role}: {m.get('content', '')[:200]}")
    dialog_text = "\n".join(turns)

    prompt = (
        "你是一位敏锐的主权 AI 架构师。请从以下夏明星与常驻 Agent 的最新对话中，提取出夏明星表达的 1 条最关键的工程偏好、设计纠正或架构共识。\n"
        '若对话中纯属普通闲聊问答、没有体现任何设计偏好或纠错，则输出: {"has_belief": false}\n'
        "若有明确偏好或教训，输出纯 JSON:\n"
        "{\n"
        '  "has_belief": true,\n'
        '  "title": "简明标题(20字以内)",\n'
        '  "category": "user_preference",\n'
        '  "summary": "一句话核心结论(30字以内)",\n'
        '  "details": "具体偏好与要求(80字以内)"\n'
        "}\n\n"
        f"对话片段:\n{dialog_text}"
    )

    try:
        raw = _invoke_llm_endpoint(4000, "fast", [{"role": "user", "content": prompt}], timeout=15, stream=False)
        if not raw:
            return
        m = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            return
        data = json.loads(m.group(0))
        if not data.get("has_belief"):
            return

        cards_dir = Path.home() / ".local/share/omostation/cards"
        cards_dir.mkdir(parents=True, exist_ok=True)
        now_ts = int(time.time())
        card_id = f"session-epiphany-{now_ts}"
        card_file = cards_dir / f"{card_id}.json"

        card_data = {
            "id": card_id,
            "type": "memory",
            "title": data.get("title", "会话萃取偏好"),
            "category": data.get("category", "user_preference"),
            "summary": data.get("summary", ""),
            "details": data.get("details", ""),
            "status": "pending_approval",
            "created_at": datetime.now(UTC).isoformat(),
            "created_by": "session-epiphany",
        }
        card_file.write_text(json.dumps(card_data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n\033[32m💡 已从会话中萃取 1 项经验偏好卡片: {data.get('title')}\033[0m")
        print("\033[2m   (已送入 SwiftBar Action Desk，待夏明星一键签署沉淀)\033[0m")
    except Exception:
        pass


def _call_llm(
    prompt: str,
    context: str = "",
    tier: str | None = None,
    model_override: str | None = None,
    messages_history: list[dict[str, str]] | None = None,
    stream: bool = False,
    enable_react: bool = True,
) -> str:
    """经由智能分层算力网关执行推理 (支持 ReAct 只读探针闭环、本地 AetherForge 与云端 Magpie 动态分派、自愈降级与流式输出)."""
    sys_prompt = _build_system_cognition_prompt()
    if context:
        sys_prompt += f"\n\n【系统实时运行快照】\n{context}"

    tier_desc, model_name, target_port = resolve_compute_tier(prompt, context, requested_tier=tier)
    if model_override:
        model_name = model_override

    # 构建完整多轮 messages
    if messages_history:
        messages = (
            [{"role": "system", "content": sys_prompt}] + messages_history + [{"role": "user", "content": prompt}]
        )
    else:
        messages = [{"role": "system", "content": sys_prompt}, {"role": "user", "content": prompt}]

    t0 = time.time()
    call_timeout = 60 if target_port == 3425 else 30

    max_turns = 3 if enable_react else 1
    last_content = ""
    used_tier = tier_desc
    used_model = model_name

    for loop_i in range(max_turns):
        content = _invoke_llm_endpoint(target_port, model_name, messages, timeout=call_timeout, stream=False)

        # 失败自愈降级链路 (Fallback Chain)
        if not content:
            if stream and loop_i == 0:
                print("\n\033[33m[首选网关超时/离线，正在透明切换至对端自愈链路...]\033[0m")
            fallback_port = 3425 if target_port == 4000 else 4000
            fallback_model = "glm-5.3" if fallback_port == 3425 else "fast"
            fb_timeout = 60 if fallback_port == 3425 else 30
            content = _invoke_llm_endpoint(fallback_port, fallback_model, messages, timeout=fb_timeout, stream=False)
            if content:
                used_tier = "自愈降级 · " + ("云端旗舰" if fallback_port == 3425 else "本地常驻")
                used_model = fallback_model
                target_port = fallback_port
                model_name = fallback_model

        if not content:
            return "（本地 AetherForge 与云端推理网关均未响应，请检查端口活性）"

        last_content = content

        # 检查是否包含 Action: tool(...)
        m = re.search(r"Action:\s*([a-zA-Z0-9_]+)\((.*?)\)", content, re.DOTALL)
        if enable_react and m:
            tool_name = m.group(1).strip()
            tool_args = m.group(2).strip()
            if stream:
                print(f"\033[36m🔍 [自主调度只读探针] {tool_name}({tool_args})\033[0m")
            obs = execute_react_tool(tool_name, tool_args)
            messages.append({"role": "assistant", "content": content})
            messages.append({"role": "user", "content": f"Observation: {obs}\n请综合现场真实事实给出最终结论。"})
            continue
        else:
            break

    cost_s = time.time() - t0
    badge = f"\n\n\033[2m[⚡️ 算力链路: {used_tier} · 模型 {used_model} · 耗时 {cost_s:.2f}s]\033[0m"
    if stream:
        print(last_content)
        print(badge)
        return last_content or ""

    if last_content:
        return last_content.strip() + badge

    return "（未生成有效内容）"


def ask(prompt: str, tier: str | None = None, history: list[dict[str, str]] | None = None) -> str:
    """公用 API 入口，供 live_server 或其它模块直接调用 (支持 ReAct 只读探针)."""
    ctx = _get_context_summary()
    return _call_llm(prompt, context=ctx, tier=tier, messages_history=history, stream=False, enable_react=True)


def _get_clipboard_text() -> str:
    """读取 macOS 系统剪贴板内容."""
    try:
        return subprocess.run(["pbpaste"], capture_output=True, text=True, timeout=2).stdout.strip()
    except Exception:
        return ""


def cmd_ask(prompt: str, dispatch: bool = False, tier: str | None = None, with_clip: bool = False) -> int:
    if not prompt and not with_clip:
        print('用法: omo resident ask "你的指令或问题" [-d] [--tier fast|pro] [--clip]')
        return 1

    clip_text = _get_clipboard_text() if with_clip else ""
    full_prompt = prompt
    if clip_text:
        full_prompt += f"\n\n【附加剪贴板内容】:\n{clip_text}"

    if dispatch:
        SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
        now_str = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        sig_file = SIGNALS_DIR / f"signal-ask-{now_str}.md"
        content = f"# 交互派发信号 — {now_str}\n\n- 发起者: 夏明星 (CLI/Ask)\n- 内容: {full_prompt}\n- 意图: 触发常驻 Agent 异步协同\n"
        sig_file.write_text(content, encoding="utf-8")
        print(f"📝 已创建信号文件: {sig_file.name}")
        subprocess.run([str(WORKSPACE / "lib/resident-role-tick.sh"), "signals"], cwd=WORKSPACE, capture_output=True)
        print("🚀 指令已发布至常驻事件总线，等待常驻 Agent 消费！")
        return 0

    print(f'🤖 常驻首席认知 Agent 研判中: "{prompt[:60]}" ...\n')
    ctx = _get_context_summary()
    _call_llm(full_prompt, context=ctx, tier=tier, stream=True)
    return 0


def cmd_tick(role: str = "all") -> int:
    """触发常驻 Agent 单次轮转 (tick). 支持 role: all, sediment, decision, execute, monitor, heartbeat, signals, inbox, promote."""
    from omo.resident.daemon import main as daemon_main

    target = (role or "all").strip().lower()
    if target in ("signals", "inbox", "promote"):
        import importlib

        mod = importlib.import_module(f"omo.resident.{target}")
        print(f"⚡️ 触发常驻直调角色: {target}")
        return mod.main([])  # type: ignore[attr-defined]

    if target in ("sediment", "decision", "execute", "monitor", "heartbeat"):
        print(f"⚡️ 触发常驻 tick 角色: {target}")
        return daemon_main(["--once", "--role", target])

    # 默认触发全局守护轮转 (tick_once)
    print("⚡️ 触发常驻全量守护轮转 (tick_once)...")
    return daemon_main(["--once"])


def _distill_session_epiphany(history: list[dict[str, str]]) -> None:
    """会话结束时，自动提炼会话中的偏好与纠错并送入 Action Desk."""
    if not history:
        return
    text = "\n".join(f"{m.get('role', 'user')}: {m.get('content', '')}" for m in history)
    try:
        from omo.resident.epiphany import distill_epiphany_from_text

        card = distill_epiphany_from_text(text)
        if card:
            print(f"\n💡 [自进化] 已根据会话纠错自动生成记忆候选卡片: {card['title']}")
            print("   已推入 SwiftBar Action Desk，点击 Approve 可合流进 LifeOS 权威库。")
    except Exception:
        pass


def cmd_distill(history: list[dict[str, str]] | None = None) -> int:
    """触发经验与偏好卡片萃取 (Distillation)."""
    if history and len(history) >= 2:
        print("💡 正在从当前会话历史中萃取架构偏好与经验卡片...")
        _distill_session_epiphany(history)
        return 0
    print("💡 提示: 会话萃取需要至少 2 轮有效对话。在 REPL 对话中交流后输入 /distill 或退出即可沉淀偏好。")
    return 0


def cmd_chat() -> int:
    """进入常驻 Agent 持续对话终端 (REPL 交互式 + 流式输出 + 会话记忆)."""
    print("=" * 64)
    print("💬 eCOS 常驻首席认知 Agent 交互终端 (REPL · Stream)")
    print("已激活: 实时流式打字机 + 多轮会话记忆 + 剪贴板注入 + 智能分层算力")
    print("输入 /help 查看内置快捷指令，输入 /exit 退出。")
    print("=" * 64 + "\n")

    history: list[dict[str, str]] = []
    current_tier: str | None = None

    while True:
        try:
            tier_hint = f" ({current_tier})" if current_tier else ""
            line = input(f"\033[1;36meCOS-Resident{tier_hint} ❯\033[0m ").strip()
        except (KeyboardInterrupt, EOFError):
            _distill_session_epiphany(history)
            print("\n已退出会话。")
            break

        if not line:
            continue

        if line in ("/exit", "/quit", "exit", "quit"):
            _distill_session_epiphany(history)
            print("再见！常驻守护进程继续在后台守护系统。")
            break
        elif line == "/help":
            print("\n[内置快捷指令]")
            print("  /status      查看常驻系统全景运行状态")
            print("  /tier <name> 切换算力档位 (fast/deep/pro/auto)")
            print("  /clip [q]    自动抓取系统剪贴板内容并向大脑提问")
            print("  /run <cmd>   安全运行只读命令 (如 /run git status, /run gh pr list)")
            print("  /clear       清空多轮会话记忆")
            print("  /skills      列出已知可调度的 186 项 Skills 清单")
            print("  /events      查看最近事件总线流")
            print("  /tick        推进一次全角色协同循环")
            print("  /distill     触发大模型夜间知识卡片萃取")
            print("  /exit        退出交互终端\n")
            continue
        elif line.startswith("/tier"):
            parts = line.split()
            if len(parts) > 1 and parts[1] != "auto":
                current_tier = parts[1]
                print(f"✅ 算力档位已锁定为: {current_tier}\n")
            else:
                current_tier = None
                print("✅ 算力档位已恢复为: 智能动态裁决 (Auto)\n")
            continue
        elif line == "/clear":
            history.clear()
            print("🧹 会话历史记忆已重置。\n")
            continue
        elif line.startswith("/clip"):
            extra_q = line[5:].strip() or "请分析以下剪贴板内容"
            clip_content = _get_clipboard_text()
            if not clip_content:
                print("⚠️ 剪贴板为空\n")
                continue
            line = f"{extra_q}:\n```\n{clip_content}\n```"
            print(f"📋 已附加剪贴板内容 ({len(clip_content)} 字符)")
        elif line.startswith("/run "):
            sub_cmd = line[5:].strip()
            # 安全防护: 仅允许只读探测命令
            safe_prefixes = (
                "git status",
                "git diff",
                "git log",
                "gh pr",
                "omo",
                "cockpit",
                "make resident",
                "tail",
                "ls",
                "cat",
                "ps",
            )
            if not any(sub_cmd.startswith(p) for p in safe_prefixes):
                print(f"⚠️ 出于主权安全防护，仅允许执行受管只读命令: {', '.join(safe_prefixes[:6])} 等\n")
                continue
            print(f"⚙️ 执行命令: {sub_cmd} ...")
            c_res = subprocess.run(sub_cmd, shell=True, cwd=WORKSPACE, capture_output=True, text=True)
            c_out = (c_res.stdout or c_res.stderr).strip()
            print(f"\n{c_out[:1200]}\n" + ("\n...(输出截断)" if len(c_out) > 1200 else ""))
            continue
        elif line == "/status":
            from omo.resident import status as _st

            report = _st.status_report()
            print("\n" + json.dumps(report, ensure_ascii=False, indent=2) + "\n")
            continue
        elif line == "/skills":
            skills = _get_skills_summary()
            print(f"\n[已知 Skills 清单 ({len(skills)} 项)]")
            for i in range(0, len(skills), 4):
                print("  " + "  ".join(f"{s:<22}" for s in skills[i : i + 4]))
            print()
            continue
        elif line == "/events":
            ctx = _get_context_summary()
            print(f"\n{ctx}\n")
            continue
        elif line == "/tick":
            cmd_tick("all")
            continue
        elif line == "/distill":
            cmd_distill(history)
            continue

        # 自然语言流式交互 (带多轮历史与上下文)
        print("\n\033[1;32m[首席认知 Agent]\033[0m")
        ctx = _get_context_summary()
        reply = _call_llm(
            line,
            context=ctx,
            tier=current_tier,
            messages_history=history[-6:],  # 保留最近 3 轮完整上下文
            stream=True,
        )
        if reply:
            history.append({"role": "user", "content": line})
            history.append({"role": "assistant", "content": reply})
            if len(history) > 12:
                history = history[-12:]
        print()


def triage_component_failure(
    component: str,
    status: str,
    error_log: str,
    auto_dispatch: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """知行全景大屏异常就地诊断与自愈待办生成 (Phase 5 Sprint 3)."""
    comp_lower = component.lower()
    err_lower = error_log.lower()

    # 1. 动态风险定级 (Q5 裁定)
    is_ops_issue = any(
        k in comp_lower for k in ("dashboard", "projection", "lease", "daemon", "service", "process", "cron", "log")
    ) or any(k in err_lower for k in ("expired", "stale", "timeout", "zombie", "restart", "503"))
    risk_tier = "R0" if is_ops_issue else "R1"

    # 2. 根因与修复建议推导 (本地模型优先)
    triage_prompt = (
        f"系统监控节点报告异常:\n"
        f"- 组件: {component}\n"
        f"- 状态: {status}\n"
        f"- 现场日志: {error_log[:300]}\n"
        f"请用简体中文给出 80 字以内核心根因分析与 1 条精准处置建议。"
    )
    try:
        diag_text = ask(triage_prompt, tier="local", max_tokens=150)
        clean_diag = diag_text.strip().replace("\n\n", " ")
        root_cause = clean_diag[:120]
    except Exception:
        if is_ops_issue:
            root_cause = f"根因初判: {component} 运行态状态漂移或租约超时 ({error_log[:60]})"
        else:
            root_cause = f"根因初判: {component} 代码逻辑或单测断言失败 ({error_log[:60]})"

    suggested_action = (
        f"自动刷新 {component} 状态并延期租约" if is_ops_issue else f"调动 Worker 在隔离工作树修复 {component}"
    )

    result: dict[str, Any] = {
        "ok": True,
        "component": component,
        "status": status,
        "risk_tier": risk_tier,
        "root_cause": root_cause,
        "suggested_action": suggested_action,
        "dispatched": False,
        "task_id": None,
        "session_id": None,
        "action_card_id": None,
    }

    # 3. 自动建单与 Action Desk 待办挂载 (Q2 裁定)
    if auto_dispatch:
        try:
            from omo.resident.dispatch import dispatch_task

            obj = f"自愈修复: {component} · {root_cause[:30]}"
            target_agent = "crush" if is_ops_issue else "codex"
            disp_res = dispatch_task(
                objective=obj,
                target_agent=target_agent,
                risk_tier=risk_tier,
                auto_worktree=True,
                dry_run=dry_run,
            )
            result["dispatched"] = disp_res.get("ok", False)
            result["task_id"] = disp_res.get("task_id")
            result["session_id"] = disp_res.get("session_id")
            if disp_res.get("session_id"):
                result["action_card_id"] = f"task-dispatch-{disp_res.get('session_id')}"
        except Exception as exc:
            logger.warning("Auto dispatch failed during triage: %s", exc)

    return result


def cmd_web(port: int = 43195, host: str = "0.0.0.0") -> int:  # noqa: S104
    """启动常驻 Copilot 轻量 Web 交互控制台 (支持跨机 iPad/手机/Mac 访问与 ReAct 可视化)."""
    import http.server
    import urllib.parse

    html_template = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>Chief Resident Copilot · 常驻首席认知智能体</title>
  <style>
    :root {
      --bg: #090d16;
      --card-bg: rgba(22, 30, 46, 0.75);
      --card-border: rgba(255, 255, 255, 0.08);
      --primary: #38bdf8;
      --primary-glow: rgba(56, 189, 248, 0.25);
      --accent: #10b981;
      --text: #f1f5f9;
      --text-muted: #94a3b8;
      --user-msg-bg: #1e293b;
      --agent-msg-bg: #172033;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: radial-gradient(circle at top right, #1e1b4b, #090d16 70%);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }
    header {
      padding: 16px 24px;
      background: rgba(9, 13, 22, 0.85);
      backdrop-filter: blur(16px);
      border-bottom: 1px solid var(--card-border);
      display: flex;
      justify-content: space-between;
      align-items: center;
      position: sticky;
      top: 0;
      z-index: 10;
    }
    .brand { display: flex; align-items: center; gap: 12px; }
    .brand-icon {
      width: 36px; height: 36px; border-radius: 10px;
      background: linear-gradient(135deg, #0284c7, #38bdf8);
      display: flex; align-items: center; justify-content: center;
      font-size: 20px; box-shadow: 0 0 16px var(--primary-glow);
    }
    .brand-title { font-size: 17px; font-weight: 700; letter-spacing: -0.3px; }
    .brand-sub { font-size: 12px; color: var(--text-muted); }
    .status-hud { display: flex; gap: 16px; font-size: 12px; align-items: center; }
    .hud-badge {
      display: flex; align-items: center; gap: 6px;
      background: var(--card-bg); padding: 5px 12px; border-radius: 999px;
      border: 1px solid var(--card-border);
    }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--accent); box-shadow: 0 0 8px var(--accent); }
    main {
      flex: 1; max-width: 900px; width: 100%; margin: 0 auto;
      padding: 20px; display: flex; flex-direction: column; gap: 16px;
    }
    .chips-bar { display: flex; gap: 8px; flex-wrap: wrap; }
    .chip {
      background: var(--card-bg); border: 1px solid var(--card-border);
      padding: 7px 14px; border-radius: 8px; font-size: 13px;
      cursor: pointer; transition: all 0.2s ease; user-select: none;
    }
    .chip:hover {
      background: rgba(56, 189, 248, 0.15); border-color: var(--primary);
      transform: translateY(-1px);
    }
    .tier-bar {
      display: flex; gap: 8px; align-items: center; font-size: 13px;
      padding: 8px 14px; background: rgba(15, 23, 42, 0.6);
      border-radius: 8px; border: 1px solid var(--card-border);
    }
    .tier-pill {
      padding: 4px 10px; border-radius: 6px; cursor: pointer;
      font-size: 12px; color: var(--text-muted); border: 1px solid transparent;
    }
    .tier-pill.active {
      background: var(--primary); color: #090d16; font-weight: 600;
    }
    .chat-container {
      flex: 1; display: flex; flex-direction: column; gap: 16px;
      overflow-y: auto; padding-bottom: 20px;
    }
    .message {
      max-width: 88%; padding: 14px 18px; border-radius: 14px;
      line-height: 1.6; font-size: 15px; word-break: break-word;
      box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    .message.user {
      align-self: flex-end; background: var(--user-msg-bg);
      border: 1px solid rgba(255,255,255,0.1); border-bottom-right-radius: 4px;
    }
    .message.agent {
      align-self: flex-start; background: var(--agent-msg-bg);
      border: 1px solid var(--card-border); border-bottom-left-radius: 4px;
    }
    .message pre {
      background: #0b1120; padding: 12px; border-radius: 8px;
      overflow-x: auto; font-family: monospace; font-size: 13px;
      margin: 8px 0; border: 1px solid rgba(255,255,255,0.05);
    }
    details.react-step {
      background: rgba(0, 0, 0, 0.25); border: 1px solid rgba(56, 189, 248, 0.2);
      border-radius: 8px; padding: 8px 12px; margin: 8px 0; font-size: 13px;
    }
    details.react-step summary { cursor: pointer; color: var(--primary); font-weight: 500; }
    .input-box-wrapper {
      position: sticky; bottom: 16px;
      background: rgba(9, 13, 22, 0.9); backdrop-filter: blur(16px);
      border: 1px solid var(--card-border); border-radius: 14px;
      padding: 10px 14px; display: flex; gap: 10px; align-items: flex-end;
      box-shadow: 0 10px 30px rgba(0,0,0,0.5);
    }
    textarea {
      flex: 1; background: transparent; border: none; outline: none;
      color: var(--text); font-size: 15px; resize: none; max-height: 140px;
      line-height: 1.5; font-family: inherit;
    }
    button.send-btn {
      background: var(--primary); color: #090d16; border: none;
      padding: 8px 18px; border-radius: 8px; font-weight: 600;
      cursor: pointer; transition: opacity 0.2s;
    }
    button.send-btn:hover { opacity: 0.9; }
    button.send-btn:disabled { opacity: 0.4; cursor: not-allowed; }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <div class="brand-icon">⚡</div>
      <div>
        <div class="brand-title">Chief Resident Copilot</div>
        <div class="brand-sub">eCOS 常驻首席认知智能体 · 主权神经中枢</div>
      </div>
    </div>
    <div class="status-hud">
      <div class="hud-badge"><span class="dot"></span> MBP 128G 算力主舰</div>
      <div class="hud-badge"><span class="dot"></span> MacMini 7×24 哨兵</div>
      <div class="hud-badge" id="patrolStatusBadge"><span class="dot"></span> 巡检待命</div>
      <a href="http://127.0.0.1:43191" target="_blank" style="color:var(--text-muted);text-decoration:none;font-size:12px;display:flex;align-items:center;gap:4px;" class="hud-badge">📊 知行全景驾驶舱 ↗</a>
    </div>
  </header>

  <main>
    <div class="tier-bar">
      <span>算力档位:</span>
      <div class="tier-pill active" onclick="setTier('auto')">Auto 智能裁决</div>
      <div class="tier-pill" onclick="setTier('fast')">本地 Fast (0ms)</div>
      <div class="tier-pill" onclick="setTier('heavy')">MBP 重算力 (35B/128G)</div>
      <div class="tier-pill" onclick="setTier('pro')">云端 Pro (GLM-5.3)</div>
    </div>

    <div class="chips-bar">
      <div class="chip" onclick="quickSend('请自主探查当前系统健康度与守护进程水位')">⚡ 系统健康探查</div>
      <div class="chip" onclick="quickSend('请扫描当前系统未解除的活跃告警与风险')">🛡️ 活跃告警扫描</div>
      <div class="chip" onclick="quickSend('请探查当前双机硬件显存与大模型负载')">🖥️ 双机算力矩阵</div>
      <div class="chip" onclick="quickSend('请探查当前 Agora 蜂群已注册的 Agent 阵容')">🤖 Agora 蜂群拓扑</div>
      <div class="chip" onclick="quickSend('请探查当前 BOS URI 核心服务契约')">🌐 BOS 能力清单</div>
      <div class="chip" onclick="quickSend('请读取最近未决的系统演进提案')">📜 未决架构提案</div>
      <div class="chip" onclick="quickSend('请指派 codex 检查并在隔离工作树中修复单测')">🚀 派发任务至 Codex</div>
      <div class="chip" onclick="quickSend('请强制执行一次 MBP 128G 夜间深度巡检')">🌅 触发夜间巡检</div>
    </div>

    <div class="chat-container" id="chat">
      <div class="message agent">
        👋 夏明星你好！我是你的常驻首席认知 Agent。我与 MacMini 哨兵及 MBP 128GB 重算力主舰保持实时神经互联，已挂载 ReAct 自主探针与全域拓扑认知。<br><br>
        你可以向我咨询全域架构状态、审计工程偏离、指派协同任务，或点击上方快捷芯片现场探查。
      </div>
    </div>

    <div class="input-box-wrapper">
      <textarea id="promptInput" rows="1" placeholder="输入自然语言问题或任务指令... (Enter 发送，Shift+Enter 换行)"></textarea>
      <button class="send-btn" id="sendBtn" onclick="sendMsg()">发送</button>
    </div>
  </main>

  <script>
    let currentTier = 'auto';
    const history = [];

    function setTier(tier) {
      currentTier = tier;
      document.querySelectorAll('.tier-pill').forEach(el => el.classList.remove('active'));
      event.target.classList.add('active');
    }

    function quickSend(text) {
      document.getElementById('promptInput').value = text;
      sendMsg();
    }

    const input = document.getElementById('promptInput');
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendMsg();
      }
    });

    async function sendMsg() {
      const text = input.value.trim();
      if (!text) return;
      input.value = '';
      input.style.height = 'auto';

      appendMsg('user', text);
      const sendBtn = document.getElementById('sendBtn');
      sendBtn.disabled = true;

      const agentMsgDiv = appendMsg('agent', '<span style="color:var(--text-muted)">正在自主研判与探查中...</span>');

      try {
        const res = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            prompt: text,
            tier: currentTier,
            history: history.slice(-6)
          })
        });
        const data = await res.json();
        if (data.ok) {
          agentMsgDiv.innerHTML = formatReply(data.reply);
          history.push({ role: 'user', content: text });
          history.push({ role: 'assistant', content: data.reply });
        } else {
          agentMsgDiv.innerHTML = '<span style="color:#ef4444">❌ 错误: ' + (data.error || '请求失败') + '</span>';
        }
      } catch (err) {
        agentMsgDiv.innerHTML = '<span style="color:#ef4444">❌ 网络连接异常: ' + err.message + '</span>';
      } finally {
        sendBtn.disabled = false;
        scrollToBottom();
      }
    }

    function appendMsg(role, contentHtml) {
      const chat = document.getElementById('chat');
      const div = document.createElement('div');
      div.className = 'message ' + role;
      div.innerHTML = contentHtml;
      chat.appendChild(div);
      scrollToBottom();
      return div;
    }

    function formatReply(text) {
      let html = text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      html = html.replace(/\\`\\`\\`([a-zA-Z]*)\n([\\s\\S]*?)\\`\\`\\`/g, '<pre><code>$2</code></pre>');
      html = html.replace(/\\`([^`]+)\\`/g, '<code>$1</code>');
      html = html.replace(/\n/g, '<br>');
      return html;
    }

    function scrollToBottom() {
      const chat = document.getElementById('chat');
      chat.scrollTop = chat.scrollHeight;
    async function syncRealtimeStatus() {
      try {
        const [taskRes, patrolRes] = await Promise.all([
          fetch('/api/tasks'),
          fetch('/api/patrol')
        ]);
        const taskData = await taskRes.json();
        const patrolData = await patrolRes.json();

        const patrolBadge = document.getElementById('patrolStatusBadge');
        if (patrolBadge && patrolData.report) {
          const rep = patrolData.report;
          if (rep.drift_audit && rep.drift_audit.drift_detected) {
            patrolBadge.innerHTML = '<span class="dot" style="background:#f59e0b;box-shadow:0 0 8px #f59e0b"></span> 巡检漂移 (' + rep.drift_audit.items.length + '项)';
          } else {
            patrolBadge.innerHTML = '<span class="dot"></span> 巡检健康 (' + (rep.commits_reviewed || 0) + ' commits)';
          }
        }
      } catch (e) {}
    }
    setInterval(syncRealtimeStatus, 5000);
    syncRealtimeStatus();

    if (new URLSearchParams(window.location.search).get('embed') === 'true') {
      const h = document.querySelector('header');
      if (h) h.style.display = 'none';
      document.body.style.background = '#090d16';
    }
  </script>
</body>
</html>"""

    class ResidentWebHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path in ("/", "/index.html"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html_template.encode("utf-8"))))
                self.end_headers()
                self.wfile.write(html_template.encode("utf-8"))
                return
            elif parsed.path == "/api/status":
                from omo.resident.status import snapshot

                data = snapshot()
                raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return
            elif parsed.path == "/api/tasks":
                from omo.resident.dispatch import TASKS_DIR

                tasks = []
                tasks_path = write_path(TASKS_DIR)
                if tasks_path.exists():
                    for f in sorted(tasks_path.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True)[:10]:
                        try:
                            tasks.append(json.loads(f.read_text(encoding="utf-8")))
                        except Exception:
                            pass
                raw = json.dumps({"ok": True, "tasks": tasks}, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return
            elif parsed.path == "/api/patrol":
                from omo.resident.night_patrol import REPORTS_DIR

                report_file = REPORTS_DIR / "night-patrol-latest.json"
                data = {}
                if report_file.exists():
                    try:
                        data = json.loads(report_file.read_text(encoding="utf-8"))
                    except Exception:
                        pass
                raw = json.dumps({"ok": True, "report": data}, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return
            self.send_response(404)
            self.end_headers()

        def do_OPTIONS(self):
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def do_POST(self):
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path == "/api/agent/triage":
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    raw_body = self.rfile.read(length) if length > 0 else b"{}"
                    body = json.loads(raw_body.decode("utf-8"))
                except Exception:
                    self.send_response(400)
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(b'{"ok": false, "error": "invalid_json"}')
                    return

                comp = body.get("component") or "unknown_component"
                st = body.get("status") or "error"
                err = body.get("error_log") or ""
                auto_disp = body.get("auto_dispatch", True)

                res = triage_component_failure(comp, st, err, auto_dispatch=auto_disp)
                res_raw = json.dumps(res, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(res_raw)))
                self.end_headers()
                self.wfile.write(res_raw)
                return

            elif parsed.path == "/api/chat":
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    raw_body = self.rfile.read(length) if length > 0 else b"{}"
                    body = json.loads(raw_body.decode("utf-8"))
                except Exception:
                    self.send_response(400)
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(b'{"ok": false, "error": "invalid_json"}')
                    return

                prompt = (body.get("prompt") or "").strip()
                if not prompt:
                    self.send_response(400)
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(b'{"ok": false, "error": "prompt_required"}')
                    return

                tier = body.get("tier")
                history = body.get("history") or []

                try:
                    reply = ask(prompt, tier=tier, history=history)
                    res_raw = json.dumps(
                        {"ok": True, "reply": reply, "timestamp": time.time()}, ensure_ascii=False
                    ).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(res_raw)))
                    self.end_headers()
                    self.wfile.write(res_raw)
                except Exception as exc:
                    err_raw = json.dumps({"ok": False, "error": str(exc)}).encode("utf-8")
                    self.send_response(500)
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(err_raw)))
                    self.end_headers()
                    self.wfile.write(err_raw)
                return

            self.send_response(404)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

        def log_message(self, format, *args):
            pass

    server = http.server.HTTPServer((host, port), ResidentWebHandler)
    print("\n\033[1;32m⚡ Chief Resident Copilot Web Console 已启动\033[0m")
    print(f"👉 本地访问: http://127.0.0.1:{port}")
    print(f"🌐 局域网/Tailscale 跨机访问: http://{host if host != '0.0.0.0' else 'mini'}:{port}")  # noqa: S104
    print("\033[2m按 Ctrl+C 停止服务...\033[0m\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n正在关闭 Resident Web 服务...")
    finally:
        server.server_close()
    return 0


def main(argv: list[str] | None = None) -> int:
    args_list = list(argv if argv is not None else sys.argv[1:])

    # 兼容通过 cli.py 调度进入: 如果第一个词直接是 prompt 内容或以 -d 开头
    if args_list and args_list[0] not in ("ask", "chat", "web", "tick", "distill", "-h", "--help"):
        dispatch = "-d" in args_list or "--dispatch" in args_list
        clean_prompt = " ".join([a for a in args_list if a not in ("-d", "--dispatch")])
        return cmd_ask(clean_prompt, dispatch=dispatch)

    parser = argparse.ArgumentParser(description="eCOS 常驻 Agent 交互与协同控制台")
    subparsers = parser.add_subparsers(dest="action", help="交互动作")

    # ask
    p_ask = subparsers.add_parser("ask", help="向常驻 Agent 提问或派发任务")
    p_ask.add_argument("prompt", nargs="?", default="", help="提问或指令内容")
    p_ask.add_argument("-d", "--dispatch", action="store_true", help="异步派发至事件总线与信号箱")

    # chat
    subparsers.add_parser("chat", help="进入持续对话终端 (REPL 交互式)")

    # web
    p_web = subparsers.add_parser("web", help="启动常驻 Copilot Web 交互控制台")
    p_web.add_argument("--port", type=int, default=43195, help="监听端口 (默认 43195)")
    p_web.add_argument("--host", default="0.0.0.0", help="监听地址 (默认 0.0.0.0)")  # noqa: S104

    # tick
    p_tick = subparsers.add_parser("tick", help="推进协同循环 (all/sediment/decision/promote/...)")
    p_tick.add_argument("role", nargs="?", default="all", help="指定角色名 (默认 all)")

    # distill
    subparsers.add_parser("distill", help="触发大模型经验卡片萃取")

    parsed = parser.parse_args(args_list)

    if parsed.action == "ask":
        return cmd_ask(parsed.prompt, dispatch=parsed.dispatch)
    elif parsed.action == "chat":
        return cmd_chat()
    elif parsed.action == "web":
        return cmd_web(port=parsed.port, host=parsed.host)
    elif parsed.action == "tick":
        return cmd_tick(getattr(parsed, "role", "all"))
    elif parsed.action == "distill":
        return cmd_distill()

    # 默认无参数进入 chat REPL
    return cmd_chat()


if __name__ == "__main__":
    sys.exit(main())
