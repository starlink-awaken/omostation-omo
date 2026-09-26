import json
import os
from pathlib import Path
from typing import Any

import httpx
import yaml

# LLM Gateway endpoint — 可配置，默认 localhost:9290
_LLM_GATEWAY_URL = os.environ.get("C2G_LLM_URL", "http://127.0.0.1:4000/v1/chat/completions")


def _find_cognitive_framework_dir() -> Path | None:
    """尽量从当前仓定位 ecos 的 cognitive framework 目录, 避免用户路径硬编码."""
    here = Path(__file__).resolve()
    candidates: list[Path] = []
    for parent in here.parents:
        candidates.append(parent / "projects" / "ecos" / "src" / "ecos" / "ssot" / "mof" / "m1" / "cognitive_framework")
        candidates.append(parent / "ecos" / "src" / "ecos" / "ssot" / "mof" / "m1" / "cognitive_framework")
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return None


def _load_cognitive_cartridges() -> str:
    """动态加载 M1 层的认知卡带"""
    frameworks = []
    framework_dir = _find_cognitive_framework_dir()
    if framework_dir:
        for f in sorted(framework_dir.glob("*.yaml")):
            try:
                with f.open("r", encoding="utf-8") as file:
                    data = yaml.safe_load(file)
                    if data and data.get("type") == "CognitiveFramework":
                        frameworks.append(f"- {data.get('id')}: {data.get('name')} ({data.get('description')})")
            except Exception:  # noqa: BLE001, S110  # defensive fallback
                pass
    if not frameworks:
        return "无可用卡带"
    return "\n".join(frameworks)


def _parse_json_result(result_text: str) -> list[dict[str, Any]]:
    """
    鲁棒 JSON 提取：优先级 1 = markdown ```json 块，优先级 2 = 裸 { 或 [。
    失败时返回 None（由调用方决定是否回退）。
    """
    text = result_text.strip()

    # 优先级 1: 提取 ```json ... ``` 块 (re.DOTALL, 非贪婪)
    import re

    m = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL)
    if m:
        json_str = m.group(1).strip()
        try:
            parsed = json.loads(json_str)
            return parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            pass  # 继续尝试优先级 2

    # 优先级 2: 找首个 { 或 [ 到末尾
    first_char = text[0] if text else ""
    if first_char in ("{", "["):
        # 找到结构末尾
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            # 尝试裁掉末尾可能的 markdown 噪音
            for end in range(len(text), 0, -1):
                try:
                    parsed = json.loads(text[:end])
                    return parsed if isinstance(parsed, list) else [parsed]
                except json.JSONDecodeError:
                    continue
            # 全部失败，放弃

    return None  # type: ignore[reportReturnType]


def extract_tasks_from_pitch(pitch_content: str) -> list[dict[str, Any]]:
    """
    通过 AetherForge LLM-Gateway 将 Markdown Pitch 解析为结构化的 OMO 任务列表。
    如果 LLM-Gateway 服务不可用，则自动回退到 Mock 逻辑。
    """
    url = _LLM_GATEWAY_URL
    cartridges_context = _load_cognitive_cartridges()

    prompt = f"""
    你是一个 OMO 架构下的首席技术合伙人 (CTO)。
    请阅读下面的点子 (Pitch) 提案，将其拆解为 1-3 个具体的 OMO 执行任务。

    在我们的系统中，我们支持挂载以下认知卡带 (Cognitive Cartridges) 以处理不同类型的任务：
    {cartridges_context}

    必须以 JSON 数组格式返回，每个任务必须包含以下字段：
    - title: 任务标题
    - description: 任务详细描述（包含技术上下文）
    - task_type: "feature" 或 "refactor" 或 "bugfix"
    - risk_level: "L0" 到 "L3" (一般填 L0 或 L1)
    - cognitive_cartridge: 字符串，请从上述可用的卡带 ID 中选择一个最适合执行此任务的卡带（例如 GSD-V1，如果没有合适的则留空）
    - deliverables: 数组，预期的交付物列表
    - evidence_required: 数组，需要提供的验收证据
    - test_plan: 数组，测试计划

    Pitch 提案内容：
    ---
    {pitch_content}
    ---

    请严格返回合法的 JSON 数组，不要包含 ```json 等 Markdown 标记，直接返回内容。
    """

    payload = {"prompt": prompt, "model": None}

    try:
        # 使用 trust_env=False 避免本地代理报错 (socksio)
        with httpx.Client(trust_env=False) as client:
            resp = client.post(url, json=payload, timeout=30.0)
            resp.raise_for_status()
            raw = resp.content
            data = json.loads(raw)
            result_text = data.get("content", "")

        # 兼容 AetherForge LLM-Gateway 在无可用模型时的 HITL 返回
        if "[HITL]" in result_text or result_text.startswith("[ERROR]"):
            print("  ⚠️ LLM-Gateway 进入了 HITL 模式或报错，回退到 Mock 逻辑。")
            return _mock_extract(pitch_content)

        tasks = _parse_json_result(result_text)
        if tasks is None:
            print("  ⚠️ LLM-Gateway 返回内容无法解析为 JSON，回退到 Mock 逻辑。")
            return _mock_extract(pitch_content)

        if not isinstance(tasks, list):
            tasks = [tasks]

        print("  ✅ 成功通过 LLM-Gateway 完成智能拆解！")
        return tasks  # type: ignore[reportReturnType]
    except Exception as e:  # noqa: BLE001  # defensive fallback
        import traceback

        print(f"  ⚠️ LLM-Gateway 请求失败或返回非预期格式: {e}，回退到 Mock 逻辑。")
        traceback.print_exc()
        return _mock_extract(pitch_content)


def _mock_extract(pitch_content: str) -> list[dict[str, Any]]:
    """
    当 LLM Gateway 不可用时，基于 Pitch 的 frontmatter 和 The What 章节生成一个合理的默认任务。
    避免对所有 Pitch 都返回固定的 'Cognitive Cartridges' 任务。
    """
    lines = pitch_content.splitlines()

    title = "执行 Pitch 派生任务"
    for line in lines:
        if line.strip().startswith("# Pitch:"):
            title = line.replace("# Pitch:", "").strip()
            break
        elif line.strip().startswith("# "):
            title = line.replace("# ", "").strip()
            break

    description = f"从 Pitch 转化而来的任务: {title}"
    in_what = False
    what_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("##") and "What" in stripped:
            in_what = True
            continue
        if in_what:
            if stripped.startswith("##"):
                break
            if stripped:
                what_lines.append(stripped)
    if what_lines:
        description = " ".join(what_lines[:3])

    deliverables = [f"完成 {title} 的目标清单"]
    evidence_required = ["Pitch 目标达成证明"]
    test_plan = ["依据 Pitch 验收标准验证"]

    return [
        {
            "title": title,
            "description": description,
            "task_type": "feature",
            "risk_level": "L1",
            "cognitive_cartridge": "",
            "deliverables": deliverables,
            "evidence_required": evidence_required,
            "test_plan": test_plan,
        }
    ]
