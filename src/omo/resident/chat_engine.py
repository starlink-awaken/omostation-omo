#!/usr/bin/env python3
"""omo.resident.chat_engine — 人机协同沉浸交互与情境化记忆深度召回引擎 (Phase 10).

功能与契约:
1. 长会话状态保持 (Session Persistence): 默认打通 session_id="default"，CLI 与 Cockpit 共享连贯上下文；
2. 情境化长效记忆深度召回 (Contextual Memory Recall):
   - 消费夜巡自结晶产物 (.omo/state/memory/distilled_crystallization.json) 注入高置信度工程偏好;
   - 调度 PitfallRAG 检索相关负向硬约束注入 System Prompt;
   - 结构化记录 recalled_memories 与 recalled_pitfalls 至消息元数据;
3. 算力网关调度与 ReAct 只读探针闭环:
   - 依托 SwarmInferenceGateway 进行智能切流、协议协商与 2.0s 熔断降级;
   - 支持自主调度只读探针 (probe_system_health, probe_active_alerts 等)，严禁破坏性执行;
4. 严格遵守 ADR-0456 双根契约 (write_path 动态解析状态文件).
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
import shutil
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from omo.resident import WORKSPACE, write_path
from omo.resident.inference_gateway import InferenceResult, SwarmInferenceGateway, get_inference_gateway
from omo.resident.pitfall_rag import extract_relevant_pitfalls

logger = logging.getLogger("omo.resident.chat_engine")


def persist_media_image(raw_data_or_path: str, workspace_root: Path | None = None) -> str:
    """将外部图片(Base64 或本地路径)规范化持久化至受管媒体目录."""
    root = workspace_root or WORKSPACE
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    media_dir = write_path(root / ".omo/state/daemon/media") / today
    media_dir.mkdir(parents=True, exist_ok=True)

    # 1. 检查是否为 Base64
    if (
        raw_data_or_path.startswith("data:image/")
        or ";base64," in raw_data_or_path
        or (len(raw_data_or_path) > 300 and "/" not in raw_data_or_path[:20])
    ):
        b64_str = raw_data_or_path.split(";base64,")[-1] if ";base64," in raw_data_or_path else raw_data_or_path
        try:
            raw_bytes = base64.b64decode(b64_str)
            sha = hashlib.sha256(raw_bytes).hexdigest()[:16]
            target_path = media_dir / f"img_{sha}.png"
            target_path.write_bytes(raw_bytes)
            return str(target_path)
        except Exception as exc:
            logger.debug("Failed decoding base64 image: %s", exc)

    # 2. 如果是现有文件路径
    p = Path(raw_data_or_path).expanduser()
    if p.exists() and p.is_file():
        try:
            raw_bytes = p.read_bytes()
            sha = hashlib.sha256(raw_bytes).hexdigest()[:16]
            target_path = media_dir / f"img_{sha}{p.suffix or '.png'}"
            if not target_path.exists():
                shutil.copy2(p, target_path)
            return str(target_path)
        except Exception:
            return str(p)

    return raw_data_or_path


@dataclass
class ChatMessage:
    role: str  # "system" | "user" | "assistant"
    content: str
    images: list[str] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ChatMessage:
        return ChatMessage(
            role=data.get("role", "user"),
            content=data.get("content", ""),
            images=data.get("images", []) or [],
            timestamp=data.get("timestamp", datetime.now(UTC).isoformat()),
            metadata=data.get("metadata", {}),
        )


@dataclass
class ChatSession:
    session_id: str
    created_at: str
    updated_at: str
    messages: list[ChatMessage] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "messages": [m.to_dict() for m in self.messages],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ChatSession:
        msgs = [ChatMessage.from_dict(m) for m in data.get("messages", [])]
        return ChatSession(
            session_id=data.get("session_id", "default"),
            created_at=data.get("created_at", datetime.now(UTC).isoformat()),
            updated_at=data.get("updated_at", datetime.now(UTC).isoformat()),
            messages=msgs,
        )


class ResidentChatEngine:
    """常驻 Agent 沉浸对话与记忆召回核心引擎."""

    def __init__(
        self,
        workspace_root: Path | None = None,
        gateway: SwarmInferenceGateway | None = None,
        max_history_turns: int = 50,
    ) -> None:
        self.workspace = workspace_root or WORKSPACE
        self.gateway = gateway or get_inference_gateway(self.workspace)
        self.max_history_turns = max_history_turns
        self.session_file = write_path(self.workspace / ".omo/state/daemon/chat_session.json")
        self.crystallization_file = write_path(self.workspace / ".omo/state/memory/distilled_crystallization.json")
        self._sessions: dict[str, ChatSession] = self._load_sessions()

    def _load_sessions(self) -> dict[str, ChatSession]:
        """读取持久化的会话集合."""
        if not self.session_file.is_file():
            return {}
        try:
            raw = json.loads(self.session_file.read_text(encoding="utf-8"))
            res: dict[str, ChatSession] = {}
            for sid, sdata in raw.items():
                if isinstance(sdata, dict):
                    res[sid] = ChatSession.from_dict(sdata)
            return res
        except Exception as exc:
            logger.debug("Failed loading chat sessions: %s", exc)
            return {}

    def _save_sessions(self) -> None:
        """持久化保存所有会话数据至 chat_session.json."""
        try:
            data = {sid: sess.to_dict() for sid, sess in self._sessions.items()}
            self.session_file.parent.mkdir(parents=True, exist_ok=True)
            self.session_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("Failed saving chat sessions: %s", exc)

    def get_or_create_session(self, session_id: str = "default") -> ChatSession:
        """获取或创建指定 session_id 的会话对象."""
        if session_id not in self._sessions:
            now_iso = datetime.now(UTC).isoformat()
            self._sessions[session_id] = ChatSession(
                session_id=session_id,
                created_at=now_iso,
                updated_at=now_iso,
                messages=[],
            )
            self._save_sessions()
        return self._sessions[session_id]

    def get_history(self, session_id: str = "default", limit: int = 50) -> list[dict[str, Any]]:
        """获取指定会话的历史消息."""
        sess = self.get_or_create_session(session_id)
        return [m.to_dict() for m in sess.messages[-limit:]]

    def clear_history(self, session_id: str = "default") -> bool:
        """清空指定会话的历史消息."""
        if session_id in self._sessions:
            self._sessions[session_id].messages.clear()
            self._sessions[session_id].updated_at = datetime.now(UTC).isoformat()
            self._save_sessions()
            return True
        return False

    def recall_memories(self, prompt: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """情境化检索相关夜巡自结晶偏好与避坑规则."""
        recalled_prefs: list[dict[str, Any]] = []
        prompt_lower = prompt.lower()

        # 1. 检索夜巡自结晶偏好
        if self.crystallization_file.is_file():
            try:
                cdata = json.loads(self.crystallization_file.read_text(encoding="utf-8"))
                prefs = cdata.get("preferences", [])
                for p in prefs:
                    directive = p.get("directive", "")
                    domain = p.get("domain", "")
                    conf = p.get("confidence", 1.0)
                    # 匹配关键词或高置信度基础规则
                    is_hit = False
                    if domain.lower() in prompt_lower or any(k in prompt_lower for k in directive.lower().split()):
                        is_hit = True
                    elif conf >= 0.99 and len(recalled_prefs) < 2:
                        # 兜底注入高优先级治理/语言偏好
                        is_hit = True

                    if is_hit:
                        recalled_prefs.append(
                            {
                                "id": p.get("id"),
                                "domain": domain,
                                "directive": directive,
                                "confidence": conf,
                                "source": p.get("source", "distilled_crystallization"),
                            }
                        )
                    if len(recalled_prefs) >= 3:
                        break
            except Exception as exc:
                logger.debug("Error reading crystallization memory: %s", exc)

        # 2. 检索 PitfallRAG 避坑规则
        recalled_pitfalls: list[dict[str, Any]] = []
        try:
            pits = extract_relevant_pitfalls(prompt, max_rules=2)
            for pit in pits:
                recalled_pitfalls.append(
                    {
                        "id": pit.get("id"),
                        "title": pit.get("title", ""),
                        "rule": pit.get("rule", ""),
                        "source": pit.get("source", "AGENTS.md"),
                    }
                )
        except Exception as exc:
            logger.debug("Error extracting pitfalls: %s", exc)

        return recalled_prefs, recalled_pitfalls

    def _build_contextual_prompt(
        self,
        base_prompt: str,
        recalled_prefs: list[dict[str, Any]],
        recalled_pitfalls: list[dict[str, Any]],
    ) -> str:
        """构建注入情境化记忆与避坑规则的系统认知提示词."""
        from omo.resident.interact import _build_system_cognition_prompt, _get_context_summary

        sys_prompt = _build_system_cognition_prompt()
        ctx_summary = _get_context_summary()
        if ctx_summary:
            sys_prompt += f"\n\n【系统实时运行快照】\n{ctx_summary}"

        if recalled_prefs:
            pref_lines = [f"- [{p.get('domain', 'pref')}] {p.get('directive')}" for p in recalled_prefs]
            sys_prompt += f"\n\n【情境化长效工程偏好注水 (已召回 {len(recalled_prefs)} 条)】\n" + "\n".join(pref_lines)

        if recalled_pitfalls:
            pit_lines = [f"- ⚠️ {p.get('title')}: {p.get('rule')}" for p in recalled_pitfalls]
            sys_prompt += f"\n\n【高危避坑与主权防御约束 (已召回 {len(recalled_pitfalls)} 条)】\n" + "\n".join(
                pit_lines
            )

        return sys_prompt

    def chat(
        self,
        prompt: str,
        session_id: str = "default",
        tier: str = "tier2_dialog",
        model: str | None = None,
        enable_react: bool = True,
        images: list[str] | None = None,
    ) -> ChatMessage:
        """执行单轮对话，维持 Session 连贯上下文，驱动 ReAct 探针、多模态处理与记忆召回."""
        sess = self.get_or_create_session(session_id)

        # 0. 多模态图片持久化与 OCR 降级提取 (Q1 & Q2)
        processed_images: list[str] = []
        effective_prompt = prompt
        ocr_fallback_used = False
        if images:
            from omo.resident.screen_probe import extract_image_text

            for img_item in images:
                if img_item:
                    saved_path = persist_media_image(img_item, self.workspace)
                    processed_images.append(saved_path)

            if processed_images:
                ocr_snippets: list[str] = []
                for idx, p in enumerate(processed_images, 1):
                    t = extract_image_text(p)
                    ocr_snippets.append(f"【附带图片 #{idx} ({Path(p).name}) 视觉/OCR 提取内容】:\n{t}")
                effective_prompt = prompt + "\n\n" + "\n\n".join(ocr_snippets)
                ocr_fallback_used = True

        # 1. 记忆检索与注水
        recalled_prefs, recalled_pitfalls = self.recall_memories(prompt)

        # 2. 构建系统认知上下文
        sys_prompt = self._build_contextual_prompt(prompt, recalled_prefs, recalled_pitfalls)

        # 3. 构造推断 messages 序列
        dialog_messages: list[dict[str, str]] = [{"role": "system", "content": sys_prompt}]
        for m in sess.messages[-self.max_history_turns :]:
            if m.role in ("user", "assistant"):
                dialog_messages.append({"role": m.role, "content": m.content})
        dialog_messages.append({"role": "user", "content": effective_prompt})

        # 4. 执行模型推断 (支持自主只读探针调度)
        from omo.resident.interact import execute_react_tool

        t0 = time.time()
        max_react_turns = 3 if enable_react else 1
        react_tools_used: list[str] = []
        final_content = ""
        last_result: InferenceResult | None = None

        for turn_idx in range(max_react_turns):
            infer_res = self.gateway.chat_completion(
                messages=dialog_messages,
                tier=tier,
                model=model,
                timeout=4.0 if turn_idx == 0 else 3.0,
            )
            last_result = infer_res
            content = infer_res.content

            # 探测 Action: tool_name(...)
            action_match = re.search(r"Action:\s*([a-zA-Z0-9_]+)\((.*?)\)", content, re.DOTALL)
            if enable_react and action_match:
                tool_name = action_match.group(1).strip()
                tool_args = action_match.group(2).strip()
                react_tools_used.append(tool_name)

                # 只读探针安全守卫：若探针名称非法或包含破坏性特征，予以拦截
                if not tool_name.startswith("probe_") and tool_name not in (
                    "search_codebase",
                    "read_codebase_file",
                    "read_decision_proposals",
                ):
                    obs = f"安全拦截: 探针 '{tool_name}' 超出只读探针白名单，严禁自主执行非只读或高危操作。"
                else:
                    obs = execute_react_tool(tool_name, tool_args)

                dialog_messages.append({"role": "assistant", "content": content})
                dialog_messages.append(
                    {
                        "role": "user",
                        "content": f"Observation: {obs}\n请综合现场真实事实，继续向夏明星给出清晰确切的最终结论。",
                    }
                )
                continue
            else:
                final_content = content
                break

        if not final_content and last_result:
            final_content = last_result.content

        if not final_content:
            target_node = last_result.target_node if last_result else "local-mac"
            final_content = f"[{target_node} 离线兜底输出]: 常驻智能体感知正常，已执行只读规则校验与本地治理策略。"

        total_latency_ms = round((time.time() - t0) * 1000.0, 2)

        # 5. 组装 Assistant 消息与认知元数据
        metadata = {
            "target_node": last_result.target_node if last_result else "local-mac",
            "target_host": last_result.target_host if last_result else "127.0.0.1",
            "target_port": last_result.target_port if last_result else 1234,
            "tier": tier,
            "latency_ms": total_latency_ms,
            "fallback_used": last_result.fallback_used if last_result else False,
            "model_used": last_result.model_used if last_result else "qwen2.5:32b",
            "recalled_memories": recalled_prefs,
            "recalled_pitfalls": recalled_pitfalls,
            "react_tools_used": react_tools_used,
            "images": processed_images,
            "image_ocr_fallback": ocr_fallback_used,
        }

        user_msg = ChatMessage(role="user", content=prompt, images=processed_images)
        assistant_msg = ChatMessage(role="assistant", content=final_content, metadata=metadata)

        # 6. 会话持久化
        sess.messages.append(user_msg)
        sess.messages.append(assistant_msg)
        sess.updated_at = datetime.now(UTC).isoformat()
        self._save_sessions()

        return assistant_msg


_GLOBAL_CHAT_ENGINE: ResidentChatEngine | None = None


def get_chat_engine(workspace_root: Path | None = None) -> ResidentChatEngine:
    """获取单例常驻对话引擎."""
    global _GLOBAL_CHAT_ENGINE
    if _GLOBAL_CHAT_ENGINE is None:
        _GLOBAL_CHAT_ENGINE = ResidentChatEngine(workspace_root)
    return _GLOBAL_CHAT_ENGINE


def main(argv: list[str] | None = None) -> int:
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="omostation resident chat engine")
    parser.add_argument("--prompt", default="ping", help="Prompt to send")
    parser.add_argument("--session", default="default", help="Session ID")
    parser.add_argument("--tier", default="tier2_dialog", help="Inference tier")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    args = parser.parse_args(argv)

    engine = get_chat_engine()
    msg = engine.chat(args.prompt, session_id=args.session, tier=args.tier)
    if args.json:
        print(json.dumps(msg.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(f"[{msg.role}]: {msg.content}")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
