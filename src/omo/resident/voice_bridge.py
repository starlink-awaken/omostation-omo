"""omo.resident.voice_bridge — 伴随式原生语音交互桥梁 (Phase 11).

功能与契约:
1. 语音文本智能清洗 (clean_text_for_speech):
   - 过滤 Markdown 代码块 (``` ... ```)、行内代码 (`...`)、JSON 配置、表格线与 URL;
   - 仅保留提炼出的自然语言关键结论与建议，防止逐字机械朗读代码;
2. 语音合成自愈分级调度 (speak_text):
   - 优先尝试 AetherForge 门面 /audio/speech;
   - 离线或异常时平滑降级至 macOS 原生 say 命令 (默认 Tingting 优质中文音色);
   - 支持生成音频文件与 afplay 伴随式播放;
3. 严格遵守 ADR-0456 契约与无损兜底.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from omo.resident import write_path

logger = logging.getLogger("omo.resident.voice_bridge")

DEFAULT_ZH_VOICE = "Tingting"
DEFAULT_EN_VOICE = "Samantha"


def clean_text_for_speech(text: str) -> str:
    """为语音朗读智能清洗文本，过滤代码块、JSON、表格与技术噪声."""
    if not text:
        return ""

    # 1. 移除 Markdown 代码块 (```lang ... ```)
    cleaned = re.sub(r"```[\s\S]*?```", " ", text)

    # 2. 移除行内代码 (`code`)
    cleaned = re.sub(r"`[^`\n]+`", " ", cleaned)

    # 3. 移除 Markdown 链接中的 URL，仅保留文本 [label](url) -> label
    cleaned = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", cleaned)

    # 4. 移除裸 URL (http://... 或 https://...)
    cleaned = re.sub(r"https?://\S+", " ", cleaned)

    # 5. 移除 Markdown 表格分割线 (|---|---|)
    cleaned = re.sub(r"\|[-:\s|]+\|", " ", cleaned)
    # 移除表格行内的竖线 |
    cleaned = re.sub(r"\|", " ", cleaned)

    # 6. 移除纯 JSON 结构 (多行或内嵌花括号对且包含键值对结构)
    cleaned = re.sub(r"\{[\s\S]*?\"[^\"]+\"\s*:[\s\S]*?\}", " ", cleaned)

    # 7. 移除 Markdown 标题标记 (#, ##, ### 等)
    cleaned = re.sub(r"^[#\*\-]+\s*", "", cleaned, flags=re.MULTILINE)

    # 8. 移除多模态与系统标签 [💡 记忆召回...]、[local-mac 离线兜底输出]: 等
    cleaned = re.sub(r"\[[^\]]+\]:?", "", cleaned)

    # 9. 合并多余空白行与空格
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    result = "，".join(lines)
    result = re.sub(r"[，\s]{2,}", "，", result).strip("， \n")

    return result or text[:100]


def speak_text(
    text: str,
    voice: str | None = None,
    play: bool = True,
    output_path: Path | str | None = None,
) -> dict[str, Any]:
    """将文本合成为语音并根据参数伴随播放.

    返回:
      {
        "success": bool,
        "engine": "aetherforge" | "say" | "mock",
        "audio_path": str | None,
        "cleaned_text": str,
        "latency_ms": float,
        "error": str | None
      }
    """
    t0 = time.time()
    cleaned = clean_text_for_speech(text)
    if not cleaned:
        cleaned = text.strip() or "常驻智能体没有可朗读的内容。"

    is_zh = bool(re.search(r"[\u4e00-\u9fff]", cleaned))
    chosen_voice = voice or (DEFAULT_ZH_VOICE if is_zh else DEFAULT_EN_VOICE)

    # 1. 尝试 AetherForge 门面 (/audio/speech)
    gateway_url = os.environ.get("OMLXC_GATEWAY_URL", "http://127.0.0.1:8000")
    try:
        import urllib.request

        payload = json.dumps({
            "model": "tts-zh" if is_zh else "tts-en",
            "input": cleaned[:500],
            "voice": "vivian" if is_zh else "af_heart",
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{gateway_url}/audio/speech",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=1.5) as resp:
            audio_data = resp.read()
            target_file = Path(output_path) if output_path else Path(tempfile.gettempdir()) / f"resident_speech_{int(time.time()*1000)}.wav"
            target_file.write_bytes(audio_data)
            if play:
                subprocess.Popen(["afplay", str(target_file)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            latency = round((time.time() - t0) * 1000.0, 2)
            return {
                "success": True,
                "engine": "aetherforge",
                "audio_path": str(target_file),
                "cleaned_text": cleaned,
                "latency_ms": latency,
                "error": None,
            }
    except Exception as exc:
        logger.debug("AetherForge TTS failed, falling back to macOS say: %s", exc)

    # 2. 降级为 macOS 原生 say 命令
    try:
        target_file = Path(output_path) if output_path else Path(tempfile.gettempdir()) / f"resident_speech_{int(time.time()*1000)}.aiff"
        
        # 录制为 aiff 文件
        cmd_record = ["say", "-v", chosen_voice, "-o", str(target_file), cleaned[:600]]
        subprocess.run(cmd_record, check=True, timeout=5, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if play:
            # 伴随式异步播放
            subprocess.Popen(["afplay", str(target_file)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        latency = round((time.time() - t0) * 1000.0, 2)
        return {
            "success": True,
            "engine": "say",
            "audio_path": str(target_file),
            "cleaned_text": cleaned,
            "latency_ms": latency,
            "error": None,
        }
    except Exception as exc:
        logger.warning("macOS say execution failed: %s", exc)
        latency = round((time.time() - t0) * 1000.0, 2)
        return {
            "success": False,
            "engine": "none",
            "audio_path": None,
            "cleaned_text": cleaned,
            "latency_ms": latency,
            "error": str(exc),
        }
