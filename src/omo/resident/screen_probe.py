"""omo.resident.screen_probe — 屏幕多模态感知与受控只读取证 (Phase 11).

功能与契约:
1. 屏幕截图捕获 (capture_screen):
   - 智能前台活跃窗口 + 全屏双模 (默认聚焦前台应用窗口减少无关背景与隐私泄漏);
   - 使用 macOS 原生 screencapture -x;
   - 遵守 ADR-0456 动态状态路径，将截图沉淀至 write_path(".omo/state/daemon/media/YYYY-MM-DD/{sha256}.png");
2. 本地 OCR 降级提取 (extract_image_text):
   - 视觉大模型不可达或加载纯文本模型时，自动调用系统能力提取图像文本;
   - 优先通过 macOS 原生 swift 脚本调用 Apple Vision 框架做轻量 OCR;
   - 失败时平滑降级并输出图像基础元数据，保证绝不崩溃.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE, write_path

logger = logging.getLogger("omo.resident.screen_probe")


def _get_frontmost_window_id() -> int | None:
    """尝试获取当前前台活跃窗口的 Window ID."""
    script = """
    tell application "System Events"
        set frontApp to first application process whose frontmost is true
        set appName to name of frontApp
    end tell
    return appName
    """
    try:
        res = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, timeout=1.5)
        if res.returncode == 0 and res.stdout.strip():
            # 找到前台应用名，继续使用 CoreGraphics 脚本查找对应 window id
            # 简化为通过 screencapture -l 时需要的 window ID 查询
            win_script = f"""
            tell application "System Events" to tell process "{res.stdout.strip()}"
                try
                    set win to front window
                    return id of win
                on error
                    return ""
                end try
            end tell
            """
            w_res = subprocess.run(["osascript", "-e", win_script], capture_output=True, text=True, timeout=1.5)
            if w_res.returncode == 0 and w_res.stdout.strip().isdigit():
                return int(w_res.stdout.strip())
    except Exception as exc:
        logger.debug("Failed to resolve frontmost window id: %s", exc)
    return None


def capture_screen(
    fullscreen: bool = False,
    output_path: Path | None = None,
    workspace_root: Path | None = None,
) -> Path:
    """捕获当前屏幕快照并存入受管持久化媒体目录."""
    root = workspace_root or WORKSPACE
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    media_dir = write_path(root / ".omo/state/daemon/media") / today
    media_dir.mkdir(parents=True, exist_ok=True)

    temp_file = media_dir / f"temp_{int(time.time()*1000)}.png"

    captured = False
    if not fullscreen:
        win_id = _get_frontmost_window_id()
        if win_id is not None:
            cmd = ["screencapture", "-x", f"-l{win_id}", str(temp_file)]
            res = subprocess.run(cmd, capture_output=True, timeout=3)
            if res.returncode == 0 and temp_file.exists() and temp_file.stat().st_size > 0:
                captured = True

    if not captured:
        # 回退或明确要求全屏捕获
        cmd = ["screencapture", "-x", str(temp_file)]
        subprocess.run(cmd, check=True, timeout=4)

    if not temp_file.exists():
        raise RuntimeError("screencapture command failed to generate image")

    # 根据内容计算 sha256 建立最终持久化文件名
    content = temp_file.read_bytes()
    sha = hashlib.sha256(content).hexdigest()[:16]
    final_path = output_path or (media_dir / f"screen_{sha}.png")

    if temp_file != final_path:
        shutil.move(str(temp_file), str(final_path))

    return final_path


_SWIFT_OCR_CODE = """
import Foundation
import Vision

guard CommandLine.arguments.count > 1 else { exit(1) }
let path = CommandLine.arguments[1]
let url = URL(fileURLWithPath: path)

guard let img = try? Data(contentsOf: url) else { exit(2) }

let request = VNRecognizeTextRequest { (request, error) in
    guard let observations = request.results as? [VNRecognizedTextObservation] else { return }
    let recognizedStrings = observations.compactMap { observation in
        observation.topCandidates(1).first?.string
    }
    print(recognizedStrings.joined(separator: "\\n"))
}
request.recognitionLanguages = ["zh-Hans", "en-US"]
request.recognitionLevel = .accurate

let handler = VNImageRequestHandler(data: img, options: [:])
try? handler.perform([request])
"""


def extract_image_text(image_path: Path | str) -> str:
    """提取图像中的文字 (macOS Vision OCR 快速提取)."""
    img = Path(image_path)
    if not img.exists():
        return f"[图片不存在: {img}]"

    # 1. 尝试通过 macOS 原生 swift 脚本调用 Vision 框架
    try:
        res = subprocess.run(
            ["swift", "-", str(img.resolve())],
            input=_SWIFT_OCR_CODE,
            capture_output=True,
            text=True,
            timeout=4,
        )
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip()
    except Exception as exc:
        logger.debug("Swift Vision OCR failed: %s", exc)

    # 2. 尝试 tesseract 命令（若安装）
    if shutil.which("tesseract"):
        try:
            res = subprocess.run(
                ["tesseract", str(img.resolve()), "stdout", "-l", "chi_sim+eng"],
                capture_output=True,
                text=True,
                timeout=3,
            )
            if res.returncode == 0 and res.stdout.strip():
                return res.stdout.strip()
        except Exception:
            pass

    # 3. 兜底提取文件大小与格式描述
    stat = img.stat()
    return f"[图像文件: {img.name}, 大小: {stat.st_size // 1024}KB, 当前环境未提取出有效文本]"
