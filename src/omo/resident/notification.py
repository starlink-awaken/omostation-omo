#!/usr/bin/env python3
"""omo.resident.notification — 多通道主动通信枢纽与告警降噪中心 (Phase 8).

提供:
1. P0/P1/P2 分级告警与跨通道分发 (macOS 原生弹窗、飞书 Webhook、邮件投递);
2. 指纹去重 (Fingerprint De-duping) 与告警风暴熔断机制;
3. 敏感凭据/Token 自动脱敏器 (Sanitizer);
4. 通知分发审计日志持久化 (.omo/state/notification/history.jsonl).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import subprocess
import time
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from omo.resident import WORKSPACE, write_path

logger = logging.getLogger("omo.resident.notification")


class AlertLevel(str, Enum):
    P0_CRITICAL = "P0_CRITICAL"  # 集群全宕机、CI 致命阻断、安全门禁高危违规
    P1_WARNING = "P1_WARNING"    # 踩坑规则待复核、治理升级待办、降级运行
    P2_INFO = "P2_INFO"          # 晨间简报就绪、夜巡平稳完成、节点上线


@dataclass
class NotificationEvent:
    id: str
    level: AlertLevel
    title: str
    body: str
    source: str = "resident"
    tags: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    metadata: dict[str, Any] = field(default_factory=dict)


# 正则脱敏敏感 Key 与 Token
SENSITIVE_PATTERNS = [
    re.compile(r"(ghp_[A-Za-z0-9]{30,})"),               # GitHub Personal Access Token
    re.compile(r"(gho_[A-Za-z0-9]{30,})"),               # GitHub OAuth Token
    re.compile(r"(xoxb-[A-Za-z0-9-]{20,})"),             # Slack Bot Token
    re.compile(r"(sk-[A-Za-z0-9]{20,})"),                # OpenAI / LLM API Key
    re.compile(r"(tskey-auth-[A-Za-z0-9-_]+)"),          # Tailscale Auth Key
    re.compile(r"(https?://open\.feishu\.cn/open-apis/bot/v2/hook/)([a-f0-9-]+)"),  # Feishu Webhook Token
    re.compile(r"([A-Za-z0-9+/]{40,}={0,2})"),           # Base64 Long Secret
]


def sanitize_text(text: str) -> str:
    """自动过滤并脱敏文本中的私有 Token 和敏感密钥."""
    if not text:
        return ""
    sanitized = text
    for pattern in SENSITIVE_PATTERNS:
        sanitized = pattern.sub(r"***REDACTED***", sanitized)
    return sanitized


class NotificationThrottler:
    """告警去重与防风暴熔断器."""

    def __init__(self, window_seconds: float = 60.0) -> None:
        self.window_seconds = window_seconds
        # fingerprint -> (last_sent_timestamp, suppressed_count)
        self._cache: dict[str, tuple[float, int]] = {}

    def compute_fingerprint(self, event: NotificationEvent) -> str:
        """根据级别、来源和标题计算事件指纹."""
        raw = f"{event.level.value}:{event.source}:{event.title.strip()}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def should_send(self, event: NotificationEvent) -> tuple[bool, int]:
        """判定当前事件是否应该分发，返回 (是否分发, 被压制的历史次数)."""
        fp = self.compute_fingerprint(event)
        now = time.time()

        if fp in self._cache:
            last_time, count = self._cache[fp]
            if now - last_time < self.window_seconds:
                # 在静默窗口内，压制并不再重复分发
                self._cache[fp] = (last_time, count + 1)
                return False, count + 1

        # 超出窗口或首次出现，放行并重置计数
        self._cache[fp] = (now, 0)
        return True, 0


class NotificationHub:
    """多通道主动通信中枢."""

    def __init__(self, workspace_root: Path | None = None) -> None:
        self.workspace = workspace_root or WORKSPACE
        self.log_dir = self.workspace / ".omo/state/notification"
        self.history_file = self.log_dir / "history.jsonl"
        self.throttler = NotificationThrottler(window_seconds=60.0)
        self.feishu_webhook_url = os.environ.get("FEISHU_WEBHOOK_URL", "")

    def send_macos_notification(self, title: str, body: str, sound: bool = True) -> bool:
        """通过 macOS 原生 osascript 发送桌面弹窗通知."""
        try:
            clean_title = sanitize_text(title).replace('"', '\\"')
            clean_body = sanitize_text(body).replace('"', '\\"')
            sound_clause = ' sound name "default"' if sound else ""
            script = f'display notification "{clean_body}" with title "{clean_title}"{sound_clause}'
            res = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, timeout=3)
            return res.returncode == 0
        except Exception as exc:
            logger.debug("Failed sending macOS notification: %s", exc)
            return False

    def send_feishu_webhook(self, webhook_url: str, title: str, body: str, level: AlertLevel) -> bool:
        """向飞书自定义机器人 Webhook 发送格式化消息."""
        if not webhook_url:
            return False
        try:
            clean_title = sanitize_text(title)
            clean_body = sanitize_text(body)
            color_prefix = "🚨 " if level == AlertLevel.P0_CRITICAL else ("⚠️ " if level == AlertLevel.P1_WARNING else "ℹ️ ")

            payload = {
                "msg_type": "text",
                "content": {
                    "text": f"{color_prefix}[{level.value}] {clean_title}\n\n{clean_body}\n\n— omostation 常驻智能守护"
                },
            }
            req = urllib.request.Request(
                webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json", "User-Agent": "omostation-notification-hub"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status == 200
        except Exception as exc:
            logger.debug("Failed sending Feishu webhook: %s", exc)
            return False

    def notify(self, event: NotificationEvent, force_macos: bool = True) -> dict[str, Any]:
        """统一分发通知主入口 (包含脱敏、去重、多通道派发与审计写盘)."""
        # 1. 文本脱敏
        event.title = sanitize_text(event.title)
        event.body = sanitize_text(event.body)

        # 2. 去重防风暴
        should_dispatch, suppressed_count = self.throttler.should_send(event)
        result: dict[str, Any] = {
            "id": event.id,
            "dispatched": False,
            "suppressed": not should_dispatch,
            "suppressed_count": suppressed_count,
            "channels": {},
        }

        if not should_dispatch:
            logger.info("Notification suppressed by throttler: %s (count=%d)", event.title, suppressed_count)
            return result

        # 3. 多通道派发
        channels: dict[str, bool] = {}

        # 通道 1: macOS 原生桌面弹窗 (所有级别均可弹，P0 必须带声音)
        if force_macos or event.level in (AlertLevel.P0_CRITICAL, AlertLevel.P1_WARNING):
            play_sound = event.level == AlertLevel.P0_CRITICAL
            ok_macos = self.send_macos_notification(event.title, event.body, sound=play_sound)
            channels["macos"] = ok_macos

        # 通道 2: 飞书 Webhook
        if self.feishu_webhook_url:
            ok_feishu = self.send_feishu_webhook(self.feishu_webhook_url, event.title, event.body, event.level)
            channels["feishu"] = ok_feishu
        else:
            channels["feishu"] = False

        result["dispatched"] = any(channels.values())
        result["channels"] = channels

        # 4. 记录审计日志
        self._record_history(event, result)
        return result

    def _record_history(self, event: NotificationEvent, dispatch_result: dict[str, Any]) -> None:
        """异步/写盘记录通知历史."""
        try:
            write_path(self.log_dir).mkdir(parents=True, exist_ok=True)
            record = {
                "id": event.id,
                "timestamp": event.created_at,
                "level": event.level.value,
                "title": event.title,
                "body": event.body,
                "source": event.source,
                "tags": event.tags,
                "dispatch": dispatch_result,
            }
            with write_path(self.history_file).open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as exc:
            logger.debug("Failed writing notification history: %s", exc)

    def get_recent_history(self, limit: int = 50) -> list[dict[str, Any]]:
        """拉取最近的通知记录."""
        hist = []
        if not write_path(self.history_file).is_file():
            return []
        try:
            lines = write_path(self.history_file).read_text(encoding="utf-8").strip().splitlines()
            for line in lines[-limit:]:
                if line.strip():
                    hist.append(json.loads(line))
        except Exception as exc:
            logger.debug("Failed reading notification history: %s", exc)
        return list(reversed(hist))


_GLOBAL_HUB: NotificationHub | None = None


def get_notification_hub(workspace_root: Path | None = None) -> NotificationHub:
    """获取单例 NotificationHub."""
    global _GLOBAL_HUB
    if _GLOBAL_HUB is None:
        _GLOBAL_HUB = NotificationHub(workspace_root)
    return _GLOBAL_HUB
