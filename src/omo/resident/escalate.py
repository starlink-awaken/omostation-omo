#!/usr/bin/env python3
"""omo.resident.escalate — Sentinel 巡检哨兵主动告警与异常触达引擎 (WP-Escalate).

实现多级渐进主动告警 (Proactive Escalation):
  1. 深度探活核心基础设施、GaC 规则硬停、Git Remote 污染与物理资源
  2. 状态机去重与静默期保护 (P0 15m / P1 60m)，避免警报风暴
  3. 多级分发: macOS 原生桌面横幅 + 警报音 (L1) -> SwiftBar 状态高亮 (L2) -> 外部移动端 Push (L3)
  4. 支持异常自愈/恢复检测与绿色回执通知

Usage:
  omo resident escalate check       # 执行一次全面巡检与主动触达
  omo resident escalate status      # 查看当前活动告警与历史
  omo resident escalate test        # 发送测试告警验证触达链路
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE, state_root, write_path

ALERTS_DIR = Path.home() / ".local/share/omostation/alerts"
ALERTS_STATE_FILE = ALERTS_DIR / "active-alerts.json"
NOTIFY_SH = Path.home() / ".local/bin/clash-notify.sh"

MACMINI_TAILSCALE_IP = "100.64.110.118"

# 冷却周期 (秒)
COOLDOWN_P0 = 900  # 15 分钟
COOLDOWN_P1 = 3600  # 60 分钟


def _probe_socket(port: int, host: str = "127.0.0.1", timeout: float = 0.8) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _load_alerts_state() -> dict[str, Any]:
    if not ALERTS_STATE_FILE.exists():
        return {"active": {}, "resolved": []}
    try:
        return json.loads(ALERTS_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"active": {}, "resolved": []}


def _save_alerts_state(state: dict[str, Any]) -> None:
    ALERTS_DIR.mkdir(parents=True, exist_ok=True)
    ALERTS_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def probe_remote_hygiene() -> dict[str, Any] | None:
    """检查主仓 remote origin 是否被污染为子模块仓库 (2026-09-13 实证故障守护)."""
    try:
        res = subprocess.run(
            ["git", "-C", str(WORKSPACE), "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        url = res.stdout.strip()
        if res.returncode != 0 or not url:
            return {
                "id": "GIT_REMOTE_MISSING",
                "severity": "P0",
                "title": "Git Remote Origin 丢失",
                "detail": f"主仓无法解析 origin URL: {res.stderr.strip()}",
                "recommended": "bash bin/gac/fix-remotes.py",
            }

        # 污染特征: origin 变成了子模块名
        submodule_names = ["cockpit-ui", "omostation-runtime", "ecos", "bus-foundation", "kairon", "gbrain"]
        for sub in submodule_names:
            if url.rstrip("/").endswith(f"/{sub}") or url.rstrip("/").endswith(f"/{sub}.git"):
                return {
                    "id": "GIT_REMOTE_POLLUTED",
                    "severity": "P0",
                    "title": "Git Remote Origin 受到子模块污染",
                    "detail": f"主仓 origin 被意外改写为子模块仓库: {url}",
                    "recommended": "python3 bin/ssot/fix-remotes.py",
                }
    except Exception as exc:
        pass
    return None


def probe_gac_violations() -> dict[str, Any] | None:
    """检查最近的 GaC 拦截与规则硬停."""
    vitality_file = write_path(WORKSPACE / ".omo/state/rule-vitality.jsonl")
    if vitality_file.exists():
        try:
            # 读最后 20 行
            lines = vitality_file.read_text(encoding="utf-8").splitlines()[-20:]
            for line_str in reversed(lines):
                if not line_str.strip():
                    continue
                d = json.loads(line_str)
                if d.get("violated"):
                    r_id = d.get("rule_id", "未知规则")
                    gate = d.get("gate", "gac-gate")
                    ts = d.get("timestamp") or d.get("ts") or ""
                    return {
                        "id": f"GAC_VIOLATION_{r_id}",
                        "severity": "P0",
                        "title": f"GaC 规则硬停拦截: {r_id}",
                        "detail": f"门禁 {gate} 拦截到违规操作 (时间: {ts})",
                        "recommended": "make gac-local-gate",
                    }
        except Exception:
            pass
    return None


def probe_infrastructure() -> list[dict[str, Any]]:
    """检查算力与驾驶舱等核心端点可用性."""
    alerts = []

    # 1. AetherForge: 本机与 MacMini 至少有一个在线
    local_af = _probe_socket(4000, "127.0.0.1")
    mini_af = _probe_socket(4000, MACMINI_TAILSCALE_IP)
    if not local_af and not mini_af:
        alerts.append(
            {
                "id": "INFRA_AETHERFORGE_OFFLINE",
                "severity": "P0",
                "title": "AetherForge 算力集群完全离线",
                "detail": f"本机 127.0.0.1:4000 与 MacMini {MACMINI_TAILSCALE_IP}:4000 均无响应",
                "recommended": "aictl heal",
            }
        )

    # 2. Web Cockpit 驾驶舱: 端口 43191
    if not _probe_socket(43191, "127.0.0.1"):
        alerts.append(
            {
                "id": "INFRA_COCKPIT_OFFLINE",
                "severity": "P1",
                "title": "全景 Web 驾驶舱离线",
                "detail": "本地端口 43191 服务未启动或已崩溃",
                "recommended": "launchctl kickstart -k gui/$(id -u)/com.omostation.zhixing-dashboard",
            }
        )

    # 3. Magpie 旗舰网关: 端口 3425 (支持双机漫游检查)
    local_magpie = _probe_socket(3425, "127.0.0.1")
    mini_magpie = _probe_socket(3425, MACMINI_TAILSCALE_IP)
    if not local_magpie and not mini_magpie:
        alerts.append(
            {
                "id": "INFRA_MAGPIE_OFFLINE",
                "severity": "P1",
                "title": "Magpie 推理网关离线",
                "detail": "本机与 MacMini 端口 3425 均无法连接",
                "recommended": "brew services restart magpie",
            }
        )

    return alerts


def probe_disk_space() -> dict[str, Any] | None:
    """检查磁盘剩余空间 (不足 10GB 报警)."""
    try:
        usage = shutil.disk_usage("/Users")
        free_gb = usage.free / (1024**3)
        if free_gb < 10.0:
            return {
                "id": "DISK_SPACE_LOW",
                "severity": "P1",
                "title": f"系统磁盘空间严重不足 ({free_gb:.1f}GB)",
                "detail": f"/Users 可用空间仅剩 {free_gb:.1f}GB，可能导致大模型缓存与日志写入中断",
                "recommended": "make worktree-hygiene",
            }
    except Exception:
        pass
    return None


def deliver_notification(alert: dict[str, Any], is_resolved: bool = False) -> None:
    """多级分发触达渠道 (macOS 桌面横幅 + 声音 + 企业微信/TG)."""
    severity = alert.get("severity", "P1")
    title = alert.get("title", "系统异常")
    detail = alert.get("detail", "")

    if is_resolved:
        banner_title = "✅ eCOS 哨兵自愈通知"
        banner_sub = f"已恢复: {title}"
        sound = "Ping"
    else:
        icon = "🚨" if severity == "P0" else "⚠️"
        banner_title = f"{icon} eCOS 哨兵主动告警 [{severity}]"
        banner_sub = title
        sound = "Basso" if severity == "P0" else "Glass"

    # L1: macOS 原生桌面横幅与警报音
    try:
        clean_sub = banner_sub.replace('"', '\\"')
        clean_detail = detail.replace('"', '\\"')[:120]
        script = f'display notification "{clean_detail}" with title "{banner_title}" subtitle "{clean_sub}" sound name "{sound}"'
        subprocess.Popen(["osascript", "-e", script])
    except Exception as exc:
        print(f"[escalate] macOS notification failed: {exc}", file=sys.stderr)

    # L3: 外部移动端推送 (若配置)
    if NOTIFY_SH.is_file() and os.access(NOTIFY_SH, os.X_OK):
        try:
            msg_text = f"{banner_title}\n- 主题: {banner_sub}\n- 详情: {detail}"
            if alert.get("recommended"):
                msg_text += f"\n- 建议自愈: {alert['recommended']}"
            subprocess.Popen([str(NOTIFY_SH), msg_text])
        except Exception:
            pass


def check_and_escalate(dry_run: bool = False) -> dict[str, Any]:
    """执行全面主动巡检、状态对账与告警分发."""
    candidates: list[dict[str, Any]] = []

    # 1. 探针收集
    r_hyg = probe_remote_hygiene()
    if r_hyg:
        candidates.append(r_hyg)

    gac_viol = probe_gac_violations()
    if gac_viol:
        candidates.append(gac_viol)

    infra_alerts = probe_infrastructure()
    candidates.extend(infra_alerts)

    disk_alert = probe_disk_space()
    if disk_alert:
        candidates.append(disk_alert)

    # 2. 状态机对账与去重
    now_ts = time.time()
    now_iso = datetime.now(UTC).isoformat()
    state = _load_alerts_state()
    active = state.get("active", {})
    resolved_history = state.get("resolved", [])

    current_ids = {a["id"] for a in candidates}
    notified_count = 0
    resolved_count = 0

    # 检查自愈已恢复项
    recovered_ids = [aid for aid in active if aid not in current_ids]
    for aid in recovered_ids:
        rec_alert = active.pop(aid)
        rec_alert["resolved_at"] = now_iso
        resolved_history.append(rec_alert)
        if len(resolved_history) > 50:
            resolved_history = resolved_history[-50:]
        resolved_count += 1
        if not dry_run:
            deliver_notification(rec_alert, is_resolved=True)

    # 检查新增或需重复告警项
    for a in candidates:
        aid = a["id"]
        sev = a["severity"]
        cooldown = COOLDOWN_P0 if sev == "P0" else COOLDOWN_P1

        need_notify = False
        if aid not in active:
            # 新增告警
            a["first_seen"] = now_iso
            a["last_notified_ts"] = now_ts
            a["notified_count"] = 1
            active[aid] = a
            need_notify = True
        else:
            # 已有告警，检查冷却时间
            last_ts = active[aid].get("last_notified_ts", 0)
            if now_ts - last_ts >= cooldown:
                active[aid]["last_notified_ts"] = now_ts
                active[aid]["notified_count"] = active[aid].get("notified_count", 1) + 1
                need_notify = True

        if need_notify:
            notified_count += 1
            if not dry_run:
                deliver_notification(a, is_resolved=False)

    if not dry_run:
        _save_alerts_state({"active": active, "resolved": resolved_history})

    return {
        "timestamp": now_iso,
        "scanned": len(candidates),
        "active_count": len(active),
        "notified_count": notified_count,
        "resolved_count": resolved_count,
        "active_alerts": list(active.values()),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("check", help="执行一次全面巡检与主动触达")
    sub.add_parser("status", help="查看当前活动告警与状态")

    test_p = sub.add_parser("test", help="触发一条测试告警验证通知链路")
    test_p.add_argument("--severity", choices=["P0", "P1"], default="P0")

    args = parser.parse_args(argv)
    cmd = args.command or "check"

    if cmd == "check":
        report = check_and_escalate(dry_run=False)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    elif cmd == "status":
        state = _load_alerts_state()
        active = state.get("active", {})
        print(f"🚨 当前活动告警: {len(active)} 项")
        for aid, a in active.items():
            print(f"  • [{a.get('severity')}] {a.get('title')}")
            print(f"    详情: {a.get('detail')}")
            print(f"    首次发现: {a.get('first_seen')} · 已通知 {a.get('notified_count', 1)} 次")
            if a.get("recommended"):
                print(f"    推荐自愈: {a.get('recommended')}")
        return 0

    elif cmd == "test":
        test_alert = {
            "id": f"TEST_ALERT_{int(time.time())}",
            "severity": args.severity,
            "title": f"哨兵主动告警通道链路测试 ({args.severity})",
            "detail": "本条告警由用户显式触发，用于验证 macOS 桌面横幅与警报音分发链路",
            "recommended": "无需处理，系统运行正常",
        }
        print(f"🚀 发送测试告警: {test_alert['title']} ...")
        deliver_notification(test_alert, is_resolved=False)
        print("✅ 桌面通知与系统声音已派发！请核对屏幕右上角横幅。")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
