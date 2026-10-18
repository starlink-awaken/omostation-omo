#!/usr/bin/env python3
"""omo.resident.service_ctl — 系统级守护进程 (LaunchAgent) 安装与运维控制 (Phase 9).

提供:
1. `install`: 生成并注册 `~/Library/LaunchAgents/com.omostation.resident-agent.plist`;
2. `uninstall`: 卸载并安全注销 LaunchAgent 任务;
3. `status`: 查看 launchctl 运行态与心跳记录;
4. `trigger`: 即时演练触发 `patrol`, `night`, `briefing`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

from omo.resident import WORKSPACE, write_path

logger = logging.getLogger("omo.resident.service_ctl")

PLIST_LABEL = "com.omostation.resident-agent"
USER_LAUNCHAGENTS_DIR = Path.home() / "Library/LaunchAgents"
TARGET_PLIST_FILE = USER_LAUNCHAGENTS_DIR / f"{PLIST_LABEL}.plist"


def generate_plist_xml(workspace: Path) -> str:
    """生成合规的 LaunchAgent 描述 XML."""
    python_bin = sys.executable
    if not Path(python_bin).exists():
        python_bin = "/opt/homebrew/bin/python3"

    stdout_path = str(workspace / "runtime/cron/resident-daemon.log")
    workspace_str = str(workspace)

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>{PLIST_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
      <string>{python_bin}</string>
      <string>-B</string>
      <string>-m</string>
      <string>omo.resident.service_runner</string>
      <string>--daemon</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
      <key>PYTHONDONTWRITEBYTECODE</key>
      <string>1</string>
      <key>PATH</key>
      <string>/Users/xiamingxing/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
      <key>PYTHONPATH</key>
      <string>{workspace_str}/projects/omo/src:{workspace_str}</string>
      <key>OMOSTATION_ROOT</key>
      <string>{workspace_str}</string>
    </dict>
    <key>WorkingDirectory</key>
    <string>{workspace_str}</string>
    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{stdout_path}</string>
    <key>StandardErrorPath</key>
    <string>{stdout_path}</string>
  </dict>
</plist>
"""


def install_service(workspace: Path = WORKSPACE) -> dict[str, Any]:
    """安装并启动 LaunchAgent 守护服务."""
    try:
        USER_LAUNCHAGENTS_DIR.mkdir(parents=True, exist_ok=True)
        log_dir = write_path(workspace / "runtime/cron")
        log_dir.mkdir(parents=True, exist_ok=True)

        xml = generate_plist_xml(workspace)
        TARGET_PLIST_FILE.write_text(xml, encoding="utf-8")

        # 先尝试卸载已存在的实例以防冲突
        subprocess.run(["launchctl", "unload", str(TARGET_PLIST_FILE)], capture_output=True, text=True)
        # 加载并自启
        res = subprocess.run(
            ["launchctl", "load", "-w", str(TARGET_PLIST_FILE)], capture_output=True, text=True, check=True
        )

        return {
            "ok": True,
            "message": f"Service successfully installed to {TARGET_PLIST_FILE}",
            "plist": str(TARGET_PLIST_FILE),
            "label": PLIST_LABEL,
        }
    except Exception as exc:
        logger.error("Failed installing LaunchAgent service: %s", exc)
        return {"ok": False, "error": str(exc)}


def uninstall_service() -> dict[str, Any]:
    """卸载并注销 LaunchAgent 守护服务."""
    try:
        if TARGET_PLIST_FILE.is_file():
            subprocess.run(["launchctl", "unload", "-w", str(TARGET_PLIST_FILE)], capture_output=True, text=True)
            TARGET_PLIST_FILE.unlink(missing_ok=True)
            return {"ok": True, "message": f"Service {PLIST_LABEL} uninstalled cleanly."}
        return {"ok": True, "message": "Service plist not found; already clean."}
    except Exception as exc:
        logger.error("Failed uninstalling service: %s", exc)
        return {"ok": False, "error": str(exc)}


def get_service_status(workspace: Path = WORKSPACE) -> dict[str, Any]:
    """查询守护进程当前的活跃状态与调度历史."""
    status_info: dict[str, Any] = {
        "installed": TARGET_PLIST_FILE.is_file(),
        "label": PLIST_LABEL,
        "launchd_running": False,
        "pid": None,
        "state": {},
        "recent_log": "",
    }

    # 1. 检查 launchctl 状态
    try:
        res = subprocess.run(["launchctl", "list"], capture_output=True, text=True)
        for line in res.stdout.splitlines():
            if PLIST_LABEL in line:
                parts = line.split()
                if len(parts) >= 3:
                    status_info["launchd_running"] = True
                    if parts[0] != "-":
                        status_info["pid"] = int(parts[0])
                break
    except Exception as exc:
        logger.debug("Failed querying launchctl list: %s", exc)

    # 2. 检查内部快照文件
    state_file = write_path(workspace / ".omo/state/daemon/service_state.json")
    if state_file.is_file():
        try:
            status_info["state"] = json.loads(state_file.read_text(encoding="utf-8"))
        except Exception:
            pass

    # 3. 截取最近 10 行日志
    log_file = write_path(workspace / "runtime/cron/resident-daemon.log")
    if log_file.is_file():
        try:
            lines = log_file.read_text(encoding="utf-8").strip().splitlines()
            status_info["recent_log"] = "\n".join(lines[-10:])
        except Exception:
            pass

    return status_info


def trigger_job(job_name: str, workspace: Path = WORKSPACE) -> dict[str, Any]:
    """手动立即触发一次指定巡航任务."""
    from omo.resident.service_runner import ResidentServiceRunner

    runner = ResidentServiceRunner(workspace)
    return runner.run_single_job(job_name)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="omostation resident service ctl")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("install", help="Install & load LaunchAgent service")
    sub.add_parser("uninstall", help="Unload & remove LaunchAgent service")
    sub.add_parser("status", help="Get service running status & logs")

    trig = sub.add_parser("trigger", help="Immediately run a scheduled job")
    trig.add_argument("job", choices=["patrol", "night", "briefing"], help="Job to trigger")

    args = parser.parse_args(argv)
    if args.cmd == "install":
        print(json.dumps(install_service(), ensure_ascii=False, indent=2))
    elif args.cmd == "uninstall":
        print(json.dumps(uninstall_service(), ensure_ascii=False, indent=2))
    elif args.cmd == "status":
        print(json.dumps(get_service_status(), ensure_ascii=False, indent=2))
    elif args.cmd == "trigger":
        print(json.dumps(trigger_job(args.job), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
