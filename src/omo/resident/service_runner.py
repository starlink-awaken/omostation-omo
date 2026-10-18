#!/usr/bin/env python3
"""omo.resident.service_runner — 常驻守护进程核心调度循环 (Phase 9).

提供:
1. 双模时间循环:
   - 日间快速巡航 (每 30 分钟: patrol 探针扫描 + R0 治愈);
   - 深夜无人体检 (每日 03:00: fork/subprocess 派生隔离子进程跑 night_patrol 与记忆自结晶);
   - 晨间简报投递 (每日 08:30: 汇编 morning_briefing 并触发多通道桌面/飞书推送);
2. 单任务即刻演练与触发入口 (--run-job [patrol|night|briefing]);
3. 状态快照与心跳持久化 (.omo/state/daemon/service_state.json).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Optional

from omo.resident import WORKSPACE, write_path

logger = logging.getLogger("omo.resident.service_runner")

STATE_FILE = WORKSPACE / ".omo/state/daemon/service_state.json"
PID_FILE = WORKSPACE / ".omo/state/daemon/resident.pid"


@dataclass
class ServiceState:
    pid: int
    started_at: str
    last_heartbeat: str
    last_patrol_at: str = ""
    last_night_patrol_at: str = ""
    last_briefing_at: str = ""
    patrol_count: int = 0
    night_patrol_count: int = 0
    briefing_count: int = 0
    status: str = "running"


class ResidentServiceRunner:
    """系统守护进程主执行器."""

    def __init__(self, workspace_root: Path | None = None) -> None:
        self.workspace = workspace_root or WORKSPACE
        self.state_file = write_path(self.workspace / ".omo/state/daemon/service_state.json")
        self.pid_file = write_path(self.workspace / ".omo/state/daemon/resident.pid")
        self._running = False
        self.state = self._init_state()

    def _init_state(self) -> ServiceState:
        now_iso = datetime.now(UTC).isoformat()
        if self.state_file.is_file():
            try:
                data = json.loads(self.state_file.read_text(encoding="utf-8"))
                return ServiceState(
                    pid=os.getpid(),
                    started_at=data.get("started_at", now_iso),
                    last_heartbeat=now_iso,
                    last_patrol_at=data.get("last_patrol_at", ""),
                    last_night_patrol_at=data.get("last_night_patrol_at", ""),
                    last_briefing_at=data.get("last_briefing_at", ""),
                    patrol_count=data.get("patrol_count", 0),
                    night_patrol_count=data.get("night_patrol_count", 0),
                    briefing_count=data.get("briefing_count", 0),
                    status="running",
                )
            except Exception:
                pass
        return ServiceState(
            pid=os.getpid(),
            started_at=now_iso,
            last_heartbeat=now_iso,
        )

    def save_state(self) -> None:
        """持久化当前守护进程运行态快照."""
        try:
            self.state.last_heartbeat = datetime.now(UTC).isoformat()
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            self.state_file.write_text(json.dumps(asdict(self.state), ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.debug("Failed saving service state: %s", exc)

    def run_patrol_job(self) -> dict[str, Any]:
        """执行日间轻量巡检."""
        logger.info("Executing scheduled day patrol...")
        try:
            from omo.resident.patrol import patrol_scan

            res = patrol_scan(auto_remediate=True, workspace_root=self.workspace)
            self.state.last_patrol_at = datetime.now(UTC).isoformat()
            self.state.patrol_count += 1
            self.save_state()
            return res
        except Exception as exc:
            logger.error("Day patrol failed: %s", exc)
            return {"ok": False, "error": str(exc)}

    def run_night_patrol_job(self, in_subprocess: bool = True) -> dict[str, Any]:
        """执行深夜无人体检与自结晶 (默认派生子进程以保证宿主内存纯净)."""
        logger.info("Executing scheduled night patrol (in_subprocess=%s)...", in_subprocess)
        if in_subprocess:
            cmd = [
                sys.executable,
                "-m",
                "omo.resident.service_runner",
                "--run-job",
                "night",
            ]
            env = os.environ.copy()
            env["PYTHONPATH"] = str(self.workspace / "projects/omo/src")
            try:
                proc = subprocess.run(
                    cmd, env=env, cwd=str(self.workspace), capture_output=True, text=True, timeout=300
                )
                logger.info("Night patrol subprocess finished with code %d", proc.returncode)
                self.state.last_night_patrol_at = datetime.now(UTC).isoformat()
                self.state.night_patrol_count += 1
                self.save_state()
                return {"ok": proc.returncode == 0, "stdout": proc.stdout[-500:], "stderr": proc.stderr[-200:]}
            except Exception as exc:
                logger.error("Night patrol subprocess error: %s", exc)
                return {"ok": False, "error": str(exc)}
        else:
            from omo.resident.night_patrol import run_night_patrol

            res = run_night_patrol(force=True, deep=False)
            self.state.last_night_patrol_at = datetime.now(UTC).isoformat()
            self.state.night_patrol_count += 1
            self.save_state()
            return res

    def run_briefing_job(self) -> dict[str, Any]:
        """组装晨间自治简报并主动触达."""
        logger.info("Executing morning briefing generation and dispatch...")
        try:
            from omo.resident.morning_briefing import get_morning_briefing_generator

            gen = get_morning_briefing_generator(self.workspace)
            res = gen.generate_and_save()
            self.state.last_briefing_at = datetime.now(UTC).isoformat()
            self.state.briefing_count += 1
            self.save_state()
            return res
        except Exception as exc:
            logger.error("Morning briefing generation failed: %s", exc)
            return {"ok": False, "error": str(exc)}

    def run_single_job(self, job_name: str) -> dict[str, Any]:
        """单次即刻执行指定任务."""
        if job_name == "patrol":
            return self.run_patrol_job()
        elif job_name == "night":
            return self.run_night_patrol_job(in_subprocess=False)
        elif job_name == "briefing":
            return self.run_briefing_job()
        else:
            return {"ok": False, "error": f"Unknown job name: {job_name}"}

    def start_loop(self) -> None:
        """常驻守护进程主循环."""
        self._running = True
        logger.info("Resident Service Daemon started (PID: %d)...", os.getpid())

        # 写入 PID 文件
        self.pid_file.parent.mkdir(parents=True, exist_ok=True)
        self.pid_file.write_text(str(os.getpid()), encoding="utf-8")

        def _handle_exit(signum, frame):
            logger.info("Received termination signal %s, shutting down...", signum)
            self._running = False

        signal.signal(signal.SIGTERM, _handle_exit)
        signal.signal(signal.SIGINT, _handle_exit)

        last_patrol_tick = time.time()
        last_night_date = ""
        last_briefing_date = ""

        # 启动后先执行一次初始轻量巡检
        self.run_patrol_job()

        while self._running:
            now_dt = datetime.now()
            today_str = now_dt.strftime("%Y-%m-%d")
            now_ts = time.time()

            # 1. 检查日间定时巡检 (每 30 分钟)
            if now_ts - last_patrol_tick >= 1800:
                self.run_patrol_job()
                last_patrol_tick = now_ts

            # 2. 检查凌晨 03:00 深度体检
            if now_dt.hour == 3 and 0 <= now_dt.minute <= 5 and last_night_date != today_str:
                self.run_night_patrol_job(in_subprocess=True)
                last_night_date = today_str

            # 3. 检查早晨 08:30 晨报投递
            if now_dt.hour == 8 and 30 <= now_dt.minute <= 35 and last_briefing_date != today_str:
                self.run_briefing_job()
                last_briefing_date = today_str

            # 4. 心跳持久化
            self.save_state()

            # 每 60 秒轮询一次时钟
            for _ in range(60):
                if not self._running:
                    break
                time.sleep(1)

        self.state.status = "stopped"
        self.save_state()
        if self.pid_file.is_file():
            self.pid_file.unlink(missing_ok=True)
        logger.info("Resident Service Daemon stopped cleanly.")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="omostation resident service runner")
    parser.add_argument("--run-job", choices=["patrol", "night", "briefing"], help="Run single job and exit")
    parser.add_argument("--daemon", action="store_true", help="Run long-lived loop")

    args = parser.parse_args()
    runner = ResidentServiceRunner()

    if args.run_job:
        res = runner.run_single_job(args.run_job)
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        runner.start_loop()


if __name__ == "__main__":
    main()
