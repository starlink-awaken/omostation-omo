#!/usr/bin/env python3
"""projects/omo/tests/unit/test_resident_phase8.py — Phase 8 记忆自结晶与晨间简报单元测试."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from omo.resident.memory_distiller import MemoryDistiller
from omo.resident.morning_briefing import MorningBriefingGenerator
from omo.resident.night_patrol import run_night_patrol


class TestResidentPhase8(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tmp_dir.name)

        # 构造模拟路径
        (self.workspace / ".omo/state/memory/candidates").mkdir(parents=True)
        (self.workspace / "runtime/dashboard").mkdir(parents=True)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_memory_distiller_scan_and_crystallize(self):
        # 1. 创建候选记忆文件
        cand_file = self.workspace / ".omo/state/memory/candidates/cand-test-1.json"
        cand_file.write_text(
            json.dumps(
                {
                    "id": "cand-test-1",
                    "category": "user_preference",
                    "title": "代码格式偏好",
                    "summary": "优先采用单文件紧凑实现",
                    "created_at": "2026-10-17T12:00:00Z",
                }
            ),
            encoding="utf-8",
        )

        distiller = MemoryDistiller(workspace_root=self.workspace)
        cryst = distiller.distill_and_crystallize(extra_text="注意：禁跑编译等CPU重活，回复优先使用中文")

        self.assertIn("updated_at", cryst)
        self.assertGreaterEqual(cryst["total_episodes_processed"], 2)

        # 检查提炼出的偏好条目
        directives = [p["directive"] for p in cryst["preferences"]]
        self.assertTrue(any("优先采用单文件紧凑实现" in d for d in directives))
        self.assertTrue(any("硬件护温偏好" in d for d in directives))

        # 检查持久化文件
        cryst_file = self.workspace / ".omo/state/memory/distilled_crystallization.json"
        self.assertTrue(cryst_file.exists())

        # 检查冷启动直接读取
        loaded = distiller.load_crystallized_memory()
        self.assertEqual(len(loaded["preferences"]), len(cryst["preferences"]))

    def test_morning_briefing_assembly_and_save(self):
        gen = MorningBriefingGenerator(workspace_root=self.workspace)

        patrol_mock = {
            "ok": True,
            "commits_reviewed": 5,
            "duration_seconds": 1.2,
        }

        briefing = gen.generate_and_save(patrol_report=patrol_mock)

        self.assertIn("greeting", briefing)
        self.assertIn("yesterday_summary", briefing)
        self.assertIn("夜间深度审查完成", briefing["night_defense_status"])
        self.assertGreaterEqual(len(briefing["top_action_items"]), 2)

        # 验证文件输出
        json_file = self.workspace / "runtime/dashboard/morning-briefing.json"
        md_file = self.workspace / "runtime/dashboard/morning-briefing.md"
        self.assertTrue(json_file.exists())
        self.assertTrue(md_file.exists())

        md_text = md_file.read_text(encoding="utf-8")
        self.assertIn("# 🌅 晨间自治与系统运行简报", md_text)
        self.assertIn("今日建议优先待办", md_text)

    @patch("omo.resident.night_patrol.check_power_and_idle")
    def test_night_patrol_integration_with_briefing(self, mock_power):
        mock_power.return_value = (True, "AC 电源接入 (模拟)")

        report = run_night_patrol(force=True, deep=False)
        self.assertTrue(report.get("ok"))
        self.assertTrue(report.get("morning_briefing_generated"))
        self.assertIn("memory_crystallization", report)


if __name__ == "__main__":
    unittest.main()
