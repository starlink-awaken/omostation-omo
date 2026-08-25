"""Unit tests for omo.resident.inbox — perception-inbox poll + idempotent watermark.

T10-15: 验证感知文件夹渠道 (BET-Y1Q3-T10-15):
- 新文件发布 + 水位记录 (content-digest 幂等)
- 二次运行不重复发布
- 文件内容变化 → 重新发布
- dry-run 不写水位
- 事件追加到统一事件流 (InboxSignal 事件)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omo.resident import inbox


@pytest.fixture(autouse=True)
def _isolate_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(inbox, "WATERMARK_FILE", tmp_path / "perception-inbox" / "watermark.json")
    monkeypatch.setattr(inbox, "EVENTS_JSONL", tmp_path / "workflow-mesh" / "events.jsonl")


@pytest.fixture
def inbox_dir(tmp_path: Path) -> Path:
    d = tmp_path / "inbox"
    d.mkdir()
    return d


def test_poll_new_file_dry_run(inbox_dir: Path) -> None:
    (inbox_dir / "research-2026-08-19-001.md").write_text("# 调研", encoding="utf-8")
    report = inbox.poll(inbox_dir=inbox_dir, dry_run=True)
    assert report["scanned"] == 1
    assert report["new"] == 1
    assert report["published"] == 0
    # dry-run 不写水位 → 下次仍视为新
    assert not inbox.WATERMARK_FILE.exists()


def test_poll_publish_and_watermark(inbox_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (inbox_dir / "research-2026-08-19-001.md").write_text("# 调研", encoding="utf-8")
    published: list[tuple] = []

    def fake_publish(topic: str, payload: dict, trace_id: str) -> bool:
        published.append((topic, payload, trace_id))
        return True

    monkeypatch.setattr(inbox, "_publish", fake_publish)
    report = inbox.poll(inbox_dir=inbox_dir, dry_run=False)
    assert report["published"] == 1
    assert len(published) == 1
    topic, payload, trace_id = published[0]
    assert topic == "mesh:perception:inbox"
    assert payload["source"] == "perception-inbox"
    assert payload["content_digest"].startswith("sha256:")
    assert trace_id == "inbox:research-2026-08-19-001"
    # 水位已写
    assert inbox.WATERMARK_FILE.is_file()


def test_poll_appends_inbox_signal_event_to_events_jsonl(inbox_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """感知信号发布后追加到统一事件流 (InboxSignal 事件)."""
    (inbox_dir / "research-2026-08-19-001.md").write_text("# 调研", encoding="utf-8")
    monkeypatch.setattr(inbox, "_publish", lambda topic, payload, trace_id: True)
    inbox.poll(inbox_dir=inbox_dir, dry_run=False)

    assert inbox.EVENTS_JSONL.is_file()
    lines = inbox.EVENTS_JSONL.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["event_type"] == "InboxSignal"
    assert event["producer"] == "perception-inbox"
    assert event["payload"]["file"] == "research-2026-08-19-001.md"
    assert event["idempotency_key"] == "inbox:research-2026-08-19-001:InboxSignal"


def test_poll_idempotent_second_run(inbox_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (inbox_dir / "research-2026-08-19-001.md").write_text("# 调研", encoding="utf-8")
    monkeypatch.setattr(inbox, "_publish", lambda topic, payload, trace_id: True)
    inbox.poll(inbox_dir=inbox_dir, dry_run=False)
    second = inbox.poll(inbox_dir=inbox_dir, dry_run=False)
    assert second["new"] == 0
    assert second["published"] == 0


def test_poll_content_change_republishes(inbox_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    f = inbox_dir / "research-2026-08-19-001.md"
    f.write_text("# 调研", encoding="utf-8")
    monkeypatch.setattr(inbox, "_publish", lambda topic, payload, trace_id: True)
    inbox.poll(inbox_dir=inbox_dir, dry_run=False)
    f.write_text("# 调研 v2 — 内容更新", encoding="utf-8")
    second = inbox.poll(inbox_dir=inbox_dir, dry_run=False)
    assert second["new"] == 1
    assert second["published"] == 1


def test_poll_publish_failure_still_watermarks(inbox_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (inbox_dir / "research-2026-08-19-001.md").write_text("# 调研", encoding="utf-8")
    monkeypatch.setattr(inbox, "_publish", lambda topic, payload, trace_id: False)
    report = inbox.poll(inbox_dir=inbox_dir, dry_run=False)
    assert report["published"] == 0
    # 失败也记录水位 (避免无限重试同文件; 内容变化会重新触发)
    assert inbox.WATERMARK_FILE.is_file()
