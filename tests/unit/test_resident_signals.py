"""Unit tests for omo.resident.signals — personal-signals poll + idempotent watermark.

M2.1b: 验证个人文件渠道:
- 新文件发布 + 水位记录 (content-digest 幂等)
- 二次运行不重复发布
- 文件内容变化 → 重新发布
- dry-run 不写水位
"""

from __future__ import annotations

from pathlib import Path

import pytest

from omo.resident import signals


@pytest.fixture(autouse=True)
def _isolate_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(signals, "WATERMARK_FILE", tmp_path / "personal-signals" / "watermark.json")


@pytest.fixture
def signals_dir(tmp_path: Path) -> Path:
    d = tmp_path / "signals"
    d.mkdir()
    return d


def test_poll_new_file_dry_run(signals_dir: Path) -> None:
    (signals_dir / "idea.md").write_text("# idea", encoding="utf-8")
    report = signals.poll(signals_dir=signals_dir, dry_run=True)
    assert report["scanned"] == 1
    assert report["new"] == 1
    assert report["published"] == 0
    # dry-run 不写水位 → 下次仍视为新
    assert not signals.WATERMARK_FILE.exists()


def test_poll_publish_and_watermark(signals_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (signals_dir / "idea.md").write_text("# idea", encoding="utf-8")
    published: list[tuple] = []

    def fake_publish(topic: str, payload: dict, trace_id: str) -> bool:
        published.append((topic, payload, trace_id))
        return True

    monkeypatch.setattr(signals, "_publish", fake_publish)
    report = signals.poll(signals_dir=signals_dir, dry_run=False)
    assert report["published"] == 1
    assert len(published) == 1
    topic, payload, trace_id = published[0]
    assert topic == "mesh:personal:signal"
    assert payload["source"] == "personal-signals"
    assert payload["content_digest"].startswith("sha256:")
    assert trace_id == "personal-signal:idea"
    # 水位已写
    assert signals.WATERMARK_FILE.is_file()


def test_poll_idempotent_second_run(signals_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (signals_dir / "idea.md").write_text("# idea", encoding="utf-8")
    monkeypatch.setattr(signals, "_publish", lambda topic, payload, trace_id: True)
    signals.poll(signals_dir=signals_dir, dry_run=False)
    second = signals.poll(signals_dir=signals_dir, dry_run=False)
    assert second["new"] == 0
    assert second["published"] == 0


def test_poll_content_change_republishes(signals_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    f = signals_dir / "idea.md"
    f.write_text("# idea", encoding="utf-8")
    monkeypatch.setattr(signals, "_publish", lambda topic, payload, trace_id: True)
    signals.poll(signals_dir=signals_dir, dry_run=False)
    f.write_text("# idea v2 — content changed", encoding="utf-8")
    second = signals.poll(signals_dir=signals_dir, dry_run=False)
    assert second["new"] == 1
    assert second["published"] == 1


def test_poll_publish_failure_still_watermarks(signals_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (signals_dir / "idea.md").write_text("# idea", encoding="utf-8")
    monkeypatch.setattr(signals, "_publish", lambda topic, payload, trace_id: False)
    report = signals.poll(signals_dir=signals_dir, dry_run=False)
    assert report["published"] == 0
    # 失败也记录水位 (避免无限重试同文件; 内容变化会重新触发)
    assert signals.WATERMARK_FILE.is_file()
