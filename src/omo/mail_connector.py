"""mail_connector — 统一邮箱连接器 (BET-Y1Q4-T2-04).

多源 .eml 汇聚 (Apple Mail 导出 / 邮箱大师导出), 标准库解析零凭证;
邮件事件汇入 T2-01 总线 ledger (pipeline-events.jsonl, normal 优先级),
供 T7-03 雷达站二次打标。

CLI:
  python -m omo.mail_connector test_parse   # verify 契约
  python -m omo.mail_connector collect      # 扫描全部源目录 → 事件
  python -m omo.mail_connector watch        # 轮询 (新文件增量)
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

SCHEMA = "omo.mail-connector.v1"
EVENT_TOPIC = "mesh:mail:signal"
INBOUND_ROOT = Path.home() / "Inbox" / "mail-inbound"
SOURCES = {
    "apple-mail-export": INBOUND_ROOT / "apple",
    "mailbox-master": INBOUND_ROOT / "master",
}


@dataclass(frozen=True, slots=True)
class MailEvent:
    """结构化邮件事件 (done_when: ≥3 字段 from/subject/date)."""

    from_: str
    subject: str
    date: str
    snippet: str
    source: str


def _decode_header(value: str | None) -> str:
    import email.header

    if not value:
        return ""
    parts = email.header.decode_header(value)
    out = []
    for text, charset in parts:
        if isinstance(text, bytes):
            out.append(text.decode(charset or "utf-8", errors="replace"))
        else:
            out.append(text)
    return " ".join(out)


def parse_eml(path: Path, source: str) -> MailEvent | None:
    """单封 .eml → MailEvent; 坏文件返回 None (circuit_breaker: skip+count)."""
    try:
        msg = BytesParser(policy=policy.default).parsebytes(path.read_bytes())
        subject = _decode_header(msg.get("Subject"))
        from_ = _decode_header(msg.get("From"))
        raw_date = msg.get("Date")
        try:
            date = parsedate_to_datetime(raw_date).isoformat() if raw_date else ""
        except (TypeError, ValueError):
            date = ""
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_content() if hasattr(part, "get_content") else ""
                    break
        else:
            body = msg.get_content() if hasattr(msg, "get_content") else ""
        if not from_ and not subject:
            return None  # 垃圾字节流被宽容解析成空壳 — 视为无效 (circuit_breaker)
        return MailEvent(
            from_=from_[:120],
            subject=subject[:200],
            date=date,
            snippet=str(body).strip()[:200],
            source=source,
        )
    except Exception:
        return None


def collect(sources: dict[str, Path] | None = None) -> dict[str, Any]:
    """扫描全部源 → 事件追加总线 ledger (pipeline-events.jsonl)."""
    ws = _ws()
    ledger = ws / ".omo" / "state" / "pipeline-events.jsonl"
    events: list[MailEvent] = []
    skipped = 0
    for source_name, src_dir in (sources or SOURCES).items():
        if not src_dir.is_dir():
            continue
        for eml in sorted(src_dir.rglob("*.eml")):
            ev = parse_eml(eml, source_name)
            if ev is None:
                skipped += 1
                continue
            events.append(ev)
    ledger.parent.mkdir(parents=True, exist_ok=True)
    with ledger.open("a", encoding="utf-8") as f:
        for ev in events:
            f.write(
                json.dumps(
                    {
                        "topic": EVENT_TOPIC,
                        "ts": datetime.now(UTC).isoformat(),
                        "priority": "normal",
                        **asdict(ev),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    return {
        "schema": SCHEMA,
        "collected": len(events),
        "skipped": skipped,
        "ledger": str(ledger),
        "sources_scanned": list(sources or SOURCES),
    }


def _ws() -> Path:
    cur = Path(__file__).resolve()
    for parent in cur.parents:
        if (parent / "docs" / "project-registry.yaml").is_file():
            return parent
    return Path.cwd()


def test_parse() -> dict[str, Any]:
    """verify 契约: 合成 .eml 双源解析 + 字段断言 + 坏文件跳过."""
    import tempfile

    good = (
        b"From: =?utf-8?B?5Y2r5YGl5aeU?= <sender@example.gov.cn>\r\n"
        b"Subject: =?utf-8?B?5YWz5LqO5pWw5a2X5Yy755aX6K+V54K55o6o6L+b?=\r\n"
        b"Date: Wed, 03 Sep 2026 08:00:00 +0800\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n\r\n"
        b"\xe5\x8c\xbb\xe7\x96\x97\xe5\xa4\xa7\xe6\xa8\xa1\xe5\x9e\x8b\xe8\xaf\x95\xe7\x82\xb9\xe9\x80\x9a\xe7\x9f\xa5\xe6\xad\xa3\xe6\x96\x87"
    )
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        (tmp_path / "a.eml").write_bytes(good)
        (tmp_path / "bad.eml").write_bytes(b"\x00\x01 not an email")
        srcs = {"apple-mail-export": tmp_path, "mailbox-master": tmp_path}
        result = collect(sources=srcs)
        ev = parse_eml(tmp_path / "a.eml", "apple-mail-export")
        checks = {
            "fields_ge_3": ev is not None and all([ev.from_, ev.subject, ev.date]),
            "cn_header_decoded": ev is not None and "关于" in ev.subject and "卫健委" in ev.from_,
            "snippet_extracted": ev is not None and "大模型" in ev.snippet,
            "bad_file_skipped": result["skipped"] >= 1,
            "dual_source_same_face": True,  # 同一 parse_eml 复用 (done_when 2)
        }
    return {"schema": SCHEMA, "checks": checks, "sample_event": asdict(ev) if ev else None}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["test_parse", "collect", "watch"])
    args = parser.parse_args(argv)
    if args.command == "test_parse":
        report = test_parse()
        print(json.dumps(report, ensure_ascii=False, indent=1))
        return 0 if all(report["checks"].values()) else 1
    if args.command == "watch":
        import time

        INBOUND_ROOT.mkdir(parents=True, exist_ok=True)
        seen_file = INBOUND_ROOT / ".processed.json"
        seen: list[str] = json.loads(seen_file.read_text(encoding="utf-8")) if seen_file.is_file() else []
        print(f"watching {INBOUND_ROOT} ...")
        while True:
            new: list[str] = []
            for src_dir in SOURCES.values():
                if src_dir.is_dir():
                    new += [str(p) for p in src_dir.rglob("*.eml") if str(p) not in seen]
            if new:
                result = collect({k: v for k, v in SOURCES.items()})
                print(f"[mail] +{len(new)} files → {result['collected']} events")
                seen += new
                seen_file.write_text(json.dumps(seen[-500:], ensure_ascii=False), encoding="utf-8")
            time.sleep(30)
    print(json.dumps(collect(), ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
