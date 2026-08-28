#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .omo_cards import _get_db, _now, _record_history


def cmd_migrate(args):
    """Import v1 CARDS/*.md files into SQLite."""
    cards_dir = Path(args.source)
    if not cards_dir.exists():
        print(f"❌ Source directory not found: {cards_dir}")
        return 1

    conn = _get_db()
    imported = 0
    skipped = 0

    for md_file in sorted(cards_dir.rglob("*.md")):
        if md_file.name == "README.md":
            continue

        text = md_file.read_text(encoding="utf-8")

        # Parse YAML frontmatter
        if text.startswith("---"):
            parts = text.split("---", 2)
            if len(parts) >= 3:
                frontmatter_text = parts[1]
                body = parts[2].strip()
            else:
                print(f"⚠️  Skipping {md_file.name}: malformed frontmatter")
                skipped += 1
                continue
        else:
            print(f"⚠️  Skipping {md_file.name}: no frontmatter")
            skipped += 1
            continue

        # Simple YAML parsing (flat key: value only)
        fm = {}
        for line in frontmatter_text.strip().split("\n"):
            line = line.strip()
            if ":" in line:
                key, _, val = line.partition(":")
                fm[key.strip()] = val.strip()

        card_id = fm.get("id", "")
        if not card_id:
            print(f"⚠️  Skipping {md_file.name}: no id")
            skipped += 1
            continue

        # Check if exists
        existing = conn.execute("SELECT id FROM cards WHERE id = ?", (card_id,)).fetchone()
        if existing:
            print(f"⏭  Skipping {card_id}: already exists")
            skipped += 1
            continue

        now = _now()
        tags = json.dumps([t.strip() for t in fm.get("tags", "").strip("[]").split(",") if t.strip()])
        extra = json.dumps(
            {
                "severity": fm.get("severity", ""),
                "task_type": fm.get("task_type", ""),
            }
            if fm.get("severity") or fm.get("task_type")
            else {}
        )

        conn.execute(
            """INSERT INTO cards (id, type, status, title, domain, priority, summary, content, parent_id, created_at, updated_at, deadline, review_due, tags, extra)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                card_id,
                fm.get("type", "task"),
                fm.get("status", "planned"),
                fm.get("title", md_file.stem),
                fm.get("domain", "meta"),
                fm.get("priority", "P2"),
                fm.get("summary", ""),
                body,
                fm.get("parent") or None,
                fm.get("created", now),
                now,
                fm.get("deadline") or None,
                fm.get("review_due") or None,
                tags,
                extra,
            ),
        )
        _record_history(conn, card_id, None, fm.get("status", "planned"), "imported from v1")
        imported += 1
        print(f"✅ {card_id}: {fm.get('title', '')}")

    conn.commit()
    conn.close()
    print(f"\n📦 Imported {imported} cards, skipped {skipped}")
    return 0


# ── main ────────────────────────────────────────────────
