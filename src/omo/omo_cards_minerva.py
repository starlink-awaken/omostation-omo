#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


from .omo_cards import _get_db, _now, _record_history


def cmd_minerva_ingest(args):
    """Bridge: trigger Minerva deep research for a CARDS research card, store results, advance status."""
    conn = _get_db()
    row = conn.execute("SELECT * FROM cards WHERE id = ?", (args.id,)).fetchone()
    if not row:
        conn.close()
        print(f"❌ Card not found: {args.id}")
        return 1
    if row["type"] != "research":
        conn.close()
        print(f"❌ Card {args.id} is type '{row['type']}', not 'research'")
        return 1

    question = row["title"]
    if row["summary"]:
        question = f"{row['title']}: {row['summary']}"

    level = getattr(args, "level", "L1") or "L1"
    kairon_dir = str(Path(__file__).resolve().parents[4] / "projects" / "kairon")

    print(f"🔬 Minerva research: {question[:80]}...")
    print(f"   Level: {level}  |  Card: {args.id}")

    # Call Minerva
    try:
        result = subprocess.run(
            [
                "uv",
                "--directory",
                kairon_dir,
                "run",
                "minerva",
                "research",
                question,
                "--level",
                level,
                "--json",
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            print(f"⚠️  Minerva exited with code {result.returncode}")
            print(f"   stderr: {result.stderr[:200]}")
            # Still save partial output
            minerva_output = result.stdout or result.stderr
        else:
            minerva_output = result.stdout
    except subprocess.TimeoutExpired:
        print("⚠️  Minerva timed out (5min). Saving partial results.")
        minerva_output = "(Minerva timed out)"
    except FileNotFoundError:
        print("❌ Minerva not found. Ensure kairon project is installed.")
        conn.close()
        return 1

    # Build enriched content
    old_content = row["content"] or ""
    new_content = f"""{old_content}

## Minerva 研究报告 (L{level})
> 自动生成于 {_now()}

{minerva_output[:5000] if len(minerva_output) > 5000 else minerva_output}
"""
    # Update card: store minerva output, advance to digest
    old_status = row["status"]
    new_status = "digest"

    extra = json.loads(row["extra"] or "{}")
    extra["minerva_level"] = level
    extra["minerva_generated_at"] = _now()

    conn.execute(
        "UPDATE cards SET status=?, content=?, extra=?, updated_at=? WHERE id=?",
        (new_status, new_content, json.dumps(extra), _now(), args.id),
    )
    _record_history(conn, args.id, old_status, new_status, f"minerva-ingest (L{level})")
    conn.commit()
    conn.close()
    print(f"✅ {args.id}: {old_status} → {new_status} (Minerva L{level} complete)")
    return 0
