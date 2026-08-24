#!/usr/bin/env python3
"""Memory Pipeline — Agent Cell 记忆整合管道. candidate → conflict → consolidate → forget."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
MEMORY_DIR = ROOT / ".omo/state/agent-cell-memory"
CANDIDATE_FILE = MEMORY_DIR / "candidates.jsonl"
MEMORY_FILE = MEMORY_DIR / "memories.jsonl"
CONFLICT_FILE = MEMORY_DIR / "conflicts.jsonl"


class MemoryPipeline:
    def __init__(self):
        MEMORY_DIR.mkdir(parents=True, exist_ok=True)

    def generate_candidates(self, episode: dict) -> list[dict]:
        candidates = []
        for r in episode.get("results", []):
            if not r.get("ok") or len(str(r.get("output", ""))) < 50:
                continue
            candidates.append(
                {
                    "schema": "memory-candidate/v1",
                    "candidate_id": f"cand-{uuid.uuid4().hex[:12]}",
                    "episode_id": episode.get("episode_id", "?"),
                    "type": "semantic" if r.get("action") in ("read_file", "search") else "procedural",
                    "content": str(r.get("output", ""))[:1000],
                    "confidence": 0.5,
                    "created_at": datetime.now(UTC).isoformat(),
                    "status": "pending",
                }
            )
        if candidates:
            with open(CANDIDATE_FILE, "a") as f:
                for c in candidates:
                    f.write(json.dumps(c, ensure_ascii=False) + "\n")
        return candidates

    def detect_conflicts(self) -> list[dict]:
        candidates = self._load_file(CANDIDATE_FILE)
        conflicts = []
        by_ep = {}
        for c in candidates:
            by_ep.setdefault(c.get("episode_id", ""), []).append(c)
        for ep, cands in by_ep.items():
            if len(cands) >= 2 and len(set(c.get("content", "") for c in cands)) > 1:
                conflicts.append(
                    {
                        "schema": "memory-conflict/v1",
                        "conflict_id": f"conf-{uuid.uuid4().hex[:12]}",
                        "candidates": [c["candidate_id"] for c in cands],
                        "status": "unresolved",
                        "detected_at": datetime.now(UTC).isoformat(),
                    }
                )
        if conflicts:
            with open(CONFLICT_FILE, "a") as f:
                for c in conflicts:
                    f.write(json.dumps(c, ensure_ascii=False) + "\n")
        return conflicts

    def consolidate(self) -> list[dict]:
        candidates = self._load_file(CANDIDATE_FILE)
        by_type = {}
        for c in candidates:
            by_type.setdefault(c.get("type", "semantic"), []).append(c)
        consolidated = []
        for t, cands in by_type.items():
            if len(cands) >= 2:
                consolidated.append(
                    {
                        "schema": "consolidated-memory/v1",
                        "memory_id": f"mem-{uuid.uuid4().hex[:12]}",
                        "type": t,
                        "content": " | ".join(c.get("content", "") for c in cands)[:2000],
                        "source_candidates": [c["candidate_id"] for c in cands],
                        "confidence": min(1.0, sum(c.get("confidence", 0.5) for c in cands) / len(cands)),
                        "created_at": datetime.now(UTC).isoformat(),
                        "access_count": 0,
                    }
                )
        if consolidated:
            with open(MEMORY_FILE, "a") as f:
                for m in consolidated:
                    f.write(json.dumps(m, ensure_ascii=False) + "\n")
        return consolidated

    def forget(self, max_age_days: int = 90, min_access: int = 1) -> list[dict]:
        memories = self._load_file(MEMORY_FILE)
        cutoff = datetime.now(UTC) - timedelta(days=max_age_days)
        forgotten, kept = [], []
        for m in memories:
            created = datetime.fromisoformat(m.get("created_at", "2020-01-01T00:00:00+00:00"))
            if created < cutoff and m.get("access_count", 0) < min_access:
                forgotten.append(m)
            else:
                kept.append(m)
        with open(MEMORY_FILE, "w") as f:
            for m in kept:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")
        return forgotten

    def _load_file(self, path: Path) -> list[dict]:
        if not path.exists():
            return []
        items = []
        with open(path) as f:
            for line in f:
                try:
                    items.append(json.loads(line.strip()))
                except Exception:
                    continue
        return items

    def get_stats(self) -> dict:
        return {
            "candidates": len(self._load_file(CANDIDATE_FILE)),
            "memories": len(self._load_file(MEMORY_FILE)),
            "conflicts": len(self._load_file(CONFLICT_FILE)),
        }


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("--process")
    parser.add_argument("--consolidate", action="store_true")
    parser.add_argument("--forget", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    p = MemoryPipeline()
    if args.process:
        print(json.dumps({"candidates": len(p.generate_candidates(json.loads(args.process)))}, ensure_ascii=False))
    elif args.consolidate:
        print(json.dumps({"consolidated": len(p.consolidate())}, ensure_ascii=False))
    elif args.forget:
        print(json.dumps({"forgotten": len(p.forget())}, ensure_ascii=False))
    elif args.status:
        print(json.dumps(p.get_stats(), ensure_ascii=False, indent=2))
