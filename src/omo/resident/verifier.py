#!/usr/bin/env python3
"""Verifier — Agent Cell 验证者. 结果验证 → 质量评估 → 裁决."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]


class Verifier:
    def __init__(self):
        self.verdict_log = []

    def verify(self, execution_result: dict, intent: dict | None = None) -> dict:
        results = execution_result.get("results", [])
        completeness = self._check_completeness(results)
        correctness = self._check_correctness(results)
        quality = self._assess_quality(results)
        score = (completeness["score"] + correctness["score"] + quality["score"]) / 3
        verdict = "accept" if score >= 0.7 else "revise" if score >= 0.4 else "reject"
        v = {
            "schema": "verdict/v1",
            "verdict_id": f"verdict-{uuid.uuid4().hex[:12]}",
            "execution_id": execution_result.get("execution_id", ""),
            "verdict": verdict,
            "score": round(score, 2),
            "details": {"completeness": completeness, "correctness": correctness, "quality": quality},
            "timestamp": datetime.now(UTC).isoformat(),
        }
        self.verdict_log.append(v)
        return v

    def _check_completeness(self, results: list) -> dict:
        if not results:
            return {"score": 0.0, "message": "No results"}
        completed = sum(1 for r in results if r.get("ok"))
        return {"score": completed / len(results), "completed": completed, "total": len(results)}

    def _check_correctness(self, results: list) -> dict:
        if not results:
            return {"score": 0.0, "message": "No results"}
        valid = sum(
            1 for r in results if r.get("ok") and r.get("output") and "error" not in str(r.get("output", "")).lower()
        )
        return {"score": valid / len(results), "valid": valid, "total": len(results)}

    def _assess_quality(self, results: list) -> dict:
        if not results:
            return {"score": 0.0, "message": "No results"}
        scores = []
        for r in results:
            if not r.get("ok"):
                scores.append(0.0)
                continue
            s = 0.5
            if len(str(r.get("output", ""))) > 100:
                s += 0.3
            if isinstance(r.get("output"), (list, dict)):
                s += 0.2
            scores.append(min(1.0, s))
        return {"score": sum(scores) / len(scores)}

    def quick_check(self, output: Any, expected: Any = None) -> dict:
        result = {"ok": True, "score": 0.0, "checks": []}
        if not output:
            result["ok"] = False
            return result
        result["score"] += 0.3
        if expected is not None and type(output) == type(expected):
            result["score"] += 0.3
        if len(str(output)) > 50:
            result["score"] += 0.2
        if "error" not in str(output).lower():
            result["score"] += 0.2
        else:
            result["ok"] = False
        result["score"] = round(result["score"], 2)
        return result


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("--result")
    parser.add_argument("--check")
    args = parser.parse_args()
    v = Verifier()
    if args.result:
        r = v.verify(json.loads(args.result))
        print(json.dumps(r, ensure_ascii=False, indent=2))
    elif args.check:
        r = v.quick_check(json.loads(args.check).get("output"))
        print(json.dumps(r, ensure_ascii=False, indent=2))
