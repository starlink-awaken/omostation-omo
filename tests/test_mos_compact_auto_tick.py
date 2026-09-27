"""compact_auto_tick: 心跳类记录滚动压缩, 不丢样本计数、不动真实场景记录、不改"最近 N 条"语义。"""

from __future__ import annotations

from pathlib import Path

from omo.omo_belief import MOSBeliefManager, _next_id, compact_auto_tick


def _cal(i: int, ref: str, rate: float = 1.0, n: int = 1) -> dict:
    return {
        "id": f"cc-{i:04d}",
        "capability_ref": ref,
        "measured_at": f"t{i:05d}",
        "success_rate": rate,
        "avg_latency_ms": 0.0,
        "sample_size": n,
        "last_run_id": None,
    }


def _state(cals: list[dict], exps: list[dict] | None = None) -> dict:
    return {"capability_calibrations": cals, "agent_experiences": exps or []}


def test_keeps_recent_raw_and_rolls_up_older_without_losing_samples() -> None:
    ref = "agent:governor:tick:alert"
    cals = [_cal(i, ref, rate=0.0 if i % 4 == 0 else 1.0) for i in range(1, 121)]
    total_success = sum(c["success_rate"] for c in cals)
    st = _state(cals)

    out = compact_auto_tick(st, keep=50)

    kept = st["capability_calibrations"]
    assert out["calibrations_removed"] == 69  # 120 → 50 原始 + 1 rollup
    rollup, raw = kept[0], kept[1:]
    assert rollup["rollup"] and rollup["sample_size"] == 70
    assert [c["id"] for c in raw] == [f"cc-{i:04d}" for i in range(71, 121)]
    assert abs(rollup["success_rate"] * 70 + sum(c["success_rate"] for c in raw) - total_success) < 1e-6
    assert rollup["rollup_since"] == "t00001" and rollup["measured_at"] == "t00070"


def test_scene_calibrations_untouched_and_latest_unchanged() -> None:
    cals = [_cal(i, "agent:a:tick:noop") for i in range(1, 80)]
    cals.insert(10, _cal(900, "scene:scene-admin-classify", rate=0.5))
    cals.append(_cal(999, "scene:scene-x", rate=0.2))
    st = _state(cals)

    compact_auto_tick(st, keep=20)

    kept = st["capability_calibrations"]
    assert [c["capability_ref"] for c in kept if c["capability_ref"].startswith("scene:")] == [
        "scene:scene-admin-classify",
        "scene:scene-x",
    ]
    assert kept[-1]["id"] == "cc-0999"  # 取最新一条的读者不受影响


def test_repeated_compaction_accumulates_into_existing_rollup() -> None:
    ref = "agent:a:tick:noop"
    st = _state([_cal(i, ref) for i in range(1, 61)])
    compact_auto_tick(st, keep=50)
    st["capability_calibrations"] += [_cal(i, ref) for i in range(61, 81)]
    compact_auto_tick(st, keep=50)

    rollups = [c for c in st["capability_calibrations"] if c.get("rollup")]
    assert len(rollups) == 1 and rollups[0]["sample_size"] == 30
    assert rollups[0]["rollup_since"] == "t00001"
    assert sum(c.get("sample_size", 1) for c in st["capability_calibrations"]) == 80


def test_experiences_rolled_up_with_outcome_counts() -> None:
    exps = [
        {
            "id": f"exp-{i:04d}",
            "agent_id": "gov",
            "experience": "tick:alert on gov",
            "outcome": "negative" if i % 5 == 0 else "positive",
            "context": "",
            "recorded_at": f"t{i}",
        }
        for i in range(1, 31)
    ]
    exps.append(
        {
            "id": "exp-0099",
            "agent_id": "x",
            "experience": "manual lesson",
            "outcome": "positive",
            "context": "",
            "recorded_at": "t99",
        }
    )
    st = _state([], exps)

    compact_auto_tick(st, keep=10)

    rollup = next(e for e in st["agent_experiences"] if e.get("rollup"))
    assert rollup["count"] == 20 and rollup["outcome_counts"] == {"positive": 16, "negative": 4}
    assert any(e["experience"] == "manual lesson" for e in st["agent_experiences"])


def test_ids_stay_unique_after_compaction(tmp_path: Path) -> None:
    m = MOSBeliefManager(root=tmp_path, registry_file=tmp_path / "reg.yaml")
    for _ in range(3):
        m.record_tick_outcomes([{"capability_ref": "agent:a:tick:noop", "success_rate": 1.0}] * 30)
    ids = [c["id"] for c in m._load_state()["capability_calibrations"]]
    assert len(ids) == len(set(ids))
    assert _next_id(m._load_state()["capability_calibrations"], "cc") == "cc-0091"
