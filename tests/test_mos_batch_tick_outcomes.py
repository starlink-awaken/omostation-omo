"""record_tick_outcomes 与逐条记录结果一致, 但状态文件只写一次 (agent-tick 每轮 ~2 分钟 CPU 的根因)."""

from __future__ import annotations

from pathlib import Path

import pytest

from omo import omo_belief
from omo.omo_belief import MOSBeliefManager

CALS = [
    {"capability_ref": "agent:a:tick:noop", "success_rate": 1.0},
    {"capability_ref": "agent:b:tick:alert", "success_rate": 0.0, "avg_latency_ms": 3.0, "sample_size": 2},
]
EXPS = [{"agent_id": "b", "experience": "tick:alert on b", "outcome": "negative"}]


def _strip(entries: list[dict]) -> list[dict]:
    return [{k: v for k, v in e.items() if k not in ("recorded_at", "calibrated_at", "created_at")} for e in entries]


def test_batch_matches_sequential_with_single_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seq = MOSBeliefManager(root=tmp_path / "seq", registry_file=tmp_path / "seq-reg.yaml")
    for c in CALS:
        seq.record_capability_calibration(**c)
    for e in EXPS:
        seq.record_experience(**e)

    writes: list[Path] = []
    real_write = omo_belief.write_yaml_atomic
    monkeypatch.setattr(omo_belief, "write_yaml_atomic", lambda p, d: (writes.append(p), real_write(p, d)))
    batch = MOSBeliefManager(root=tmp_path / "batch", registry_file=tmp_path / "batch-reg.yaml")
    ids = batch.record_tick_outcomes(CALS, EXPS)

    assert ids == ["cc-0001", "cc-0002", "exp-0001"]
    assert writes.count(batch.state_file) == 1
    s, b = seq._load_state(), batch._load_state()
    assert _strip(b["capability_calibrations"]) == _strip(s["capability_calibrations"])
    assert _strip(b["agent_experiences"]) == _strip(s["agent_experiences"])


def test_batch_empty_is_noop(tmp_path: Path) -> None:
    m = MOSBeliefManager(root=tmp_path, registry_file=tmp_path / "reg.yaml")
    assert m.record_tick_outcomes([], []) == []
    assert not m.state_file.exists()
