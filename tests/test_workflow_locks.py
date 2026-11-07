"""Tests for BET-Y1Q1-T1-00: lock heartbeat, stale detection, prune."""

from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

import omo.workflow.lifecycle as lifecycle_mod
from omo.workflow.core import WorkflowError
from omo.workflow.lifecycle import (
    _HEARTBEAT_STALE_SECONDS,
    _classify_existing_lock,
    acquire_locks,
    claim_run,
    closeout_run,
    heartbeat_lock,
    heartbeat_run,
    lock_scope_object_present,
    lock_scopes_past_expiry,
    prune_stale_locks,
    release_locks,
    run_update_lock,
    sanitize_lock_name,
    scan_locks,
)


@pytest.fixture()
def registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    import omo.workflow.core as core_mod

    monkeypatch.setattr(core_mod, "WORKSPACE", tmp_path)
    monkeypatch.setattr(lifecycle_mod, "WORKSPACE", tmp_path)
    return {"runner": {"lock_state_dir": "locks"}}


def _make_lock(lock_dir: Path, name: str, payload: dict) -> Path:
    path = lock_dir / name
    path.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")
    return path


def _lock_dir(registry: dict) -> Path:
    return lifecycle_mod.WORKSPACE / registry["runner"]["lock_state_dir"]


def test_acquire_adds_heartbeat(registry: dict) -> None:
    locks = acquire_locks(registry, ["path:foo.py"], "run-1", "agent-a", False)
    assert len(locks) == 1
    lock_file = _lock_dir(registry) / "path_foo.py.lock.yaml"
    data = yaml.safe_load(lock_file.read_text())
    assert "last_heartbeat" in data
    assert data["run_id"] == "run-1"


def test_run_update_lock_preserves_legacy_legal_filename(registry: dict) -> None:
    run_id = "r" * 239
    lock_file = _lock_dir(registry) / f"run_{run_id}.update.lock"

    with run_update_lock(registry, run_id):
        assert lock_file.exists()
        assert len(lock_file.name.encode("utf-8")) == 255

    assert not lock_file.exists()


def test_acquire_lock_preserves_legacy_legal_filename(registry: dict) -> None:
    scope = "s" * 245
    lock_file = _lock_dir(registry) / f"{scope}.lock.yaml"

    acquire_locks(registry, [scope], "run-legacy", "agent-a", False)

    assert lock_file.exists()
    assert len(lock_file.name.encode("utf-8")) == 255


def test_overlong_run_update_lock_is_bounded_and_released(registry: dict) -> None:
    run_id = "run-" + ("argv-segment-" * 40)

    with run_update_lock(registry, run_id):
        lock_files = list(_lock_dir(registry).glob("*.update.lock"))
        assert len(lock_files) == 1
        assert len(lock_files[0].name.encode("utf-8")) == 255

    assert not list(_lock_dir(registry).glob("*.update.lock"))


def test_overlong_lock_names_are_bounded_unique_and_releasable(registry: dict) -> None:
    run_id = "run-overlong"
    common_prefix = "path:" + ("component/" * 40)
    scopes = [f"{common_prefix}alpha", f"{common_prefix}beta"]

    acquire_locks(registry, scopes, run_id, "agent-a", False)
    lock_files = sorted(_lock_dir(registry).glob("*.lock.yaml"))

    assert len(lock_files) == 2
    assert len({lock_file.name for lock_file in lock_files}) == 2
    assert all(len(lock_file.name.encode("utf-8")) == 255 for lock_file in lock_files)
    for lock_file in lock_files:
        heartbeat_lock(lock_file)

    assert len(release_locks(registry, run_id)) == 2
    assert not list(_lock_dir(registry).glob("*.lock.yaml"))


def test_classify_live_lock(registry: dict) -> None:
    ldir = _lock_dir(registry)
    now = datetime.now(UTC).isoformat()
    _make_lock(
        ldir,
        "test.lock.yaml",
        {
            "run_id": "r1",
            "actor": "a",
            "scope": "s",
            "created_at": now,
            "last_heartbeat": now,
            "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        },
    )
    result = _classify_existing_lock(ldir / "test.lock.yaml")
    assert result["kind"] == "live"


def test_classify_expired_lock(registry: dict) -> None:
    ldir = _lock_dir(registry)
    past = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    _make_lock(
        ldir,
        "test.lock.yaml",
        {
            "run_id": "r1",
            "actor": "a",
            "scope": "s",
            "created_at": past,
            "last_heartbeat": past,
            "expires_at": past,
        },
    )
    result = _classify_existing_lock(ldir / "test.lock.yaml")
    assert result["kind"] == "zombie_expired"


def test_classify_stale_heartbeat(registry: dict) -> None:
    ldir = _lock_dir(registry)
    now = datetime.now(UTC)
    stale_hb = (now - timedelta(seconds=_HEARTBEAT_STALE_SECONDS + 100)).isoformat()
    _make_lock(
        ldir,
        "test.lock.yaml",
        {
            "run_id": "r1",
            "actor": "a",
            "scope": "s",
            "created_at": stale_hb,
            "last_heartbeat": stale_hb,
            "expires_at": (now + timedelta(hours=1)).isoformat(),
        },
    )
    result = _classify_existing_lock(ldir / "test.lock.yaml")
    assert result["kind"] == "zombie_stale_heartbeat"


def test_acquire_replaces_zombie_lock(registry: dict) -> None:
    ldir = _lock_dir(registry)
    past = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    _make_lock(
        ldir,
        "path_foo.py.lock.yaml",
        {
            "run_id": "old-run",
            "actor": "old-agent",
            "scope": "path:foo.py",
            "created_at": past,
            "last_heartbeat": past,
            "expires_at": past,
        },
    )
    locks = acquire_locks(registry, ["path:foo.py"], "new-run", "agent-b", False)
    assert len(locks) == 1
    data = yaml.safe_load((ldir / "path_foo.py.lock.yaml").read_text())
    assert data["run_id"] == "new-run"


def test_acquire_blocks_on_live_lock(registry: dict) -> None:
    ldir = _lock_dir(registry)
    now = datetime.now(UTC).isoformat()
    _make_lock(
        ldir,
        "path_foo.py.lock.yaml",
        {
            "run_id": "live-run",
            "actor": "agent-a",
            "scope": "path:foo.py",
            "created_at": now,
            "last_heartbeat": now,
            "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        },
    )
    with pytest.raises(WorkflowError, match="lock HELD.*live"):
        acquire_locks(registry, ["path:foo.py"], "new-run", "agent-b", False)


def test_acquire_same_run_same_scope_renews_instead_of_colliding(registry: dict) -> None:
    """同 run 补 claim 已持有路径 = 续期, 不是冲突 (BET-Y2Q4-T10-341).

    `claim_run` 的 A2A 循环按 run 文件跳过自己, 所以撞不在那一段; 撞在 acquire_locks
    的 `live` 分支 —— 它原来不问持锁 run_id 是谁。补 claim (合并型交付) 因此必然报错。
    """
    ldir = _lock_dir(registry)
    now_iso = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    hour_ago = (datetime.now(UTC) - timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    _make_lock(
        ldir,
        "path_foo.py.lock.yaml",
        {
            "run_id": "run-1",
            "actor": "agent-a",
            "scope": "path:foo.py",
            "created_at": hour_ago,
            "last_heartbeat": now_iso,  # 心跳新鲜, 才落到 live 分支 (过期心跳会被接管重建, 那是另一条判据)
            "expires_at": (datetime.now(UTC) + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
        },
    )

    again = acquire_locks(registry, ["path:foo.py"], "run-1", "agent-a", False)

    assert len(again) == 1 and again[0].endswith("path_foo.py.lock.yaml")  # 同一把锁, 不新增文件
    assert sorted(p.name for p in ldir.glob("*.lock.yaml")) == ["path_foo.py.lock.yaml"]
    new = yaml.safe_load((ldir / "path_foo.py.lock.yaml").read_text())
    assert new["run_id"] == "run-1"
    assert new["scope"] == "path:foo.py"
    assert new["actor"] == "agent-a"
    assert new["created_at"] == hour_ago  # 续期不重建: 创建时刻必须留住
    assert new["last_heartbeat"] > hour_ago
    renewed_expiry = datetime.fromisoformat(new["expires_at"].replace("Z", "+00:00"))
    assert renewed_expiry > datetime.now(UTC) + timedelta(hours=23)


def test_acquire_same_run_other_scope_on_same_filename_still_blocks(registry: dict) -> None:
    """锁文件名会 sanitize + 截断加 hash, 所以「同一文件名」不等于「同一 scope」。

    只在 run_id 上放行会让一个同 run 的 claim 悄悄改写别人 scope 的锁; 这条钉子要求
    scope 也逐字相等, 否则维持冲突。
    """
    ldir = _lock_dir(registry)
    now = datetime.now(UTC).isoformat()
    _make_lock(
        ldir,
        "path_foo.py.lock.yaml",
        {
            "run_id": "run-1",
            "actor": "agent-a",
            "scope": "path:other.py",
            "created_at": now,
            "last_heartbeat": now,
            "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        },
    )
    with pytest.raises(WorkflowError, match="lock HELD.*live"):
        acquire_locks(registry, ["path:foo.py"], "run-1", "agent-a", False)
    assert yaml.safe_load((ldir / "path_foo.py.lock.yaml").read_text())["scope"] == "path:other.py"


def test_acquire_same_run_legacy_lock_without_identity_still_blocks(registry: dict) -> None:
    """缺 run_id/scope 的老格式锁: 身份算不出来就不是「我自己」, 维持 fail-closed."""
    ldir = _lock_dir(registry)
    _make_lock(ldir, "path_foo.py.lock.yaml", {"actor": "agent-a"})
    with pytest.raises(WorkflowError, match="lock HELD.*live"):
        acquire_locks(registry, ["path:foo.py"], "run-1", "agent-a", False)


def test_acquire_partial_failure_keeps_same_run_existing_lock(registry: dict) -> None:
    """一次失败的补 claim 不得释放掉本 run 早已持有的锁.

    续期分支不得把这把锁计入回滚集合 (acquired_paths 只收本次新建的锁), 否则第二批
    scope 撞车时前一批已经隔离好的路径会被反向拆锁。
    """
    ldir = _lock_dir(registry)
    acquire_locks(registry, ["path:mine.py"], "run-1", "agent-a", False)
    created = yaml.safe_load((ldir / "path_mine.py.lock.yaml").read_text())["created_at"]
    now = datetime.now(UTC).isoformat()
    _make_lock(
        ldir,
        "path_theirs.py.lock.yaml",
        {
            "run_id": "run-2",
            "actor": "agent-b",
            "scope": "path:theirs.py",
            "created_at": now,
            "last_heartbeat": now,
            "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        },
    )
    with pytest.raises(WorkflowError, match="lock HELD.*live"):
        acquire_locks(registry, ["path:mine.py", "path:theirs.py"], "run-1", "agent-a", False)
    kept = yaml.safe_load((ldir / "path_mine.py.lock.yaml").read_text())
    assert kept["run_id"] == "run-1"
    assert kept["created_at"] == created


def test_heartbeat_updates_timestamp(registry: dict) -> None:
    acquire_locks(registry, ["path:foo.py"], "run-1", "agent-a", False)
    lock_file = _lock_dir(registry) / "path_foo.py.lock.yaml"
    old_data = yaml.safe_load(lock_file.read_text())
    time.sleep(0.01)
    heartbeat_lock(lock_file)
    new_data = yaml.safe_load(lock_file.read_text())
    assert new_data["last_heartbeat"] >= old_data["last_heartbeat"]


def test_scan_locks_report(registry: dict) -> None:
    acquire_locks(registry, ["path:a.py"], "run-1", "agent-a", False)
    ldir = _lock_dir(registry)
    past = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    _make_lock(
        ldir,
        "path_b.py.lock.yaml",
        {
            "run_id": "old-run",
            "actor": "old",
            "scope": "path:b.py",
            "created_at": past,
            "last_heartbeat": past,
            "expires_at": past,
        },
    )
    report = scan_locks(registry)
    assert len(report) == 2
    kinds = {e["kind"] for e in report}
    assert "live" in kinds
    assert "zombie_expired" in kinds


def test_prune_stale_locks(registry: dict) -> None:
    acquire_locks(registry, ["path:a.py"], "run-1", "agent-a", False)
    ldir = _lock_dir(registry)
    past = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    _make_lock(
        ldir,
        "path_b.py.lock.yaml",
        {
            "run_id": "old-run",
            "actor": "old",
            "scope": "path:b.py",
            "created_at": past,
            "last_heartbeat": past,
            "expires_at": past,
        },
    )
    pruned = prune_stale_locks(registry)
    assert len(pruned) == 1
    assert pruned[0]["kind"] == "zombie_expired"
    remaining = scan_locks(registry)
    assert len(remaining) == 1
    assert remaining[0]["kind"] == "live"


# ---------------------------------------------------------------------------
# SR-01 heartbeat hardening: heartbeat_run + CLI + lifecycle wiring
# ---------------------------------------------------------------------------


def _run_dir(registry: dict) -> Path:
    return lifecycle_mod.WORKSPACE / registry.get("runner", {}).get(
        "run_state_dir", ".omo/_delivery/agent-workflows/runs"
    )


def _seed_run(
    registry: dict,
    run_id: str = "run-1",
    lock_scopes: list[str] | None = None,
    status: str = "active",
) -> Path:
    """Create a run YAML plus its lock files. Returns the run file path."""
    scopes = lock_scopes or ["path:foo.py"]
    lock_paths = acquire_locks(registry, scopes, run_id, "agent-a", False)
    rdir = _run_dir(registry)
    rdir.mkdir(parents=True, exist_ok=True)
    now_ts = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    record: dict = {
        "run_id": run_id,
        "workflow_id": "test-wf",
        "status": status,
        "actor": "agent-a",
        "created_at": now_ts,
        "updated_at": now_ts,
        "locks": lock_paths,
    }
    run_path = rdir / f"{run_id}.yaml"
    run_path.write_text(yaml.safe_dump(record, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return run_path


def _lock_file_for_scope(registry: dict, scope: str) -> Path:
    return _lock_dir(registry) / f"{sanitize_lock_name(scope)}.lock.yaml"


def test_heartbeat_run_multi_lock_success(registry: dict) -> None:
    """Multiple locks all renewed in a single heartbeat_run call."""
    scopes = ["path:alpha.py", "path:beta.py"]
    _seed_run(registry, "run-multi", lock_scopes=scopes)
    receipt = heartbeat_run(registry, "run-multi")
    assert receipt["run_id"] == "run-multi"
    assert receipt["count"] == 2
    assert len(receipt["renewed"]) == 2
    assert "heartbeat_at" in receipt
    for scope in scopes:
        data = yaml.safe_load(_lock_file_for_scope(registry, scope).read_text())
        assert data["last_heartbeat"] == receipt["heartbeat_at"]


def test_heartbeat_run_preserves_immutable_fields(registry: dict) -> None:
    """Only last_heartbeat changes; created_at, expires_at, run_id, actor, scope frozen."""
    _seed_run(registry, "run-preserve", lock_scopes=["path:keep.py"])
    lock_file = _lock_file_for_scope(registry, "path:keep.py")
    before = yaml.safe_load(lock_file.read_text())
    time.sleep(1.1)  # utc_now() is second-level precision
    heartbeat_run(registry, "run-preserve")
    after = yaml.safe_load(lock_file.read_text())
    assert after["last_heartbeat"] > before["last_heartbeat"]
    for key in ("created_at", "expires_at", "run_id", "actor", "scope"):
        assert after[key] == before[key]


def test_heartbeat_run_rejects_closed_run(registry: dict) -> None:
    """Closed run cannot heartbeat."""
    _seed_run(registry, "run-closed", status="ok")
    with pytest.raises(WorkflowError, match="active"):
        heartbeat_run(registry, "run-closed")


def test_heartbeat_run_missing_lock(registry: dict) -> None:
    """Lock file listed by run but absent on disk → WorkflowError."""
    _seed_run(registry, "run-missing", lock_scopes=["path:gone.py"])
    _lock_file_for_scope(registry, "path:gone.py").unlink()
    with pytest.raises(WorkflowError, match="missing"):
        heartbeat_run(registry, "run-missing")


def test_heartbeat_run_malformed_yaml(registry: dict) -> None:
    """Malformed lock (non-mapping YAML) → WorkflowError."""
    _seed_run(registry, "run-bad-yaml", lock_scopes=["path:bad.py"])
    _lock_file_for_scope(registry, "path:bad.py").write_text("not-a-mapping-just-a-string\n", encoding="utf-8")
    with pytest.raises(WorkflowError, match="malformed"):
        heartbeat_run(registry, "run-bad-yaml")


def test_heartbeat_run_mismatched_run_id(registry: dict) -> None:
    """Lock owned by different run_id → WorkflowError."""
    _seed_run(registry, "run-mismatch", lock_scopes=["path:wrong.py"])
    lock_file = _lock_file_for_scope(registry, "path:wrong.py")
    data = yaml.safe_load(lock_file.read_text())
    data["run_id"] = "somebody-else"
    lock_file.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(WorkflowError, match="run_id"):
        heartbeat_run(registry, "run-mismatch")


def test_heartbeat_run_path_escape(registry: dict) -> None:
    """Lock path outside lock_state_dir → WorkflowError."""
    _seed_run(registry, "run-escape", lock_scopes=["path:ok.py"])
    run_path = _run_dir(registry) / "run-escape.yaml"
    data = yaml.safe_load(run_path.read_text())
    data["locks"] = ["../../etc/shadow.lock.yaml"]
    run_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(WorkflowError, match="escape|outside"):
        heartbeat_run(registry, "run-escape")


def test_heartbeat_run_no_partial_mutation(registry: dict) -> None:
    """When one lock fails validation, all locks remain unchanged."""
    _seed_run(registry, "run-partial", lock_scopes=["path:good.py", "path:bad.py"])
    _lock_file_for_scope(registry, "path:bad.py").write_text(":::not[yaml::", encoding="utf-8")
    good_lock = _lock_file_for_scope(registry, "path:good.py")
    good_before = good_lock.read_text()
    with pytest.raises(WorkflowError):
        heartbeat_run(registry, "run-partial")
    assert good_lock.read_text() == good_before


def test_heartbeat_cli_success_json(
    registry: dict, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CLI heartbeat --json outputs receipt and rc=0."""
    _seed_run(registry, "run-cli-ok", lock_scopes=["path:cli.py"])
    import omo.workflow.cli as cli_mod

    monkeypatch.setattr(cli_mod, "load_registry", lambda _path: registry)
    monkeypatch.setattr(cli_mod, "is_default_registry_path", lambda _path: False)
    rc = cli_mod.main(["heartbeat", "run-cli-ok", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["run_id"] == "run-cli-ok"
    assert out["count"] == 1


def test_heartbeat_cli_failure_rc2(
    registry: dict, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CLI heartbeat on non-existent run → rc=2."""
    import omo.workflow.cli as cli_mod

    monkeypatch.setattr(cli_mod, "load_registry", lambda _path: registry)
    monkeypatch.setattr(cli_mod, "is_default_registry_path", lambda _path: False)
    rc = cli_mod.main(["heartbeat", "no-such-run", "--json"])
    assert rc == 2


def test_heartbeat_wired_into_claim(registry: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """claim_run renews heartbeat before acquiring new locks."""
    _seed_run(registry, "run-claim", lock_scopes=["path:init.py"])

    heartbeat_called: list[str] = []
    original = lifecycle_mod.heartbeat_run

    def spy(reg: dict, rid: str) -> dict:
        heartbeat_called.append(rid)
        return original(reg, rid)

    monkeypatch.setattr(lifecycle_mod, "heartbeat_run", spy)
    monkeypatch.setattr(
        lifecycle_mod,
        "validate_affected_graph_receipt",
        lambda *_args, **_kwargs: {"receipt_hash": "a" * 64},
    )

    claim_run(
        registry,
        "run-claim",
        "agent-a",
        paths=["path:new.py"],
        surfaces=[],
        force_lock=False,
        affected_receipt="receipt.json",
    )
    assert "run-claim" in heartbeat_called


def test_heartbeat_wired_into_closeout_ok(registry: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """closeout status=ok calls heartbeat before verify/observe."""
    _seed_run(registry, "run-co", lock_scopes=["path:co.py"])

    heartbeat_called: list[str] = []
    original = lifecycle_mod.heartbeat_run

    def spy(reg: dict, rid: str) -> dict:
        heartbeat_called.append(rid)
        return original(reg, rid)

    monkeypatch.setattr(lifecycle_mod, "heartbeat_run", spy)

    from omo.workflow import diagnostics as diag_mod

    monkeypatch.setattr(
        diag_mod,
        "build_verify_report",
        lambda *a, **kw: {"ok": False, "check_count": 0, "checks": []},
    )

    with pytest.raises(WorkflowError, match="closeout blocked"):
        closeout_run(registry, "run-co", "ok", [], [], False, False, False, False)

    assert "run-co" in heartbeat_called


# ---------------------------------------------------------------------------
# SR-01 fix-only: atomic writes + CLI verify renewal + symlink escape
# ---------------------------------------------------------------------------


def test_heartbeat_run_uses_atomic_write(registry: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """heartbeat_run must use write_yaml_atomic, not raw Path.write_text."""
    _seed_run(registry, "run-atomic", lock_scopes=["path:atomic.py"])

    import omo.omo_io as omo_io_mod

    atomic_calls: list[Path] = []
    original = omo_io_mod.write_yaml_atomic

    def spy(path: Path, data: dict) -> None:
        atomic_calls.append(path)
        original(path, data)

    monkeypatch.setattr(lifecycle_mod, "write_yaml_atomic", spy, raising=False)

    receipt = heartbeat_run(registry, "run-atomic")
    assert receipt["count"] == 1
    assert len(atomic_calls) == 1
    assert atomic_calls[0].name == "path_atomic.py.lock.yaml"


def test_heartbeat_run_serializes_concurrent_renewals(registry: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """Concurrent renewals for one run never enter the atomic writer together."""
    _seed_run(registry, "run-concurrent", lock_scopes=["path:concurrent.py"])

    original_write = lifecycle_mod.write_yaml_atomic
    state_lock = threading.Lock()
    start = threading.Barrier(2)
    active_writers = 0
    max_active_writers = 0

    def slow_atomic_write(path: Path, data: dict) -> None:
        nonlocal active_writers, max_active_writers
        with state_lock:
            active_writers += 1
            max_active_writers = max(max_active_writers, active_writers)
        try:
            time.sleep(0.05)
            original_write(path, data)
        finally:
            with state_lock:
                active_writers -= 1

    def renew() -> dict:
        start.wait(timeout=5)
        return heartbeat_run(registry, "run-concurrent")

    monkeypatch.setattr(lifecycle_mod, "write_yaml_atomic", slow_atomic_write)
    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = [future.result(timeout=5) for future in (pool.submit(renew), pool.submit(renew))]

    assert [receipt["count"] for receipt in receipts] == [1, 1]
    assert max_active_writers == 1


def test_heartbeat_run_symlink_escape(registry: dict) -> None:
    """Symlink inside lock_dir pointing outside → WorkflowError."""
    _seed_run(registry, "run-symlink", lock_scopes=["path:symlinked.py"])
    lock_file = _lock_file_for_scope(registry, "path:symlinked.py")
    # File outside the lock directory
    outside = _lock_dir(registry).parent / "outside_target.yaml"
    outside.write_text("hijacked: true\n", encoding="utf-8")
    # Replace the real lock with a symlink pointing outside
    lock_file.unlink()
    os.symlink(outside, lock_file)
    with pytest.raises(WorkflowError, match="escape|outside"):
        heartbeat_run(registry, "run-symlink")


def test_cli_verify_explicit_run_id_renews_heartbeat(
    registry: dict, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CLI verify with explicit run_id renews heartbeat before verify."""
    _seed_run(registry, "run-verify", lock_scopes=["path:verify.py"])
    import omo.workflow.cli as cli_mod

    heartbeat_called: list[str] = []
    original_hb = lifecycle_mod.heartbeat_run

    def hb_spy(reg: dict, rid: str) -> dict:
        heartbeat_called.append(rid)
        return original_hb(reg, rid)

    monkeypatch.setattr(cli_mod, "heartbeat_run", hb_spy)
    monkeypatch.setattr(cli_mod, "load_registry", lambda _p: registry)
    monkeypatch.setattr(cli_mod, "is_default_registry_path", lambda _p: False)
    monkeypatch.setattr(
        cli_mod,
        "build_verify_report",
        lambda *a, **kw: {"ok": True, "check_count": 0, "checks": []},
    )

    rc = cli_mod.main(["verify", "run-verify", "--all", "--json"])
    assert rc == 0
    assert "run-verify" in heartbeat_called


def test_cli_verify_no_run_id_skips_heartbeat(
    registry: dict, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CLI verify without run_id does NOT call heartbeat_run."""
    import omo.workflow.cli as cli_mod

    heartbeat_called: list[str] = []

    def hb_spy(reg: dict, rid: str) -> dict:
        heartbeat_called.append(rid)
        return {
            "run_id": rid,
            "heartbeat_at": "",
            "renewed": [],
            "count": 0,
        }

    monkeypatch.setattr(cli_mod, "heartbeat_run", hb_spy)
    monkeypatch.setattr(cli_mod, "load_registry", lambda _p: registry)
    monkeypatch.setattr(cli_mod, "is_default_registry_path", lambda _p: False)
    monkeypatch.setattr(
        cli_mod,
        "build_verify_report",
        lambda *a, **kw: {"ok": True, "check_count": 0, "checks": []},
    )

    rc = cli_mod.main(["verify", "--all", "--json"])
    assert rc == 0
    assert heartbeat_called == []


# ---------------------------------------------------------------------------
# Final review blockers: payload locks type validation + read-failure wrapping
# ---------------------------------------------------------------------------


def test_heartbeat_run_integer_locks_raises(registry: dict) -> None:
    """Run payload with integer locks field → WorkflowError before any mutation."""
    _seed_run(registry, "run-int", lock_scopes=["path:foo.py"])
    run_path = _run_dir(registry) / "run-int.yaml"
    data = yaml.safe_load(run_path.read_text())
    data["locks"] = 42
    run_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(WorkflowError, match="locks must be a list"):
        heartbeat_run(registry, "run-int")


def test_heartbeat_run_mapping_locks_raises(registry: dict) -> None:
    """Run payload with mapping locks field → WorkflowError."""
    _seed_run(registry, "run-map", lock_scopes=["path:foo.py"])
    run_path = _run_dir(registry) / "run-map.yaml"
    data = yaml.safe_load(run_path.read_text())
    data["locks"] = {"path": "bad"}
    run_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(WorkflowError, match="locks must be a list"):
        heartbeat_run(registry, "run-map")


def test_heartbeat_run_scalar_string_locks_raises(registry: dict) -> None:
    """Run payload with scalar string locks field → WorkflowError."""
    _seed_run(registry, "run-scalar", lock_scopes=["path:foo.py"])
    run_path = _run_dir(registry) / "run-scalar.yaml"
    data = yaml.safe_load(run_path.read_text())
    data["locks"] = "not-a-list"
    run_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(WorkflowError, match="locks must be a list"):
        heartbeat_run(registry, "run-scalar")


def test_heartbeat_run_mixed_list_member_raises(registry: dict) -> None:
    """Locks list containing a non-string entry → WorkflowError."""
    _seed_run(registry, "run-mixed", lock_scopes=["path:good.py"])
    run_path = _run_dir(registry) / "run-mixed.yaml"
    data = yaml.safe_load(run_path.read_text())
    data["locks"] = ["locks/path_good.py.lock.yaml", 42]
    run_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(WorkflowError, match="invalid"):
        heartbeat_run(registry, "run-mixed")


def test_heartbeat_run_empty_string_entry_raises(registry: dict) -> None:
    """Locks list containing an empty string → WorkflowError."""
    _seed_run(registry, "run-empty-str", lock_scopes=["path:good.py"])
    run_path = _run_dir(registry) / "run-empty-str.yaml"
    data = yaml.safe_load(run_path.read_text())
    data["locks"] = ["locks/path_good.py.lock.yaml", ""]
    run_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(WorkflowError, match="invalid"):
        heartbeat_run(registry, "run-empty-str")


def test_heartbeat_run_oserror_on_read(registry: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """OSError during lock read → WorkflowError, not bare OSError."""
    _seed_run(registry, "run-oserr", lock_scopes=["path:target.py"])
    lock_file = _lock_file_for_scope(registry, "path:target.py")

    original_read_text = Path.read_text

    def fake_read_text(self: Path, *args: object, **kwargs: object) -> str:
        if self == lock_file:
            raise OSError("simulated IO failure")
        return original_read_text(self, *args, **kwargs)  # type: ignore[call-arg]

    monkeypatch.setattr(Path, "read_text", fake_read_text)
    with pytest.raises(WorkflowError, match="unreadable"):
        heartbeat_run(registry, "run-oserr")


def test_heartbeat_run_unicodeerror_on_read(registry: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """UnicodeDecodeError during lock read → WorkflowError."""
    _seed_run(registry, "run-unicode", lock_scopes=["path:target.py"])
    lock_file = _lock_file_for_scope(registry, "path:target.py")

    original_read_text = Path.read_text

    def fake_read_text(self: Path, *args: object, **kwargs: object) -> str:
        if self == lock_file:
            raise UnicodeDecodeError("utf-8", b"\xff\xfe", 0, 1, "invalid start byte")
        return original_read_text(self, *args, **kwargs)  # type: ignore[call-arg]

    monkeypatch.setattr(Path, "read_text", fake_read_text)
    with pytest.raises(WorkflowError, match="malformed|encoding"):
        heartbeat_run(registry, "run-unicode")


def test_heartbeat_run_no_partial_mutation_on_oserror(registry: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """When second lock read raises OSError, first lock remains unchanged."""
    _seed_run(
        registry,
        "run-no-partial-oserr",
        lock_scopes=["path:first.py", "path:second.py"],
    )
    second_lock = _lock_file_for_scope(registry, "path:second.py")
    first_lock = _lock_file_for_scope(registry, "path:first.py")
    first_before = first_lock.read_text()

    original_read_text = Path.read_text

    def fake_read_text(self: Path, *args: object, **kwargs: object) -> str:
        if self == second_lock:
            raise OSError("simulated IO failure on second lock")
        return original_read_text(self, *args, **kwargs)  # type: ignore[call-arg]

    monkeypatch.setattr(Path, "read_text", fake_read_text)
    with pytest.raises(WorkflowError):
        heartbeat_run(registry, "run-no-partial-oserr")
    assert first_lock.read_text() == first_before


# ---------------------------------------------------------------------------
# BET-Y2Q4-T10-292 (PITFALL-COO-008): A2A collision predicate + lock expiry
# ---------------------------------------------------------------------------


_DROP = object()


def _past_ts(hours: int = 2) -> str:
    return (datetime.now(UTC) - timedelta(hours=hours)).isoformat()


def _future_ts(hours: int = 2) -> str:
    return (datetime.now(UTC) + timedelta(hours=hours)).isoformat()


def _seed_foreign_claim(
    registry: dict,
    run_id: str,
    claim_paths: list[str],
    *,
    expires_at: str | None = None,
    last_heartbeat: str | None = None,
    status: str = "active",
    lock_overrides: dict[str, object] | None = None,
    raw_lock: str | None = None,
    skip_lock: bool = False,
) -> Path:
    """Write another run's record (with claims[].paths) and its scope locks.

    Mirrors the production shape: ``claims[].paths`` and the lock ``scope`` field
    come from the same normalized string, so ``f"path:{op}"`` is the join key.
    """
    ldir = _lock_dir(registry)
    ldir.mkdir(parents=True, exist_ok=True)
    created = datetime.now(UTC).isoformat()
    lock_refs: list[str] = []
    for claim_path in claim_paths:
        scope = f"path:{claim_path}"
        lock_file = ldir / f"{sanitize_lock_name(scope)}.lock.yaml"
        lock_refs.append(str(lock_file.relative_to(lifecycle_mod.WORKSPACE)))
        if skip_lock:
            continue
        if raw_lock is not None:
            lock_file.write_text(raw_lock, encoding="utf-8")
            continue
        payload: dict[str, object] = {
            "run_id": run_id,
            "actor": "other-agent",
            "scope": scope,
            "created_at": created,
            "last_heartbeat": last_heartbeat or created,
        }
        if expires_at is not None:
            payload["expires_at"] = expires_at
        for key, value in (lock_overrides or {}).items():
            if value is _DROP:
                payload.pop(key, None)
            else:
                payload[key] = value
        _make_lock(ldir, lock_file.name, payload)

    rdir = _run_dir(registry)
    rdir.mkdir(parents=True, exist_ok=True)
    record = {
        "run_id": run_id,
        "workflow_id": "test-wf",
        "status": status,
        "actor": "other-agent",
        "created_at": created,
        "updated_at": created,
        "locks": lock_refs,
        "claims": [
            {"claimed_at": created, "actor": "other-agent", "paths": claim_paths, "surfaces": []},
        ],
    }
    run_file = rdir / f"{run_id}.yaml"
    run_file.write_text(yaml.safe_dump(record, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return run_file


def _claim_as_mine(
    registry: dict,
    monkeypatch: pytest.MonkeyPatch,
    paths: list[str],
    run_id: str = "run-mine",
    force_lock: bool = False,
) -> dict:
    """Claim ``paths`` from a seeded active run, bypassing the receipt prereq."""
    _seed_run(registry, run_id, lock_scopes=["path:init.py"])
    monkeypatch.setattr(
        lifecycle_mod,
        "validate_affected_graph_receipt",
        lambda *_args, **_kwargs: {"receipt_hash": "a" * 64},
    )
    return claim_run(
        registry,
        run_id,
        "agent-a",
        paths=paths,
        surfaces=[],
        force_lock=force_lock,
        affected_receipt="receipt.json",
    )


LEDGER_PATH = "docs/plans/3y-bet-ledger.yaml"


def test_claim_admits_foreign_active_run_whose_lock_expired(registry: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """正例: status=active 但锁 expires_at 已过 → claim 放行 (PITFALL-COO-008 正解)."""
    foreign = _seed_foreign_claim(registry, "run-other", [LEDGER_PATH], expires_at=_past_ts(hours=13))
    before = foreign.read_text()

    result = _claim_as_mine(registry, monkeypatch, [LEDGER_PATH])

    assert result["paths"] == [LEDGER_PATH]
    assert f"path:{LEDGER_PATH}" in result["scopes"]
    # the foreign run record itself is never touched by the predicate
    assert foreign.read_text() == before
    lock = yaml.safe_load(_lock_file_for_scope(registry, f"path:{LEDGER_PATH}").read_text())
    assert lock["run_id"] == "run-mine"


def test_claim_still_blocks_foreign_active_run_whose_lock_is_live(
    registry: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """反例: status=active 且锁未过期 → 仍拒 (今日行为不得退化)."""
    _seed_foreign_claim(registry, "run-other", [LEDGER_PATH], expires_at=_future_ts(hours=20))

    with pytest.raises(WorkflowError, match="A2A Path Lock Collision"):
        _claim_as_mine(registry, monkeypatch, [LEDGER_PATH])


def test_claim_blocks_on_stale_heartbeat_with_live_expiry(registry: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """宽度: 心跳已过 _HEARTBEAT_STALE_SECONDS 但 expires_at 未到 → 仍拒.

    钉住判据 1: 本谓词不得复用 _classify_existing_lock() 的 zombie_stale_heartbeat
    宽度 (PITFALL-COO-008 实测一次 prune-locks 把 22/29 条活锁判成 zombie)。
    """
    _seed_foreign_claim(
        registry,
        "run-other",
        [LEDGER_PATH],
        expires_at=_future_ts(hours=10),
        last_heartbeat=_past_ts(hours=(_HEARTBEAT_STALE_SECONDS // 3600) + 5),
    )
    scope_lock = _lock_file_for_scope(registry, f"path:{LEDGER_PATH}")
    assert _classify_existing_lock(scope_lock)["kind"] == "zombie_stale_heartbeat"

    with pytest.raises(WorkflowError, match="A2A Path Lock Collision"):
        _claim_as_mine(registry, monkeypatch, [LEDGER_PATH])


def test_claim_blocks_on_hierarchy_overlap_with_expired_child_lock(
    registry: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """重叠由前缀判定, 但过期只按对方 claim 路径自身的锁证明: 父目录 claim 未过期 → 仍拒."""
    _seed_foreign_claim(registry, "run-other", ["docs/plans"], expires_at=_future_ts(hours=6))

    with pytest.raises(WorkflowError, match="A2A Path Lock Collision"):
        _claim_as_mine(registry, monkeypatch, [LEDGER_PATH])


def test_claim_admits_hierarchy_overlap_when_parent_claim_lock_expired(
    registry: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """父目录 claim 的锁已过期 → 子路径 claim 放行 (与锁层 _classify_existing_lock 同向)."""
    _seed_foreign_claim(registry, "run-other", ["docs/plans"], expires_at=_past_ts(hours=30))

    result = _claim_as_mine(registry, monkeypatch, [LEDGER_PATH])

    assert result["paths"] == [LEDGER_PATH]


@pytest.mark.parametrize(
    ("case", "kwargs"),
    [
        ("lock_file_absent", {"skip_lock": True}),
        ("lock_yaml_unparseable", {"raw_lock": ":::not[yaml::\n"}),
        ("lock_not_a_mapping", {"raw_lock": "just-a-string\n"}),
        ("no_expires_at_key", {"expires_at": None}),
        ("expires_at_unparseable", {"expires_at": "tomorrow-ish"}),
        ("expires_at_naive_timestamp", {"expires_at": "2026-01-01T00:00:00"}),
        ("no_scope_key", {"expires_at": _past_ts(), "lock_overrides": {"scope": _DROP}}),
        ("expires_at_null", {"expires_at": _past_ts(), "lock_overrides": {"expires_at": None}}),
    ],
    ids=[
        "lock_file_absent",
        "lock_yaml_unparseable",
        "lock_not_a_mapping",
        "no_expires_at_key",
        "expires_at_unparseable",
        "expires_at_naive_timestamp",
        "no_scope_key",
        "expires_at_null",
    ],
)
def test_claim_fail_closed_when_expiry_cannot_be_proven(
    registry: dict, monkeypatch: pytest.MonkeyPatch, case: str, kwargs: dict
) -> None:
    """fail-closed 面: 不能证明过期的一律算冲突 (契约 2)."""
    _seed_foreign_claim(registry, "run-other", [LEDGER_PATH], **kwargs)

    with pytest.raises(WorkflowError, match="A2A Path Lock Collision") as excinfo:
        _claim_as_mine(registry, monkeypatch, [LEDGER_PATH])
    # 契约 5: 报错前缀不变, 既有处置脚本按前缀识别
    assert str(excinfo.value).startswith("A2A Path Lock Collision:")


def test_lock_scopes_past_expiry_selects_only_provably_expired_locks(registry: dict) -> None:
    """判据面: 混合锁目录里只收「可证明过期」的 scope, 且不看心跳."""
    ldir = _lock_dir(registry)
    ldir.mkdir(parents=True, exist_ok=True)
    _make_lock(
        ldir,
        "path_expired.py.lock.yaml",
        {
            "run_id": "r1",
            "scope": "path:expired.py",
            "expires_at": _past_ts(),
            "last_heartbeat": datetime.now(UTC).isoformat(),
        },
    )
    _make_lock(
        ldir,
        "path_live.py.lock.yaml",
        {
            "run_id": "r2",
            "scope": "path:live.py",
            "expires_at": _future_ts(),
            "last_heartbeat": _past_ts(hours=48),
        },
    )
    _make_lock(
        ldir,
        "path_broken.py.lock.yaml",
        {"run_id": "r3", "scope": "path:broken.py", "expires_at": "not-a-time"},
    )

    assert lock_scopes_past_expiry(ldir) == {"path:expired.py"}


def test_lock_scopes_past_expiry_survives_missing_and_unreadable_planes(registry: dict, tmp_path: Path) -> None:
    """判据面: 目录缺失 / 非目录 / 无匹配文件 都返回空集, 不上抛."""
    assert lock_scopes_past_expiry(tmp_path / "no-such-dir") == set()
    a_file = tmp_path / "not-a-dir"
    a_file.write_text("x\n", encoding="utf-8")
    assert lock_scopes_past_expiry(a_file) == set()
    empty = tmp_path / "empty-locks"
    empty.mkdir()
    assert lock_scopes_past_expiry(empty) == set()


def test_lock_scopes_past_expiry_ignores_update_locks(registry: dict) -> None:
    """判据面: *.update.lock 不是 scope 锁, 不参与过期集合."""
    ldir = _lock_dir(registry)
    ldir.mkdir(parents=True, exist_ok=True)
    (ldir / "run_r1.update.lock").write_text(f"run_id: r1\ncreated_at: {_past_ts()}\n", encoding="utf-8")
    assert lock_scopes_past_expiry(ldir) == set()


def test_claim_admits_absent_lock_under_explicit_takeover(registry: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """正例: 对方 active claim 的锁对象根本不在盘上 → --force 显式接管放行 (PITFALL-COO-009).

    这一形状此前无任何恢复路径: release_locks 只 unlink run_id 等于自己的锁,
    prune_stale_locks 只遍历存在的文件, 对方 closeout 需要那个早已消失的会话。
    """
    foreign = _seed_foreign_claim(registry, "run-other", [LEDGER_PATH], skip_lock=True)
    before = foreign.read_text()

    result = _claim_as_mine(registry, monkeypatch, [LEDGER_PATH], force_lock=True)

    assert result["paths"] == [LEDGER_PATH]
    assert f"path:{LEDGER_PATH}" in result["scopes"]
    assert foreign.read_text() == before
    lock = yaml.safe_load(_lock_file_for_scope(registry, f"path:{LEDGER_PATH}").read_text())
    assert lock["run_id"] == "run-mine"


def test_absent_lock_without_takeover_names_the_absence_and_the_exit(
    registry: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """反例配平: 同一份 seed, force_lock=False 仍拒 (契约 2 宽度一字不动)。

    报错仍以 `A2A Path Lock Collision:` 开头 (契约 5), 但必须点名「锁对象缺席」
    与「--force 显式接管」—— 否则调用方只能读到与「活锁在持」逐字相同的拒绝。
    """
    _seed_foreign_claim(registry, "run-other", [LEDGER_PATH], skip_lock=True)

    with pytest.raises(WorkflowError, match="A2A Path Lock Collision") as excinfo:
        _claim_as_mine(registry, monkeypatch, [LEDGER_PATH])
    message = str(excinfo.value)
    assert message.startswith("A2A Path Lock Collision:")
    assert "lock object for path:" in message
    assert "--force" in message


@pytest.mark.parametrize(
    ("case", "kwargs"),
    [("lock_file_present_live", {"expires_at": None}), ("lock_yaml_unparseable", {"raw_lock": ":::not[yaml::\n"})],
    ids=["lock_file_present_live", "lock_yaml_unparseable"],
)
def test_takeover_does_not_widen_present_but_unprovable_locks(
    registry: dict, monkeypatch: pytest.MonkeyPatch, case: str, kwargs: dict
) -> None:
    """反向边界: 锁对象在盘上但证明不了过期 → --force 也拒.

    放行判据是「没有对象可持」, 不是「读不出」—— 后者与活锁同形, 接管会拆掉别人的活锁
    (PITFALL-COO-008 实测 prune-locks 一次把 22/29 条活锁判成 zombie)。
    """
    if case == "lock_file_present_live":
        kwargs = {"expires_at": _future_ts(hours=20)}
    _seed_foreign_claim(registry, "run-other", [LEDGER_PATH], **kwargs)
    assert lock_scope_object_present(_lock_dir(registry), f"path:{LEDGER_PATH}") is True

    with pytest.raises(WorkflowError, match="A2A Path Lock Collision"):
        _claim_as_mine(registry, monkeypatch, [LEDGER_PATH], force_lock=True)


def test_takeover_exemption_is_per_scope_not_per_run(registry: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """粒度: 同一个对方 run 的两条 claim 路径, 一条锁在一条锁丢 → 只放行丢锁的那条.

    生产实读: run d825f60f 的 claim 列 3 把锁, 盘上只剩 2 把且均已过期。
    """
    _seed_foreign_claim(registry, "run-other", ["src/a.py"], skip_lock=True)
    _seed_foreign_claim(registry, "run-other-2", ["src/b.py"], expires_at=_future_ts(hours=8))

    with pytest.raises(WorkflowError, match="A2A Path Lock Collision"):
        _claim_as_mine(registry, monkeypatch, ["src/a.py", "src/b.py"], force_lock=True)

    result = _claim_as_mine(registry, monkeypatch, ["src/a.py"], force_lock=True)
    assert result["paths"] == ["src/a.py"]
    still_held = yaml.safe_load(_lock_file_for_scope(registry, "path:src/b.py").read_text())
    assert still_held["run_id"] == "run-other-2"


def test_lock_scope_object_present_matches_writer_bounded_name(registry: dict) -> None:
    """判据面: 存在性探测与写者按同一个有界名落位, 超长 scope 不得误报缺席."""
    ldir = _lock_dir(registry)
    ldir.mkdir(parents=True, exist_ok=True)
    long_path = "docs/" + "-".join(["segment"] * 60) + "/ledger.yaml"
    scope = f"path:{long_path}"

    assert lock_scope_object_present(ldir, scope) is False
    created = acquire_locks(registry, [scope], "run-writer", "agent-a", False)
    assert len(created) == 1
    assert lock_scope_object_present(ldir, scope) is True
    assert len(Path(created[0]).name) <= 255


def test_lock_scope_object_present_fails_closed_on_unusable_dir(tmp_path: Path) -> None:
    """宽度面: 锁目录不可 stat → 报「在」(维持冲突), 探测失败不得被读成无持有者.

    反面是 Path.is_file() 把 ENOTDIR 吞成 False —— 那会让一个坏掉的锁面冒充「无人持有」,
    正好绕过本 BET 要守的契约 2。
    """
    a_file = tmp_path / "not-a-dir"
    a_file.write_text("x\n", encoding="utf-8")
    assert lock_scope_object_present(a_file, "path:foo.py") is True
    empty = tmp_path / "empty-locks"
    empty.mkdir()
    assert lock_scope_object_present(empty, "path:foo.py") is False
    assert lock_scope_object_present(tmp_path / "no-such-dir", "path:foo.py") is False
