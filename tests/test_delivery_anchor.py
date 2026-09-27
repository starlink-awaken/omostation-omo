"""BET-Y2Q4-T10-208 — delivery-state canonical anchor tests (fake checkouts only).

Every fixture builds FAKE canonical/worktree trees under ``tmp_path`` and
patches the locator env (``HOME`` / ``OMOSTATION_ROOT`` /
``OMOSTATION_STATE_ROOT``). No test here may resolve to the real checkout that
hosts this session: it holds a LIVE governance run in a real (non-symlink)
``.omo/_delivery/agent-workflows`` directory.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

import omo.workflow.core as core_mod
from omo.workflow.delivery_anchor import (
    CONFLICT_LOG_NAME,
    DELIVERY_RELATIVE,
    ensure_delivery_anchor,
    is_worktree_of,
    locate_anchor,
)

MARKER = Path("docs") / "project-registry.yaml"


def _snapshot(root: Path) -> dict[str, str | None]:
    """Byte-level snapshot of a tree (None = directory, hex = file bytes)."""
    state: dict[str, str | None] = {}
    if not root.exists() and not root.is_symlink():
        return state
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        if path.is_symlink():
            state[rel] = f"symlink->{os.readlink(path)}"
        elif path.is_dir():
            state[rel] = None
        else:
            state[rel] = path.read_bytes().hex()
    return state


def _fake_canonical(root: Path) -> Path:
    canonical = root / "canonical"
    (canonical / MARKER).parent.mkdir(parents=True, exist_ok=True)
    (canonical / MARKER).write_text("schema: project-registry/v1\n", encoding="utf-8")
    (canonical / ".git" / "worktrees" / "wt").mkdir(parents=True, exist_ok=True)
    return canonical


def _fake_worktree(root: Path, gitdir: Path | None = None, name: str = "wt") -> Path:
    ws = root / f"ws-{name}"
    ws.mkdir(parents=True, exist_ok=True)
    target = gitdir if gitdir is not None else root / "canonical" / ".git" / "worktrees" / name
    (ws / ".git").write_text(f"gitdir: {target}\n", encoding="utf-8")
    return ws


def _anchor_env(monkeypatch, canonical: Path, home: Path | None = None) -> None:
    monkeypatch.setenv("OMOSTATION_ROOT", str(canonical))
    monkeypatch.delenv("OMOSTATION_STATE_ROOT", raising=False)
    monkeypatch.setenv("HOME", str(home if home is not None else canonical.parent / "fake-home"))


def test_placement_dict_path_unchanged_resolves_into_canonical(tmp_path: Path, monkeypatch) -> None:
    canonical = _fake_canonical(tmp_path)
    ws = _fake_worktree(tmp_path)
    _anchor_env(monkeypatch, canonical)
    registry = {"runner": {"workspace_root": str(ws)}}

    run_dir = core_mod.run_state_dir(registry)

    # dict path stays workspace-relative (never the absolute canonical path)
    assert run_dir == ws.resolve() / DELIVERY_RELATIVE / "runs"
    link = ws / DELIVERY_RELATIVE
    assert link.is_symlink()
    assert Path(os.readlink(link)) == canonical / DELIVERY_RELATIVE
    # ...but resolves into the canonical checkout
    assert run_dir.resolve() == (canonical / DELIVERY_RELATIVE / "runs").resolve()
    # display_path stays dict-relative: records must not leak canonical paths
    monkeypatch.setattr(core_mod, "WORKSPACE", ws.resolve())
    assert core_mod.display_path(run_dir) == (DELIVERY_RELATIVE / "runs").as_posix()


def test_merge_migration_moves_children_conflicts_keep_canonical(tmp_path: Path, monkeypatch) -> None:
    canonical = _fake_canonical(tmp_path)
    ws = _fake_worktree(tmp_path)
    _anchor_env(monkeypatch, canonical)
    canonical_delivery = canonical / DELIVERY_RELATIVE
    (canonical_delivery / "runs").mkdir(parents=True)
    (canonical_delivery / "runs" / "shared.yaml").write_text("canonical\n", encoding="utf-8")
    (canonical_delivery / "runs" / "canonical-only.yaml").write_text("canonical-only\n", encoding="utf-8")
    (canonical_delivery / "events.jsonl").write_text('{"event":"canonical"}\n', encoding="utf-8")
    worktree_delivery = ws / DELIVERY_RELATIVE
    (worktree_delivery / "runs").mkdir(parents=True)
    (worktree_delivery / "runs" / "shared.yaml").write_text("worktree\n", encoding="utf-8")
    (worktree_delivery / "runs" / "worktree-only.yaml").write_text("worktree-only\n", encoding="utf-8")
    (worktree_delivery / "locks").mkdir()
    (worktree_delivery / "locks" / "scope.py.lock.yaml").write_text("lock\n", encoding="utf-8")
    (worktree_delivery / "events.jsonl").write_text('{"event":"worktree"}\n', encoding="utf-8")

    result = ensure_delivery_anchor(ws)

    assert result == canonical_delivery
    assert worktree_delivery.is_symlink()
    assert (worktree_delivery / "runs" / "shared.yaml").read_text(encoding="utf-8") == "canonical\n"
    assert (canonical_delivery / "runs" / "worktree-only.yaml").read_text(encoding="utf-8") == "worktree-only\n"
    assert (canonical_delivery / "runs" / "canonical-only.yaml").is_file()
    assert (canonical_delivery / "locks" / "scope.py.lock.yaml").is_file()
    assert (canonical_delivery / "events.jsonl").read_text(encoding="utf-8") == (
        '{"event":"canonical"}\n{"event":"worktree"}\n'
    )
    conflict_log = (canonical_delivery / CONFLICT_LOG_NAME).read_text(encoding="utf-8")
    assert "runs/shared.yaml" in conflict_log
    assert "canonical_wins" in conflict_log

    before = _snapshot(canonical_delivery)
    assert ensure_delivery_anchor(ws) == canonical_delivery
    assert _snapshot(canonical_delivery) == before  # second call: byte-identical


def test_noop_when_ws_is_not_a_worktree(tmp_path: Path, monkeypatch) -> None:
    canonical = _fake_canonical(tmp_path)
    _anchor_env(monkeypatch, canonical)
    ws = tmp_path / "ws-clone"
    (ws / ".git").mkdir(parents=True)  # ordinary clone: .git is a directory

    before = _snapshot(ws)
    assert ensure_delivery_anchor(ws) is None
    assert _snapshot(ws) == before


def test_noop_for_foreign_worktree(tmp_path: Path, monkeypatch) -> None:
    canonical = _fake_canonical(tmp_path)
    _anchor_env(monkeypatch, canonical)
    elsewhere = tmp_path / "elsewhere" / ".git" / "worktrees" / "other"
    elsewhere.mkdir(parents=True)
    ws = _fake_worktree(tmp_path, gitdir=elsewhere, name="foreign")

    before = _snapshot(ws)
    assert ensure_delivery_anchor(ws) is None
    assert _snapshot(ws) == before


def test_noop_when_no_anchor_locatable(tmp_path: Path, monkeypatch) -> None:
    canonical = _fake_canonical(tmp_path)
    ws = _fake_worktree(tmp_path)
    monkeypatch.delenv("OMOSTATION_STATE_ROOT", raising=False)
    monkeypatch.delenv("OMOSTATION_ROOT", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "empty-home"))  # no ~/Workspace marker

    assert locate_anchor() is None
    before = _snapshot(ws)
    assert ensure_delivery_anchor(ws) is None
    assert _snapshot(ws) == before


def test_state_root_env_wins_over_canonical_root(tmp_path: Path, monkeypatch) -> None:
    canonical = _fake_canonical(tmp_path)
    state_root = tmp_path / "state-root"
    (state_root / MARKER).parent.mkdir(parents=True, exist_ok=True)
    (state_root / MARKER).write_text("schema: project-registry/v1\n", encoding="utf-8")
    (state_root / ".git" / "worktrees" / "wt").mkdir(parents=True, exist_ok=True)
    ws = _fake_worktree(tmp_path, gitdir=state_root / ".git" / "worktrees" / "wt")
    _anchor_env(monkeypatch, canonical)
    monkeypatch.setenv("OMOSTATION_STATE_ROOT", str(state_root))

    assert locate_anchor() == state_root
    result = ensure_delivery_anchor(ws)

    assert result == state_root / DELIVERY_RELATIVE
    assert Path(os.readlink(ws / DELIVERY_RELATIVE)) == state_root / DELIVERY_RELATIVE


def test_custom_relative_runner_config_never_anchors(tmp_path: Path, monkeypatch) -> None:
    canonical = _fake_canonical(tmp_path)
    ws = _fake_worktree(tmp_path)
    _anchor_env(monkeypatch, canonical)
    registry = {
        "runner": {
            "workspace_root": str(ws),
            "run_state_dir": "runs",
            "lock_state_dir": "locks",
            "ledger_path": "events.jsonl",
        }
    }

    assert core_mod.run_state_dir(registry) == ws.resolve() / "runs"
    assert core_mod.lock_state_dir(registry) == ws.resolve() / "locks"
    assert core_mod.ledger_path(registry) == ws.resolve() / "events.jsonl"
    assert not (ws / DELIVERY_RELATIVE).exists()
    assert not (ws / DELIVERY_RELATIVE).is_symlink()


def test_absolute_runner_config_never_anchors(tmp_path: Path, monkeypatch) -> None:
    canonical = _fake_canonical(tmp_path)
    ws = _fake_worktree(tmp_path)
    _anchor_env(monkeypatch, canonical)
    absolute = tmp_path / "custom" / "runs"
    registry = {"runner": {"workspace_root": str(ws), "run_state_dir": str(absolute)}}

    assert core_mod.run_state_dir(registry) == absolute
    assert not (ws / DELIVERY_RELATIVE).exists()
    assert not (ws / DELIVERY_RELATIVE).is_symlink()


@pytest.mark.skipif(shutil.which("git") is None, reason="git unavailable")
def test_worktree_remove_force_keeps_anchored_run(tmp_path: Path, monkeypatch) -> None:
    """#4435 regression: a run written through the bridge survives
    ``git worktree remove --force`` because it lives in canonical."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / MARKER).parent.mkdir(parents=True, exist_ok=True)
    (repo / MARKER).write_text("schema: project-registry/v1\n", encoding="utf-8")
    git_env = {
        **os.environ,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_AUTHOR_NAME": "anchor-test",
        "GIT_AUTHOR_EMAIL": "anchor-test@example.com",
        "GIT_COMMITTER_NAME": "anchor-test",
        "GIT_COMMITTER_EMAIL": "anchor-test@example.com",
    }

    def git(*args: str) -> None:
        subprocess.run(
            ["git", *args],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
            env=git_env,
        )

    git("init", "-q")
    git("add", ".")
    git("commit", "-q", "-m", "init")
    ws = tmp_path / "wt"
    git("worktree", "add", "-q", "-b", "test-delivery-anchor", str(ws))
    monkeypatch.setenv("OMOSTATION_ROOT", str(repo))
    monkeypatch.delenv("OMOSTATION_STATE_ROOT", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "empty-home"))

    run_file = ws / DELIVERY_RELATIVE / "runs" / "run-1.yaml"
    run_file.parent.mkdir(parents=True)
    run_file.write_text("run_id: run-1\n", encoding="utf-8")
    assert ensure_delivery_anchor(ws) is not None
    canonical_run = repo / DELIVERY_RELATIVE / "runs" / "run-1.yaml"
    assert canonical_run.is_file()
    assert (ws / DELIVERY_RELATIVE).is_symlink()

    git("worktree", "remove", "--force", str(ws))

    assert canonical_run.is_file()
    assert canonical_run.read_text(encoding="utf-8") == "run_id: run-1\n"


def test_worktree_of_rejects_missing_and_relative_gitdirs(tmp_path: Path, monkeypatch) -> None:
    canonical = _fake_canonical(tmp_path)
    _anchor_env(monkeypatch, canonical)
    bare = tmp_path / "ws-bare"
    bare.mkdir()
    assert is_worktree_of(bare, canonical) is False
    relative = tmp_path / "ws-relative"
    relative.mkdir()
    (relative / ".git").write_text("gitdir: ../nope/worktrees/x\n", encoding="utf-8")
    assert is_worktree_of(relative, canonical) is False
    absolute_ok = _fake_worktree(tmp_path)
    assert is_worktree_of(absolute_ok, canonical) is True
