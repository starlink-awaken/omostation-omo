"""Delivery-state canonical anchor + self-healing symlink bridge (BET-Y2Q4-T10-208).

The workflow kernel used to derive the repo root from ``__file__``
(``core.WORKSPACE``), so inside a linked git worktree every delivery-state write
(``.omo/_delivery/agent-workflows/{runs,locks,events.jsonl}``) landed inside the
worktree checkout. ``.gitignore`` drops that directory, so
``git worktree remove --force`` destroyed run records permanently (#4435):
closeout became impossible and the audit chain broke.

Fix: physically anchor delivery state to the canonical checkout and present it
inside linked worktrees as a self-healing symlink. Dict (workspace-relative)
paths are unchanged, so every read-side tool keeps working unmodified and
records never leak absolute canonical paths.

Trigger discipline — all three must hold, otherwise byte-identical no-op:
  (a) ``ws/.git`` is a file whose ``gitdir:`` resolves under
      ``<anchor>/.git/worktrees/`` — a linked worktree *of* the anchor;
      bare/foreign clones never anchor.
  (b) the production layout is being addressed (enforced by the caller hook in
      ``core._runner_path``; custom runner configs never reach here).
  (c) an anchor is locatable (``OMOSTATION_STATE_ROOT`` first — ADR-0456
      profile — then ``OMOSTATION_ROOT`` + marker, then the checkout that owns
      ``ws`` as read from its own ``gitdir:`` pointer + marker); otherwise
      silent no-op (CI-safe). The ``gitdir:`` pointer is the same value
      ``git rev-parse --git-common-dir`` resolves to, so the owning root is read
      from git's own record instead of guessed from a machine location — a
      worktree of a *different* clone (e.g. an install root) now anchors to its
      own clone rather than to whatever ``~/Workspace`` happens to be.
"""

from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

DELIVERY_RELATIVE = Path(".omo") / "_delivery" / "agent-workflows"
STATE_ROOT_ENV = "OMOSTATION_STATE_ROOT"
CANONICAL_ROOT_ENV = "OMOSTATION_ROOT"
MARKER = Path("docs") / "project-registry.yaml"
CONFLICT_LOG_NAME = ".delivery-anchor-conflicts.log"
LEDGER_NAME = "events.jsonl"


def _worktree_gitdir(ws: Path) -> Path | None:
    """Resolved ``gitdir:`` target of a linked worktree; ``None`` otherwise.

    ``ws/.git`` is a *file* only for a linked worktree — an ordinary clone keeps
    a directory there and a foreign checkout of some other repo never anchors.
    """
    git_entry = ws / ".git"
    if not git_entry.is_file():
        return None
    try:
        text = git_entry.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return None
    head, sep, raw = text.partition("gitdir:")
    if not sep or head.strip():
        return None
    gitdir = Path(raw.strip())
    if not gitdir.is_absolute():
        gitdir = ws / gitdir
    try:
        return gitdir.resolve()
    except OSError:
        return None


def _anchor_from_checkout(ws: Path) -> Path | None:
    """The checkout that owns ``ws``, read from ``ws``'s own ``gitdir:`` pointer."""
    gitdir = _worktree_gitdir(ws)
    if gitdir is None:
        return None
    worktrees = gitdir.parent
    if worktrees.name != "worktrees" or worktrees.parent.name != ".git":
        return None
    root = worktrees.parent.parent
    return root if (root / MARKER).is_file() else None


def locate_anchor(ws: Path | None = None) -> Path | None:
    """Locate the root that owns delivery state; ``None`` when not locatable.

    Declared ADR-0456 profile env wins, then ``OMOSTATION_ROOT`` + marker, then
    the checkout that owns ``ws``. Never falls back to ``__file__`` or to a
    guessed machine path — an unlocatable anchor is a silent no-op.
    """
    declared = os.environ.get(STATE_ROOT_ENV)
    if declared:
        candidate = Path(declared).expanduser()
        return candidate if candidate.is_dir() else None
    env_root = os.environ.get(CANONICAL_ROOT_ENV)
    if env_root:
        candidate = Path(env_root).expanduser()
        if (candidate / MARKER).is_file():
            return candidate
    return _anchor_from_checkout(ws) if ws is not None else None


def is_worktree_of(ws: Path, anchor: Path) -> bool:
    """True only for a linked worktree *of* ``anchor`` (stricter than
    ``repo_root.is_worktree``: a foreign clone of some other repo never anchors)."""
    gitdir = _worktree_gitdir(ws)
    if gitdir is None:
        return False
    worktrees_root = anchor / ".git" / "worktrees"
    if not worktrees_root.is_dir():
        return False
    try:
        return gitdir.is_relative_to(worktrees_root.resolve())
    except OSError:
        return False


def ensure_delivery_anchor(ws: Path) -> Path | None:
    """Present ``ws``'s delivery dir as a symlink to the canonical one.

    Returns the canonical delivery dir when the bridge is in effect (created,
    repaired, migrated, or already in place), ``None`` for a byte-identical
    no-op (no anchor locatable, not a worktree of it, or an unexpected
    non-directory occupant).
    """
    anchor = locate_anchor(ws)
    if anchor is None or not is_worktree_of(ws, anchor):
        return None
    target = ws / DELIVERY_RELATIVE
    canonical = anchor / DELIVERY_RELATIVE
    if target.is_symlink():
        if canonical.is_dir() and _resolved(target) == _resolved(canonical):
            return canonical  # already bridged — idempotent no-op
        target.unlink()  # dangling or stale pointer: self-heal
    elif target.exists():
        if not target.is_dir():
            return None  # unexpected plain-file occupant: leave bytes untouched
        if not _merge_migrate(target, canonical):
            return None  # verify failed: keep the original so a retry can finish
    canonical.mkdir(parents=True, exist_ok=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.symlink_to(canonical, target_is_directory=True)
    return canonical


def _resolved(path: Path) -> Path:
    try:
        return path.resolve()
    except OSError:
        return Path(os.path.realpath(path))


def _merge_migrate(source: Path, canonical: Path) -> bool:
    """Copy ``source``'s children into ``canonical``, verify, then rmtree.

    Same-name conflicts keep the canonical copy and append a conflict-log
    entry; ``events.jsonl`` is appended (JSONL) instead of overwritten;
    same-name directories merge recursively so worktree-only runs survive.
    """
    canonical.mkdir(parents=True, exist_ok=True)
    log = canonical / CONFLICT_LOG_NAME
    for child in sorted(source.iterdir()):
        if child.name == CONFLICT_LOG_NAME:
            continue
        _merge_child(child, canonical / child.name, log)
    missing = [
        child.name
        for child in source.iterdir()
        if child.name != CONFLICT_LOG_NAME and not (canonical / child.name).exists()
    ]
    if missing:
        return False
    shutil.rmtree(source)
    return True


def _merge_child(source: Path, dest: Path, log: Path) -> None:
    if not dest.exists():
        if source.is_dir():
            shutil.copytree(source, dest)
        else:
            shutil.copy2(source, dest)
        return
    if source.name == LEDGER_NAME and source.is_file() and dest.is_file():
        _append_bytes(dest, source.read_bytes())
        return
    if source.is_dir() and dest.is_dir():
        for child in sorted(source.iterdir()):
            _merge_child(child, dest / child.name, log)
        return
    _log_conflict(log, dest)  # canonical wins


def _append_bytes(dest: Path, payload: bytes) -> None:
    existing = dest.read_bytes()
    with dest.open("ab") as handle:
        if existing and not existing.endswith(b"\n"):
            handle.write(b"\n")
        handle.write(payload)
        if payload and not payload.endswith(b"\n"):
            handle.write(b"\n")


def _log_conflict(log: Path, dest: Path) -> None:
    stamp = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    try:
        name = dest.relative_to(log.parent).as_posix()
    except ValueError:
        name = dest.as_posix()
    with log.open("a", encoding="utf-8") as handle:
        handle.write(f"{stamp} canonical_wins {name}\n")
