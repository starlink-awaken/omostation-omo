"""Shadow observer helpers for engineering delivery consumer."""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path
from typing import Any

from .engineering_delivery_consumer_constants import (
    MOS_PROJECTION_RECEIPT_LOG,
    QUALIFIED_DECISION_OUTCOME_LOG,
    SCENE_BINDING,
    SHADOW_OBSERVER_SCHEMA,
    _SHADOW_OBSERVER_TOTAL_MAX_BYTES,
)
from .engineering_delivery_consumer_projection import _workspace_root
from .engineering_delivery_consumer_validators import (
    EngineeringDeliveryConsumerError,
    EngineeringDeliveryProjectionError,
)
from .outcome_feedback import OUTCOME_FEEDBACK_LOG
from .workflow_mesh import WORKFLOW_MESH_LOG, project_workflow_run
from .engineering_delivery_consumer_validators import (
    EngineeringDeliveryConsumerError,
    EngineeringDeliveryProjectionError,
)
from .workflow_mesh import project_workflow_run


class _ShadowObserverInputError(EngineeringDeliveryConsumerError):
    """Shadow observer input validation failure."""


class _ShadowObserverInputChangedError(_ShadowObserverInputError):
    """Shadow observer input changed during read."""


class _ShadowObserverInputSnapshot:
    __slots__ = ("path", "fd", "identity", "digest", "payload")

    def __init__(
        self,
        path: Path,
        fd: int | None,
        identity: tuple[int, int, int, int, int] | None,
        digest: str | None,
        payload: bytes | None,
    ) -> None:
        self.path = path
        self.fd = fd
        self.identity = identity
        self.digest = digest
        self.payload = payload


def _shadow_observer_identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode), info.st_size, info.st_mtime_ns)


def _shadow_observer_relative_parts(path: Path, *, workspace_root: Path) -> tuple[str, ...]:
    try:
        relative = path.absolute().relative_to(workspace_root.absolute())
    except ValueError as exc:
        raise _ShadowObserverInputError("shadow observer input escapes its workspace") from exc
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise _ShadowObserverInputError("shadow observer input path is invalid")
    return relative.parts


def _shadow_observer_directory_flags() -> int:
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise _ShadowObserverInputError("secure directory traversal is unavailable")
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)


def _open_shadow_observer_workspace(workspace_root: Path) -> int | None:
    try:
        return os.open(workspace_root, _shadow_observer_directory_flags())
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise _ShadowObserverInputError("shadow observer workspace cannot be opened safely") from exc


def _open_shadow_observer_leaf(workspace_fd: int, parts: tuple[str, ...]) -> int | None:
    current_fd = workspace_fd
    try:
        for part in parts:
            next_fd = os.open(current_fd, part, os.O_RDONLY)
            if current_fd != workspace_fd:
                os.close(current_fd)
            current_fd = next_fd
    except OSError:
        if current_fd != workspace_fd:
            os.close(current_fd)
        return None
    return current_fd


def _read_shadow_observer_bytes(fd: int, *, max_bytes: int) -> bytes:
    try:
        data = os.read(fd, max_bytes)
    except OSError as exc:
        raise _ShadowObserverInputError(str(exc)) from exc
    return data


def _digest_shadow_observer_bytes(fd: int, *, max_bytes: int) -> str:
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = os.read(fd, 65_536)
        if not chunk:
            return digest.hexdigest()
        size += len(chunk)
        if size > max_bytes:
            raise _ShadowObserverInputError("shadow observer input exceeds the byte limit")
        digest.update(chunk)


def _read_shadow_observer_input(
    path: Path,
    *,
    workspace_root: Path,
    workspace_fd: int,
    max_bytes: int,
) -> _ShadowObserverInputSnapshot:
    fd = _open_shadow_observer_leaf(
        workspace_fd,
        _shadow_observer_relative_parts(path, workspace_root=workspace_root),
    )
    if fd is None:
        return _ShadowObserverInputSnapshot(path, None, None, None, None)
    keep_open = False
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise _ShadowObserverInputError("shadow observer input is not a regular file")
        if before.st_size > max_bytes:
            raise _ShadowObserverInputError("shadow observer input exceeds the byte limit")
        payload = _read_shadow_observer_bytes(fd, max_bytes=max_bytes)
        after = os.fstat(fd)
        if _shadow_observer_identity(before) != _shadow_observer_identity(after):
            raise _ShadowObserverInputChangedError("shadow observer input changed during capture")
        snapshot = _ShadowObserverInputSnapshot(
            path,
            fd,
            _shadow_observer_identity(after),
            hashlib.sha256(payload).hexdigest(),
            payload,
        )
        keep_open = True
        return snapshot
    finally:
        if not keep_open:
            os.close(fd)


def _read_shadow_observer_inputs(
    omo_dir: Path,
) -> tuple[dict[Path, _ShadowObserverInputSnapshot], tuple[int, int, int, int, int] | None]:
    snapshots: dict[Path, _ShadowObserverInputSnapshot] = {}
    workspace_root = _workspace_root(omo_dir)
    workspace_fd = _open_shadow_observer_workspace(workspace_root)
    if workspace_fd is None:
        return (
            {
                path: _ShadowObserverInputSnapshot(path, None, None, None, None)
                for path in _shadow_observer_input_paths(omo_dir)
            },
            None,
        )
    workspace_identity = _shadow_observer_identity(os.fstat(workspace_fd))
    total_bytes = 0
    try:
        for path in _shadow_observer_input_paths(omo_dir):
            remaining_bytes = _SHADOW_OBSERVER_TOTAL_MAX_BYTES - total_bytes
            snapshot = _read_shadow_observer_input(
                path,
                workspace_root=workspace_root,
                workspace_fd=workspace_fd,
                max_bytes=remaining_bytes,
            )
            total_bytes += len(snapshot.payload or b"")
            if total_bytes > _SHADOW_OBSERVER_TOTAL_MAX_BYTES:
                raise _ShadowObserverInputError(
                    f"shadow observer input exceeds {_SHADOW_OBSERVER_TOTAL_MAX_BYTES} bytes"
                )
            snapshots[path] = snapshot
    finally:
        os.close(workspace_fd)
    return snapshots, workspace_identity


def _read_shadow_observer_jsonl(snapshot: _ShadowObserverInputSnapshot) -> list[dict[str, Any]]:
    if snapshot.payload is None:
        return []
    records: list[dict[str, Any]] = []
    for line in snapshot.payload.decode("utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(__import__("json").loads(line))
        except ValueError:
            records.append({"raw": line[:200]})
    return records


def _verify_shadow_observer_input(
    snapshot: _ShadowObserverInputSnapshot,
    *,
    workspace_root: Path,
) -> _ShadowObserverInputSnapshot:
    try:
        current = _read_shadow_observer_input(
            snapshot.path,
            workspace_root=workspace_root,
            workspace_fd=_open_shadow_observer_workspace(workspace_root)
            or _open_shadow_observer_workspace(workspace_root),
            max_bytes=_SHADOW_OBSERVER_TOTAL_MAX_BYTES,
        )
    except _ShadowObserverInputError:
        return snapshot
    if current.identity != snapshot.identity:
        raise _ShadowObserverInputChangedError(f"{snapshot.path} changed during shadow observer read")
    return snapshot


def _verify_shadow_observer_inputs(
    snapshots: dict[Path, _ShadowObserverInputSnapshot],
    *,
    workspace_root: Path,
) -> dict[Path, _ShadowObserverInputSnapshot]:
    verified: dict[Path, _ShadowObserverInputSnapshot] = {}
    for path, snapshot in snapshots.items():
        verified[path] = _verify_shadow_observer_input(snapshot, workspace_root=workspace_root)
    return verified


def _close_shadow_observer_inputs(snapshots: dict[Path, _ShadowObserverInputSnapshot]) -> None:
    for snapshot in snapshots.values():
        if snapshot.fd is not None:
            try:
                os.close(snapshot.fd)
            except OSError:
                pass


def _shadow_observer_input_paths(omo_dir: Path) -> tuple[Path, ...]:
    """Return every durable input consumed by the query-only observer."""
    return (
        omo_dir / QUALIFIED_DECISION_OUTCOME_LOG,
        omo_dir / MOS_PROJECTION_RECEIPT_LOG,
        omo_dir / OUTCOME_FEEDBACK_LOG,
        omo_dir / WORKFLOW_MESH_LOG,
        _workspace_root(omo_dir) / ".omo" / "state" / "agent-beliefs" / "index.yaml",
    )
