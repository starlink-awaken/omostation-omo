from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

import omo.workflow.core as core_mod


def _make_git_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=path, check=True)


def _registry(tmp_path: Path, root: Path, *, patterns: list[str] | None = None) -> Path:
    registry_path = tmp_path / "external-write-roots.yaml"
    registry_path.write_text(
        yaml.safe_dump(
            {
                "schema": "external-write-root-registry/v1",
                "roots": [
                    {
                        "id": "dashboard",
                        "path": str(root),
                        "kind": "local_git",
                        "status": "admitted",
                        "patterns": patterns or ["*.js", "**/*.js", "**/*.py"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return registry_path


def test_external_git_changes_use_synthetic_path(tmp_path, monkeypatch) -> None:
    workspace = tmp_path / "workspace"
    root = tmp_path / "outside" / "dashboard"
    workspace.mkdir()
    _make_git_repo(workspace)
    _make_git_repo(root)
    (root / "app.js").write_text("console.log(1)\n", encoding="utf-8")
    subprocess.run(["git", "add", "app.js"], cwd=root, check=True)
    subprocess.run(["git", "commit", "-m", "init", "-q"], cwd=root, check=True)
    (root / "app.js").write_text("console.log(2)\n", encoding="utf-8")
    registry_path = _registry(tmp_path, root)
    monkeypatch.setattr(core_mod, "WORKSPACE", workspace)
    monkeypatch.setattr(core_mod, "EXTERNAL_ROOT_REGISTRY_PATH", registry_path)

    assert core_mod.external_synthetic_path(root / "app.js") == "external/dashboard/app.js"
    assert "external/dashboard/app.js" in core_mod.changed_files_from_git(include_untracked=True)


def test_external_path_mapping_rejects_disallowed_patterns_and_non_git_roots(tmp_path, monkeypatch) -> None:
    workspace = tmp_path / "workspace"
    root = tmp_path / "outside" / "not-git"
    root.mkdir(parents=True)
    workspace.mkdir()
    (root / "app.js").write_text("", encoding="utf-8")
    registry_path = _registry(tmp_path, root)
    monkeypatch.setattr(core_mod, "WORKSPACE", workspace)
    monkeypatch.setattr(core_mod, "EXTERNAL_ROOT_REGISTRY_PATH", registry_path)

    try:
        core_mod.external_synthetic_path(root / "app.js")
    except core_mod.WorkflowError as exc:
        assert "local git repository" in str(exc)
    else:
        raise AssertionError("non-git external root was admitted")

    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    (root / "notes.txt").write_text("", encoding="utf-8")
    assert core_mod.external_synthetic_path(root / "notes.txt") is None
    assert core_mod.external_synthetic_path(root / "app.js") == "external/dashboard/app.js"


def test_external_symlink_root_is_rejected(tmp_path, monkeypatch) -> None:
    workspace = tmp_path / "workspace"
    target = tmp_path / "outside" / "real-dashboard"
    link = tmp_path / "outside" / "dashboard"
    target.mkdir(parents=True)
    link.symlink_to(target, target_is_directory=True)
    workspace.mkdir()
    registry_path = _registry(tmp_path, link)
    monkeypatch.setattr(core_mod, "WORKSPACE", workspace)
    monkeypatch.setattr(core_mod, "EXTERNAL_ROOT_REGISTRY_PATH", registry_path)

    try:
        core_mod.load_external_write_roots(registry_path)
    except core_mod.WorkflowError as exc:
        assert "real directory" in str(exc)
    else:
        raise AssertionError("symlinked external root was admitted")
