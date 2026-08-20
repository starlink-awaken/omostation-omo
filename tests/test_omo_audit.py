from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

import omo.omo_audit as audit
from omo.omo_audit import _load_yaml_safely, governance_check_agora_health
from omo.omo_paths import KAIRON_DIR


def test_governance_check_agora_health_with_active_event_loop(monkeypatch):
    async def _fake_check_all_health(endpoints):
        class _Result:
            def __init__(self, service: str, is_healthy: bool):
                self.service = service
                self.is_healthy = is_healthy

        return [_Result("agora", True), _Result("forge", False)]

    monkeypatch.setattr("omo.omo_health.load_agora_routes", lambda: {"routes": {}})
    monkeypatch.setattr(
        "omo.omo_health.derive_endpoints",
        lambda routes: {"agora": "http://localhost:7422/health"},
    )
    monkeypatch.setattr("omo.omo_health.check_all_health", _fake_check_all_health)

    async def _invoke():
        return governance_check_agora_health()

    result = asyncio.run(_invoke())
    assert result.category == "agora"
    assert result.message == "1/2 services healthy"
    assert result.severity == "warn"


def test_load_yaml_safely_accepts_multi_document_yaml(tmp_path: Path) -> None:
    payload = tmp_path / "audit.yaml"
    payload.write_text(
        "---\nstatus: active\nowner: governance\n---\n---\ncurrent_phase: 42\nhealth_score: 100\n",
        encoding="utf-8",
    )

    data = _load_yaml_safely(payload)

    assert data == {
        "status": "active",
        "owner": "governance",
        "current_phase": 42,
        "health_score": 100,
    }


def test_governance_lint_uses_canonical_kairon_directory(monkeypatch) -> None:
    observed: dict[str, object] = {}

    def fake_run(*_args, **kwargs):
        observed.update(kwargs)
        return SimpleNamespace(stdout="All checks passed!\n", stderr="")

    monkeypatch.setattr(audit.subprocess, "run", fake_run)

    result = audit.governance_check_lint()

    assert result.severity == "ok"
    assert Path(str(observed["cwd"])) == KAIRON_DIR


def test_governance_audit_workspace_override_uses_canonical_kairon_directory(monkeypatch, tmp_path: Path) -> None:
    observed_cwds: list[Path] = []

    def fake_run(*_args, **kwargs):
        observed_cwds.append(Path(str(kwargs["cwd"])))
        return SimpleNamespace(stdout="All checks passed!\n", stderr="")

    monkeypatch.setattr(audit, "_OMO_ROOT", audit._OMO_ROOT)
    monkeypatch.setattr(audit, "_KAIRON_DIR", audit._KAIRON_DIR)
    monkeypatch.setattr(audit, "_WORKSPACE_ROOT", audit._WORKSPACE_ROOT)
    monkeypatch.setattr(audit.subprocess, "run", fake_run)
    monkeypatch.setenv(audit.ENV_SKIP_AGORA, "1")
    audit.run_governance_audit(workspace=tmp_path)
    audit.governance_check_lint()

    assert observed_cwds == [
        tmp_path / "projects" / "knowledge" / "kairon",
        KAIRON_DIR,
    ]


def test_governance_audit_workspace_override_restores_paths_after_error(monkeypatch, tmp_path: Path) -> None:
    observed_cwds: list[Path] = []

    def fake_run(*_args, **kwargs):
        observed_cwds.append(Path(str(kwargs["cwd"])))
        return SimpleNamespace(stdout="All checks passed!\n", stderr="")

    def raise_coverage_error():
        raise RuntimeError("coverage check failed")

    monkeypatch.setattr(audit, "_OMO_ROOT", audit._OMO_ROOT)
    monkeypatch.setattr(audit, "_KAIRON_DIR", audit._KAIRON_DIR)
    monkeypatch.setattr(audit, "_WORKSPACE_ROOT", audit._WORKSPACE_ROOT)
    monkeypatch.setattr(audit.subprocess, "run", fake_run)
    monkeypatch.setattr(audit, "governance_check_test_coverage", raise_coverage_error)

    with pytest.raises(RuntimeError, match="coverage check failed"):
        audit.run_governance_audit(workspace=tmp_path)
    audit.governance_check_lint()

    assert observed_cwds == [
        tmp_path / "projects" / "knowledge" / "kairon",
        KAIRON_DIR,
    ]


def test_governance_lint_recommendation_uses_canonical_kairon_directory() -> None:
    recommendations = audit.build_recommendations(
        [
            audit.CheckResult(
                name="ruff lint",
                category="lint",
                severity="warn",
                score=90.0,
                message="2 errors",
            )
        ]
    )

    assert recommendations == [
        "修复 ruff 错误, 参考 `cd projects/knowledge/kairon && uv run ruff check packages/ --fix`"
    ]
