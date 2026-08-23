"""Unit tests for omo.resident.resources — shared resource discovery (M3.3).

验证:
- _load_registry 读取 multi-doc YAML 注册表
- list_resources kind/capability 过滤
- summarize 统计
"""

from __future__ import annotations

from pathlib import Path

import pytest

from omo.resident import resources

_FABRIC_DOC = """\
---
title: Fabric Registry
status: active
---
version: "1.0"
resource_kinds:
  knowledge_source:
    allowed_capabilities: [search, read]
resources:
  - id: res-a
    kind: knowledge_source
    provider: omo
    protocol: local_fs
    capabilities: [search, read]
    data_classification: internal
    provenance: .omo/_knowledge
    lifecycle: active
    health: operational
    owner: architecture-governance
    version: "1.0"
    permission_ref: permission://internal/knowledge
    bos_uri: bos://memory/mos/knowledge-ref
    domain: system
    visibility: public
  - id: res-b
    kind: tool_capability
    provider: agora
    protocol: bos
    capabilities: [discover, invoke]
    data_classification: internal
    provenance: bin/gac
    lifecycle: active
    health: operational
    owner: architecture-governance
    version: "1.0"
    permission_ref: permission://internal/tools
    bos_uri: bos://capability/tools
    domain: system
    visibility: public
"""


@pytest.fixture
def _registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "fabric.yaml"
    path.write_text(_FABRIC_DOC, encoding="utf-8")
    monkeypatch.setattr(resources, "FABRIC_REGISTRY", path)
    return path


def test_load_registry_multi_doc(_registry: Path) -> None:
    doc = resources._load_registry()
    assert doc["version"] == "1.0"
    assert len(doc["resources"]) == 2


def test_list_resources_all(_registry: Path) -> None:
    out = resources.list_resources()
    assert [r["id"] for r in out] == ["res-a", "res-b"]


def test_list_resources_by_kind(_registry: Path) -> None:
    out = resources.list_resources(kind="tool_capability")
    assert len(out) == 1
    assert out[0]["id"] == "res-b"


def test_list_resources_by_capability(_registry: Path) -> None:
    out = resources.list_resources(capability="search")
    assert len(out) == 1
    assert out[0]["id"] == "res-a"


def test_list_resources_filter_no_match(_registry: Path) -> None:
    assert resources.list_resources(kind="channel") == []
    assert resources.list_resources(capability="publish") == []


def test_summarize(_registry: Path) -> None:
    summary = resources.summarize(resources.list_resources())
    assert summary["total"] == 2
    assert summary["by_kind"] == {"knowledge_source": 1, "tool_capability": 1}
    assert summary["kinds_covered"] == 2


def test_missing_registry_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(resources, "FABRIC_REGISTRY", tmp_path / "missing.yaml")
    with pytest.raises(ValueError):
        resources.list_resources()
