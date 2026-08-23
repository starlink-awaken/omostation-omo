"""Unit tests for omo.resident.resources — discovery + domain isolation (M4.2).

验证:
- _load_registry 读取 multi-doc YAML 注册表
- list_resources kind/capability/domain 过滤
- _visible 领域可见性 (public 全可见; private 仅同 domain)
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
  - id: res-public-system
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
  - id: res-public-knowledge
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
    bos_uri: bos://knowledge/sediment
    domain: knowledge
    visibility: public
  - id: res-private-knowledge
    kind: asset_source
    provider: omo
    protocol: local_fs
    capabilities: [read]
    data_classification: internal
    provenance: .omo/_knowledge
    lifecycle: active
    health: operational
    owner: architecture-governance
    version: "1.0"
    permission_ref: permission://internal/knowledge
    bos_uri: bos://knowledge/private
    domain: knowledge
    visibility: private
  - id: res-private-system
    kind: tool_capability
    provider: agora
    protocol: bos
    capabilities: [invoke]
    data_classification: internal
    provenance: bin/gac
    lifecycle: active
    health: operational
    owner: architecture-governance
    version: "1.0"
    permission_ref: permission://internal/tools
    bos_uri: bos://capability/tools
    domain: system
    visibility: private
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
    assert len(doc["resources"]) == 4


def test_list_all_default_open(_registry: Path) -> None:
    out = resources.list_resources()
    assert len(out) == 4  # 未声明 actor_domain → 全开放


def test_list_by_domain(_registry: Path) -> None:
    out = resources.list_resources(domain="knowledge")
    assert len(out) == 2
    assert all(r["domain"] == "knowledge" for r in out)


def test_visibility_actor_system_see_system_private(_registry: Path) -> None:
    out = resources.list_resources(actor_domain="system")
    ids = {r["id"] for r in out}
    # system 私有可见; knowledge 私有不可见; 所有 public 可见
    assert "res-private-system" in ids
    assert "res-private-knowledge" not in ids
    assert "res-public-system" in ids
    assert "res-public-knowledge" in ids


def test_visibility_actor_knowledge_see_knowledge_private(_registry: Path) -> None:
    out = resources.list_resources(actor_domain="knowledge")
    ids = {r["id"] for r in out}
    assert "res-private-knowledge" in ids
    assert "res-private-system" not in ids


def test_visibility_unknown_actor_sees_only_public(_registry: Path) -> None:
    out = resources.list_resources(actor_domain="other-domain")
    ids = {r["id"] for r in out}
    assert "res-private-knowledge" not in ids
    assert "res-private-system" not in ids
    assert "res-public-knowledge" in ids


def test_summarize(_registry: Path) -> None:
    summary = resources.summarize(resources.list_resources())
    assert summary["total"] == 4
    assert summary["by_domain"] == {"system": 2, "knowledge": 2}
    assert summary["kinds_covered"] == 3  # knowledge_source/asset_source/tool_capability


def test_missing_registry_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(resources, "FABRIC_REGISTRY", tmp_path / "missing.yaml")
    with pytest.raises(ValueError):
        resources.list_resources()
