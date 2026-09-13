"""test_role_registry.py — T10-165 件一 Role 注册表与准入状态机单元测试.

覆盖: 注册校验 (坏前缀/空 capabilities/重复拒绝)、合法链
(pending→admitted→suspended→admitted→revoked)、非法跃迁拒绝
(pending 直跳 revoked、revoked 后再变)、stale-version 拒绝、
digest 稳定性、can_admit verifier 雏形、JSONL 落盘 round-trip。
"""

from __future__ import annotations

import json

import pytest

from omo.workflow.role_registry import (
    RoleRecord,
    RoleRegistry,
    RoleRegistryError,
    admission_digest,
    role_verifier_binding,
)


@pytest.fixture()
def registry() -> RoleRegistry:
    return RoleRegistry()


@pytest.fixture()
def admitted(registry: RoleRegistry) -> RoleRegistry:
    registry.register("role:cell-alpha", {"dispatch", "observe"})
    registry.admit("role:cell-alpha", expected_version=1)
    return registry


def test_register_ok(registry: RoleRegistry) -> None:
    r = registry.register("role:cell-alpha", {"dispatch"})
    assert isinstance(r, RoleRecord)
    assert (r.admission_state, r.version) == ("pending", 1)
    assert r.digest.startswith("sha256:")


def test_register_bad_prefix_rejected(registry: RoleRegistry) -> None:
    with pytest.raises(RoleRegistryError) as exc:
        registry.register("cell-alpha", {"dispatch"})
    assert exc.value.code == "invalid-role-id"


def test_register_empty_capabilities_rejected(registry: RoleRegistry) -> None:
    with pytest.raises(RoleRegistryError) as exc:
        registry.register("role:cell-alpha", set())
    assert exc.value.code == "empty-capabilities"


def test_register_duplicate_rejected(registry: RoleRegistry) -> None:
    registry.register("role:cell-alpha", {"dispatch"})
    with pytest.raises(RoleRegistryError) as exc:
        registry.register("role:cell-alpha", {"dispatch"})
    assert exc.value.code == "duplicate-role"


def test_legal_chain(admitted: RoleRegistry) -> None:
    r = admitted.suspend("role:cell-alpha", expected_version=2)
    assert (r.admission_state, r.version) == ("suspended", 3)
    r = admitted.admit("role:cell-alpha", expected_version=3)
    assert (r.admission_state, r.version) == ("admitted", 4)
    r = admitted.revoke("role:cell-alpha", expected_version=4)
    assert (r.admission_state, r.version) == ("revoked", 5)


def test_pending_direct_revoke_ok(registry: RoleRegistry) -> None:
    # pending → revoked 是允许的 (注册即否决路径)
    registry.register("role:cell-alpha", {"dispatch"})
    r = registry.revoke("role:cell-alpha", expected_version=1)
    assert r.admission_state == "revoked"


def test_pending_to_suspended_rejected(registry: RoleRegistry) -> None:
    registry.register("role:cell-alpha", {"dispatch"})
    with pytest.raises(RoleRegistryError) as exc:
        registry.suspend("role:cell-alpha", expected_version=1)
    assert exc.value.code == "illegal-transition"


def test_revoked_is_terminal(admitted: RoleRegistry) -> None:
    admitted.revoke("role:cell-alpha", expected_version=2)
    with pytest.raises(RoleRegistryError) as exc:
        admitted.admit("role:cell-alpha", expected_version=3)
    assert exc.value.code == "illegal-transition"


def test_unknown_role_rejected(registry: RoleRegistry) -> None:
    with pytest.raises(RoleRegistryError) as exc:
        registry.admit("role:ghost", expected_version=1)
    assert exc.value.code == "unknown-role"


def test_stale_version_rejected(admitted: RoleRegistry) -> None:
    with pytest.raises(RoleRegistryError) as exc:
        admitted.suspend("role:cell-alpha", expected_version=1)
    assert exc.value.code == "stale-version"


def test_digest_stable() -> None:
    body = {"schema": "omo-role-registry/v1", "role_id": "role:x"}
    assert admission_digest(body) == admission_digest(dict(body))


def test_can_admit_verifier(admitted: RoleRegistry) -> None:
    assert admitted.can_admit("role:cell-alpha", "dispatch") is True
    assert admitted.can_admit("role:cell-alpha", "launch-nukes") is False
    admitted.suspend("role:cell-alpha", expected_version=2)
    assert admitted.can_admit("role:cell-alpha", "dispatch") is False


def test_role_verifier_binding_and_custom_verifier(admitted: RoleRegistry) -> None:
    def require_dispatch(record: RoleRecord, capability: str) -> bool:
        return capability == "dispatch" and record.version >= 2

    verification = admitted.verify_role("role:cell-alpha", "dispatch", verifier=require_dispatch)
    assert verification.allowed is True
    assert verification.role_version == 2
    assert verification.role_digest == admitted.get("role:cell-alpha").digest
    binding = role_verifier_binding(require_dispatch)
    assert verification.verifier_id == binding["verifier_id"]
    assert verification.verifier_digest == binding["verifier_digest"]

    # 自定义 verifier 只能加严，不能绕过 admitted/capability 基线。
    assert admitted.verify_role("role:cell-alpha", "unknown").allowed is False
    admitted.suspend("role:cell-alpha", expected_version=2)
    assert admitted.verify_role("role:cell-alpha", "dispatch", verifier=require_dispatch).allowed is False


def test_jsonl_roundtrip(tmp_path) -> None:
    store = tmp_path / "roles.jsonl"
    reg = RoleRegistry(store)
    reg.register("role:cell-alpha", {"dispatch", "observe"})
    reg.admit("role:cell-alpha", expected_version=1)
    reloaded = RoleRegistry(store)
    r = reloaded.get("role:cell-alpha")
    assert r is not None
    assert (r.admission_state, r.version) == ("admitted", 2)
    assert r.capabilities == frozenset({"dispatch", "observe"})
    assert r.digest.startswith("sha256:")


def test_tampered_role_record_rejected_on_load(tmp_path) -> None:
    store = tmp_path / "roles.jsonl"
    registry = RoleRegistry(store)
    registry.register("role:cell-alpha", {"dispatch"})
    data = json.loads(store.read_text(encoding="utf-8"))
    data["capabilities"] = ["dispatch", "extra"]
    store.write_text(json.dumps(data, ensure_ascii=False) + "\n", encoding="utf-8")

    with pytest.raises(RoleRegistryError) as exc:
        RoleRegistry(store)
    assert exc.value.code == "digest-mismatch"


def test_list_filter(admitted: RoleRegistry) -> None:
    admitted.register("role:cell-beta", {"observe"})
    assert [r.role_id for r in admitted.list(state="admitted")] == ["role:cell-alpha"]
    assert [r.role_id for r in admitted.list(state="pending")] == ["role:cell-beta"]
