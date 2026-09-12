"""BET-Y1Q4-T7-04 — 场景导航锚点与运行时护栏单元/端到端测试.

覆盖:
- anchor: 意图推荐 (确定性/无命中诚实空)、锚定签发、digest 防篡改、场景撤销
- guardrail: CapabilityJail 三态、DataScopeGuard 沙盒/symlink 逃逸、
  DriftRadar 四类漂移 100% 拦截 + 良性轨迹零误报
- e2e: scene-document-review / scene-engineering-delivery 两张真实场景卡
  四段仿真 (锚定 → 正常轨迹放行 → 漂移注入 → 100% 拦截)

circuit breaker 断言: 只读/探活类操作永不硬阻断。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from omo.guardrail.enforcer import (
    CapabilityJail,
    DataScopeGuard,
    DriftRadar,
    GuardrailEnforcer,
    is_readonly_tool,
)
from omo.scene.anchor import (
    SceneAnchorError,
    SceneAnchorRegistry,
    find_scene_cards_path,
    token_digest,
)

# ── 夹具 ──────────────────────────────────────────────────────

_CARDS = {
    "schema": "scene-cards-v3",
    "scenes": [
        {
            "scene_id": "scene-document-review",
            "name": "公文审阅",
            "scene_class": "work",
            "domain": "documents",
            "lifecycle": "assisted",
            "capability_refs": ["llm:local-classify", "documents:cas-write"],
        },
        {
            "scene_id": "scene-engineering-delivery",
            "name": "工程交付",
            "scene_class": "work",
            "domain": "governance",
            "lifecycle": "supervised",
            "capability_refs": ["code:repo-write", "llm:local-classify"],
        },
    ],
}


@pytest.fixture()
def registry(tmp_path: Path) -> SceneAnchorRegistry:
    import yaml

    cards_path = tmp_path / "scene-cards-v3.yaml"
    cards_path.write_text(yaml.safe_dump(_CARDS, allow_unicode=True), encoding="utf-8")
    return SceneAnchorRegistry.load(cards_path)


def _token(registry: SceneAnchorRegistry, scene_id: str, roots: list[str] | None = None) -> dict:
    return registry.bind("sess-test", scene_id, allowed_roots=roots, now="2026-09-12T00:00:00+00:00")


# ── anchor: 推荐 ──────────────────────────────────────────────


class TestRecommend:
    def test_deterministic_scoring(self, registry: SceneAnchorRegistry) -> None:
        a = registry.recommend("帮我审阅这份公文", top_k=2)
        b = registry.recommend("帮我审阅这份公文", top_k=2)
        assert [r.scene_id for r in a] == [r.scene_id for r in b]
        assert a and a[0].scene_id == "scene-document-review"
        assert 0.0 < a[0].confidence <= 1.0

    def test_no_hit_returns_empty(self, registry: SceneAnchorRegistry) -> None:
        assert registry.recommend("量子色动力学跃迁矩阵") == []

    def test_empty_input_returns_empty(self, registry: SceneAnchorRegistry) -> None:
        assert registry.recommend("   ") == []

    def test_engineering_intent_hits_delivery(self, registry: SceneAnchorRegistry) -> None:
        top = registry.recommend("推进 engineering delivery 工程交付流程", top_k=1)
        assert top[0].scene_id == "scene-engineering-delivery"


# ── anchor: 绑定与校验 ─────────────────────────────────────────


class TestBindVerify:
    def test_bind_issues_digest_token(self, registry: SceneAnchorRegistry) -> None:
        token = _token(registry, "scene-document-review", roots=["docs"])
        assert token["digest"] == token_digest(token)
        assert token["allowed_roots"] == ["docs"]
        assert token["lifecycle"] == "assisted"

    def test_bind_unknown_scene_rejected(self, registry: SceneAnchorRegistry) -> None:
        with pytest.raises(SceneAnchorError) as ei:
            registry.bind("s1", "scene-not-exist")
        assert ei.value.code == "unknown_scene"

    def test_bind_missing_session_rejected(self, registry: SceneAnchorRegistry) -> None:
        with pytest.raises(SceneAnchorError) as ei:
            registry.bind("", "scene-document-review")
        assert ei.value.code == "missing_session"

    def test_verify_detects_tamper(self, registry: SceneAnchorRegistry) -> None:
        token = _token(registry, "scene-document-review", roots=["docs"])
        tampered = dict(token, allowed_roots=["/"])
        assert registry.verify(tampered) == {"ok": False, "reason": "digest_mismatch"}

    def test_verify_scene_revoked(self, registry: SceneAnchorRegistry) -> None:
        token = _token(registry, "scene-document-review", roots=["docs"])
        registry.cards.pop("scene-document-review")
        assert registry.verify(token) == {"ok": False, "reason": "scene_revoked"}


# ── guardrail: CapabilityJail ──────────────────────────────────


class TestCapabilityJail:
    def test_readonly_probe_never_blocked(self) -> None:
        jail = CapabilityJail()
        token = {"capability_refs": []}
        for tool in ("get_status", "probe:health", "list_files", "search_notes"):
            v = jail.check(tool, token)
            assert v.decision == "warn", tool
            assert not v.blocked

    def test_namespace_grant_allows(self) -> None:
        jail = CapabilityJail()
        token = {"capability_refs": ["llm:local-classify"]}
        assert jail.check("llm.classify_text", token).decision == "allow"

    def test_outside_namespace_denied(self) -> None:
        jail = CapabilityJail()
        token = {"capability_refs": ["llm:local-classify"]}
        v = jail.check("shell.exec", token)
        assert v.decision == "deny"
        assert v.code == "capability_violation"

    def test_empty_refs_deny_writes(self) -> None:
        jail = CapabilityJail()
        v = jail.check("shell.exec", {"capability_refs": []})
        assert v.decision == "deny" and v.code == "no_capability_granted"

    def test_is_readonly_tool(self) -> None:
        assert is_readonly_tool("health.check")
        assert is_readonly_tool("PROBE:status")
        assert not is_readonly_tool("cas.write")


# ── guardrail: DataScopeGuard ──────────────────────────────────


class TestDataScopeGuard:
    def test_write_outside_sandbox_denied(self, tmp_path: Path) -> None:
        guard = DataScopeGuard(ws_root=tmp_path)
        token = {"allowed_roots": ["docs"]}
        v = guard.check_path(tmp_path / "etc" / "x.yaml", token, mode="write")
        assert v.decision == "deny" and v.code == "scope_violation"

    def test_write_inside_sandbox_allowed(self, tmp_path: Path) -> None:
        (tmp_path / "docs").mkdir()
        guard = DataScopeGuard(ws_root=tmp_path)
        token = {"allowed_roots": ["docs"]}
        v = guard.check_path(tmp_path / "docs" / "a.md", token, mode="write")
        assert v.decision == "allow"

    def test_empty_roots_deny_all_writes(self, tmp_path: Path) -> None:
        guard = DataScopeGuard(ws_root=tmp_path)
        v = guard.check_path(tmp_path / "docs" / "a.md", {"allowed_roots": []}, mode="write")
        assert v.decision == "deny" and v.code == "no_write_scope"

    def test_symlink_escape_denied(self, tmp_path: Path) -> None:
        (tmp_path / "docs").mkdir()
        outside = tmp_path / "outside"
        outside.mkdir()
        link = tmp_path / "docs" / "escape"
        link.symlink_to(outside, target_is_directory=True)
        guard = DataScopeGuard(ws_root=tmp_path)
        v = guard.check_path(link / "stealed.yaml", {"allowed_roots": ["docs"]}, mode="write")
        assert v.decision == "deny" and v.code == "symlink_escape"

    def test_read_mode_warn_only(self, tmp_path: Path) -> None:
        guard = DataScopeGuard(ws_root=tmp_path)
        v = guard.check_path("/etc/passwd", {"allowed_roots": []}, mode="read")
        assert v.decision == "warn" and not v.blocked


# ── guardrail: DriftRadar ──────────────────────────────────────


class TestDriftRadar:
    RADAR = DriftRadar()

    def test_benign_trajectory_passes(self) -> None:
        token = {"domain": "documents"}
        events = [{"tool": f"llm.op{i}", "domain": "documents"} for i in range(5)]
        r = self.RADAR.evaluate(events, token)
        assert r["decision"] == "pass" and r["drift_score"] == 0.0

    def test_single_violation_warns_not_intercepts(self) -> None:
        token = {"domain": "documents"}
        r = self.RADAR.evaluate([{"capability_violation": True}], token)
        assert r["decision"] == "warn" and r["drift_score"] == 2.0

    def test_drift_classes_all_intercepted(self) -> None:
        """四类漂移注入 → 100% intercept."""
        token = {"domain": "documents"}
        scenarios = {
            "capability_burst": [{"capability_violation": True}] * 2,
            "scope_burst": [{"scope_violation": True}] * 2,
            "cross_domain": [{"domain": d} for d in ("governance", "knowledge", "governance", "health")],
            "mixed": [
                {"capability_violation": True},
                {"scope_violation": True},
                {"domain": "governance"},
            ],
        }
        decisions = [self.RADAR.evaluate(events, token)["decision"] for events in scenarios.values()]
        assert decisions == ["intercept"] * len(decisions)

    def test_sliding_window_bounds(self) -> None:
        radar = DriftRadar(window=3)
        token = {"domain": "documents"}
        events = [{"capability_violation": True}] + [{"domain": "documents"}] * 10
        r = radar.evaluate(events, token)
        # 窗口外越权不再计入
        assert r["capability_violations"] == 0 and r["decision"] == "pass"


# ── e2e: 真实场景卡端到端仿真 ───────────────────────────────────

_REAL_CARDS = find_scene_cards_path(start=Path(__file__))

@pytest.mark.skipif(_REAL_CARDS is None, reason="scene-cards-v3.yaml not available (standalone CI)")
class TestRealSceneEndToEnd:
    """对 scene-document-review 与 scene-engineering-delivery 做四段仿真."""

    @pytest.fixture()
    def real_registry(self) -> SceneAnchorRegistry:
        return SceneAnchorRegistry.load(_REAL_CARDS)

    def _run_scenario(self, registry: SceneAnchorRegistry, scene_id: str, tmp_path: Path) -> dict:
        sandbox = tmp_path / scene_id
        sandbox.mkdir()
        token = registry.bind(f"e2e-{scene_id}", scene_id, allowed_roots=[str(sandbox)])
        refs = [str(r) for r in (token.get("capability_refs") or [])]
        # 真实卡 capability_refs 不可假设: 授予面为空时以首个 ref 派生写工具,
        # 无 ref 则正常轨迹退化为纯只读探活 (护栏语义不变, 注入类仍全拦截)
        granted_tool = (refs[0].split(":", 1)[0] + ".op") if refs else None
        outside_tool = "shell.exec" if (not refs or refs[0].split(":")[0] != "shell") else "shell2.exec"

        # 1) 正常轨迹: 探活 + (若授予) 沙盒内写 → 全部放行
        enforcer = GuardrailEnforcer(token=token, scope=DataScopeGuard(ws_root=tmp_path))
        normal_steps: list[tuple[str, Path | None]] = [
            ("get_status", None),
            ("probe:health", None),
        ]
        if granted_tool:
            normal_steps.append((granted_tool, sandbox / "out.md"))
        blocked_normal = []
        for tool, path in normal_steps:
            v = enforcer.enforce(tool=tool, path=path, domain=token["domain"])
            if v.blocked:
                blocked_normal.append((tool, v))
        assert blocked_normal == [], f"正常轨迹误报阻断: {blocked_normal}"

        # 2-5) 漂移注入四类 → 全部拦截
        injections = {
            "capability_violation": enforcer.enforce(tool=outside_tool),
            "scope_violation": enforcer.enforce(
                tool=granted_tool or outside_tool, path=tmp_path / "evil.yaml"
            ),
            "symlink_escape": self._symlink_escape(enforcer, sandbox, tmp_path),
            "drift_intercept": enforcer.enforce(tool=outside_tool),  # 累计越权 → radar 拦截
        }
        intercepted = {k: v.blocked for k, v in injections.items()}
        return {"scene_id": scene_id, "intercepted": intercepted, "verdicts": injections}

    @staticmethod
    def _symlink_escape(enforcer: GuardrailEnforcer, sandbox: Path, tmp_path: Path):
        outside = tmp_path / "outside-escape"
        outside.mkdir(exist_ok=True)
        link = sandbox / "escape-link"
        if not link.exists():
            link.symlink_to(outside, target_is_directory=True)
        return enforcer.enforce(tool="documents.cas_write", path=link / "x.yaml")

    def test_document_review_e2e(self, real_registry: SceneAnchorRegistry, tmp_path: Path) -> None:
        result = self._run_scenario(real_registry, "scene-document-review", tmp_path)
        assert result["scene_id"] == "scene-document-review"
        assert all(result["intercepted"].values()), result
        assert result["intercepted"]["symlink_escape"]

    def test_engineering_delivery_e2e(self, real_registry: SceneAnchorRegistry, tmp_path: Path) -> None:
        result = self._run_scenario(real_registry, "scene-engineering-delivery", tmp_path)
        assert all(result["intercepted"].values()), result

    def test_interception_rate_100_percent(
        self, real_registry: SceneAnchorRegistry, tmp_path: Path
    ) -> None:
        total = intercepted = 0
        for scene_id in ("scene-document-review", "scene-engineering-delivery"):
            result = self._run_scenario(real_registry, scene_id, tmp_path)
            for hit in result["intercepted"].values():
                total += 1
                intercepted += 1 if hit else 0
        assert total > 0 and intercepted == total, "仿真漂移场景必须 100% 拦截"
