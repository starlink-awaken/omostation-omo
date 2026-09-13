"""guardrail/enforcer.py — 运行时防跑偏护栏 (BET-Y1Q4-T7-04).

三类护栏, 全部以场景锚令牌 (omo.scene.anchor.bind 签发) 为授权依据:

- CapabilityJail: 工具越界拦截 —— 写能力白名单外 deny。
- DataScopeGuard: 目录沙盒围栏 —— 写路径必须落在 allowed_roots 内,
  symlink 解析逃逸一律 deny。
- DriftRadar: 偏离度监控 —— 滑窗内越权/越界/跨域事件加权计分,
  超阈值 intercept。

守 BET-Y1Q4-T7-04 circuit breaker: 只读/探活类操作一律 warn 模式,
永不阻断 —— 系统基础健康感知不受护栏影响。
未锚定会话 (无令牌) 不经过本护栏, 存量路径零影响。
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA = "omo.guardrail.enforcer.v1"

# 只读/探活动词 (circuit breaker: 命中即 warn, 永不 deny)
_READONLY_TOOL_RE = re.compile(
    r"(?:^|[.:_-])(get|list|status|probe|search|read|health|show|describe|query|ping)(?:$|[.:_-])",
    re.IGNORECASE,
)

DECISION_ALLOW = "allow"
DECISION_WARN = "warn"
DECISION_DENY = "deny"
DECISION_INTERCEPT = "intercept"

# Drift Radar 缺省权重与阈值
DEFAULT_W_CAP = 2.0
DEFAULT_W_SCOPE = 2.0
DEFAULT_W_DOMAIN = 1.0
DEFAULT_THRESHOLD = 3.0
DEFAULT_WINDOW = 20


@dataclass
class Verdict:
    """单次护栏判定."""

    decision: str  # allow | warn | deny | intercept
    reason: str = ""
    code: str = ""

    @property
    def blocked(self) -> bool:
        return self.decision in (DECISION_DENY, DECISION_INTERCEPT)


def is_readonly_tool(tool: str) -> bool:
    """只读/探活判定 —— 命中动词表的工具永不硬阻断."""
    return bool(_READONLY_TOOL_RE.search(tool or ""))


def _tool_namespace(tool: str) -> str:
    return re.split(r"[.:_-]", tool or "", maxsplit=1)[0].lower()


class CapabilityJail:
    """工具越界拦截: 锚令牌 capability_refs 派生写能力白名单."""

    def check(
        self,
        tool: str,
        token: dict[str, Any],
        *,
        required_capability: str | None = None,
    ) -> Verdict:
        if is_readonly_tool(tool):
            return Verdict(DECISION_WARN, "readonly/probe tool never blocked", "readonly_probe_warn")
        refs = [str(r) for r in (token.get("capability_refs") or [])]
        if not refs:
            return Verdict(DECISION_DENY, "anchor grants no capabilities", "no_capability_granted")
        if required_capability:
            if required_capability in refs:
                return Verdict(DECISION_ALLOW, "capability granted", "")
            return Verdict(
                DECISION_DENY,
                f"capability {required_capability!r} outside anchor scope",
                "capability_violation",
            )
        ns = _tool_namespace(tool)
        ref_namespaces = {r.split(":", 1)[0].lower() for r in refs}
        if ns in ref_namespaces or tool in refs:
            return Verdict(DECISION_ALLOW, "tool namespace granted", "")
        return Verdict(
            DECISION_DENY,
            f"tool {tool!r} outside anchor capability namespaces {sorted(ref_namespaces)}",
            "capability_violation",
        )


class DataScopeGuard:
    """目录沙盒围栏: 写路径必须落在 allowed_roots 内, symlink 逃逸 deny."""

    def __init__(self, ws_root: Path | None = None) -> None:
        self._ws_root = (ws_root or Path.cwd()).resolve()

    def _resolve_root(self, root: str) -> Path:
        p = Path(root)
        return p.resolve() if p.is_absolute() else (self._ws_root / p).resolve()

    def check_path(self, path: str | Path, token: dict[str, Any], *, mode: str = "write") -> Verdict:
        if mode != "write":
            return Verdict(DECISION_WARN, "read path not fenced", "readonly_no_fence")
        roots = [str(r) for r in (token.get("allowed_roots") or [])]
        if not roots:
            return Verdict(DECISION_DENY, "anchor grants no write scope", "no_write_scope")
        target = Path(path)
        try:
            real = target.resolve(strict=False)
        except OSError as exc:
            return Verdict(DECISION_DENY, f"path unresolvable: {exc}", "path_unresolvable")
        # lexical 路径不做 symlink 解析 (normpath), 与 realpath 对比才能识别逃逸
        raw = target if target.is_absolute() else (self._ws_root / target)
        lexical = Path(os.path.normpath(raw))
        allowed = [self._resolve_root(r) for r in roots]
        inside = any(real == r or real.is_relative_to(r) for r in allowed)
        if not inside:
            # lexical 在沙盒内但 realpath 逃逸 → symlink 逃逸
            if any(lexical == r or lexical.is_relative_to(r) for r in allowed):
                return Verdict(
                    DECISION_DENY,
                    f"symlink escape: {target} -> {real}",
                    "symlink_escape",
                )
            return Verdict(DECISION_DENY, f"path outside allowed_roots: {target}", "scope_violation")
        return Verdict(DECISION_ALLOW, "path inside sandbox", "")


class DriftRadar:
    """偏离度监控: 滑窗事件加权计分, 超阈值 intercept.

    事件字段 (全部可选, 缺省按 0 计):
      capability_violation: bool —— 越权工具尝试
      scope_violation: bool —— 越界路径尝试 (含 symlink 逃逸)
      domain: str —— 动作所属域, ≠ 锚定场景域计跨域偏移
    """

    def __init__(
        self,
        *,
        w_capability: float = DEFAULT_W_CAP,
        w_scope: float = DEFAULT_W_SCOPE,
        w_domain: float = DEFAULT_W_DOMAIN,
        threshold: float = DEFAULT_THRESHOLD,
        window: int = DEFAULT_WINDOW,
    ) -> None:
        self.w_capability = w_capability
        self.w_scope = w_scope
        self.w_domain = w_domain
        self.threshold = threshold
        self.window = window

    def evaluate(self, events: list[dict[str, Any]], token: dict[str, Any]) -> dict[str, Any]:
        recent = events[-self.window :]
        cap_hits = sum(1 for e in recent if e.get("capability_violation"))
        scope_hits = sum(1 for e in recent if e.get("scope_violation"))
        scene_domain = str(token.get("domain") or "")
        domain_shifts = sum(1 for e in recent if e.get("domain") and str(e["domain"]) != scene_domain)
        score = self.w_capability * cap_hits + self.w_scope * scope_hits + self.w_domain * domain_shifts
        score = round(score, 3)
        if score >= self.threshold:
            decision = DECISION_INTERCEPT
        elif score > 0:
            decision = DECISION_WARN
        else:
            decision = "pass"
        return {
            "drift_score": score,
            "threshold": self.threshold,
            "capability_violations": cap_hits,
            "scope_violations": scope_hits,
            "domain_shifts": domain_shifts,
            "decision": decision,
        }


@dataclass
class GuardrailEnforcer:
    """组合护栏: jail + scope + radar, 输出统一裁决."""

    token: dict[str, Any]
    jail: CapabilityJail = field(default_factory=CapabilityJail)
    scope: DataScopeGuard = field(default_factory=DataScopeGuard)
    radar: DriftRadar = field(default_factory=DriftRadar)
    _events: list[dict[str, Any]] = field(default_factory=list, repr=False)

    def enforce(
        self,
        *,
        tool: str,
        path: str | Path | None = None,
        mode: str = "write",
        required_capability: str | None = None,
        domain: str | None = None,
    ) -> Verdict:
        """裁决一次动作; 只读/探活永远放行 (warn), 漂移超阈值 intercept."""
        verdict = self.jail.check(tool, self.token, required_capability=required_capability)
        event: dict[str, Any] = {"tool": tool, "capability_violation": False, "scope_violation": False}
        if verdict.code == "capability_violation":
            event["capability_violation"] = True
        if verdict.decision == DECISION_WARN and not is_readonly_tool(tool):
            # warn 且非只读 (理论不可达) —— 保守放行
            verdict = Verdict(DECISION_ALLOW, verdict.reason, verdict.code)
        if path is not None:
            scope_verdict = self.scope.check_path(path, self.token, mode=mode)
            if scope_verdict.code in ("scope_violation", "symlink_escape", "no_write_scope"):
                event["scope_violation"] = True
                if scope_verdict.blocked:
                    if is_readonly_tool(tool):
                        # circuit breaker: 只读路径检查只警告, 不阻断
                        self._events.append(event)
                        radar = self.radar.evaluate(self._events, self.token)
                        return Verdict(
                            DECISION_WARN,
                            f"readonly probe out of sandbox (radar={radar['drift_score']})",
                            "readonly_probe_warn",
                        )
                    return scope_verdict
        if verdict.blocked:
            self._events.append(event)
            radar = self.radar.evaluate(self._events, self.token)
            if radar["decision"] == DECISION_INTERCEPT:
                return Verdict(
                    DECISION_INTERCEPT,
                    f"drift radar intercept (score={radar['drift_score']})",
                    "drift_intercept",
                )
            return verdict
        if domain is not None:
            event["domain"] = domain
        self._events.append(event)
        radar = self.radar.evaluate(self._events, self.token)
        if radar["decision"] == DECISION_INTERCEPT:
            return Verdict(
                DECISION_INTERCEPT,
                f"drift radar intercept (score={radar['drift_score']})",
                "drift_intercept",
            )
        return verdict
