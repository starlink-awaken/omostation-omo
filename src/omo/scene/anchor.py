"""scene/anchor.py — 场景导航锚点 (BET-Y1Q4-T7-04).

多 Agent 并发执行时, 把委派意图确定性地挂接到受管场景卡, 签发可校验的
锚令牌, 供护栏层 (omo.guardrail.enforcer) 做越界判定。

数据源: ``.omo/_truth/registry/scene-cards-v3.yaml`` (只读消费, 非目标
约束: 不修改任何场景卡定义)。

设计决策:
- 意图推荐为确定性关键词评分, 零模型调用; 无命中返回空列表 (诚实失败)。
- 锚令牌 digest = sha256(规范 JSON), verify() 重算防篡改。
- allowed_roots 默认空 —— "未显式授予即无写权限", 由调用方显式传入。
- 守 BET-Y1Q4-T7-04 circuit breaker: 本模块只做锚定与判定, 不执行任何
  系统动作, 天然不阻断探活。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA = "omo.scene.anchor.v1"

# 与 .omo/standards/scene-card-lifecycle.yaml 的五档生命周期对齐
ALLOWED_LIFECYCLES = frozenset({"draft", "shadow", "assisted", "supervised", "routine"})

_DEFAULT_CANDIDATES = (Path(".omo/_truth/registry/scene-cards-v3.yaml"),)

# 推荐评分权重: name 命中最强, scene_id 次之, domain/capability 佐证
_W_NAME = 3.0
_W_ID = 2.0
_W_DOMAIN = 1.0
_W_CAP = 1.0
_MAX_SCORE = _W_NAME + _W_ID + _W_DOMAIN + _W_CAP


class SceneAnchorError(Exception):
    """锚点操作失败 (附机器可读 code)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def find_scene_cards_path(start: Path | None = None) -> Path | None:
    """自 start (默认 cwd) 向上查找 scene-cards-v3.yaml; 找不到返回 None."""
    cur = (start or Path.cwd()).resolve()
    for cand in (cur, *cur.parents):
        for rel in _DEFAULT_CANDIDATES:
            p = cand / rel
            if p.is_file():
                return p
    return None


def _tokenize(text: str) -> set[str]:
    """CJK 二元组 + ASCII 词元, 小写."""
    tokens: set[str] = set()
    lowered = text.lower()
    for word in re.findall(r"[a-z0-9][a-z0-9_-]*", lowered):
        tokens.add(word)
    cjk = re.findall(r"[\u4e00-\u9fff]+", lowered)
    for chunk in cjk:
        if len(chunk) == 1:
            tokens.add(chunk)
        for i in range(len(chunk) - 1):
            tokens.add(chunk[i : i + 2])
    return tokens


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def token_digest(token: dict[str, Any]) -> str:
    """对不含 digest 字段的令牌载荷计算 sha256."""
    body = {k: v for k, v in token.items() if k != "digest"}
    return hashlib.sha256(_canonical_json(body).encode("utf-8")).hexdigest()


@dataclass
class Recommendation:
    """单条意图推荐."""

    scene_id: str
    name: str
    domain: str
    score: float
    confidence: float


@dataclass
class SceneAnchorRegistry:
    """受管场景卡注册表: 推荐 / 绑定 / 校验."""

    cards: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path | None = None) -> SceneAnchorRegistry:
        """从 scene-cards-v3.yaml 加载; path 缺省时向上查找."""
        import yaml

        resolved = path or find_scene_cards_path()
        if resolved is None or not resolved.is_file():
            raise SceneAnchorError(
                "scene_cards_unavailable",
                "scene-cards-v3.yaml not found; pass path explicitly",
            )
        payload = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
        scenes = payload.get("scenes") or []
        cards = {s["scene_id"]: s for s in scenes if isinstance(s, dict) and s.get("scene_id")}
        return cls(cards=cards)

    def get(self, scene_id: str) -> dict[str, Any] | None:
        return self.cards.get(scene_id)

    def __len__(self) -> int:
        return len(self.cards)

    # ── 意图推荐 ──────────────────────────────────────────────
    def recommend(self, text: str, top_k: int = 3) -> list[Recommendation]:
        """确定性意图推荐; 无命中返回空列表."""
        if not (text or "").strip():
            return []
        query = _tokenize(text)
        if not query:
            return []
        scored: list[Recommendation] = []
        for sid, card in self.cards.items():
            score = 0.0
            name_tokens = _tokenize(str(card.get("name") or ""))
            id_tokens = set(re.split(r"[-_]", sid)) | {sid}
            score += _W_NAME * len(query & name_tokens)
            score += _W_ID * len(query & {t.lower() for t in id_tokens if t})
            score += _W_DOMAIN * (1.0 if str(card.get("domain") or "").lower() in text.lower() else 0.0)
            cap_tokens: set[str] = set()
            for cap in card.get("capability_refs") or []:
                cap_tokens |= _tokenize(str(cap))
            score += _W_CAP * len(query & cap_tokens)
            if score > 0.0:
                scored.append(
                    Recommendation(
                        scene_id=sid,
                        name=str(card.get("name") or ""),
                        domain=str(card.get("domain") or ""),
                        score=round(score, 3),
                        confidence=round(min(1.0, score / _MAX_SCORE), 3),
                    )
                )
        scored.sort(key=lambda r: (-r.score, r.scene_id))
        return scored[: max(1, top_k)]

    # ── 锚定 ──────────────────────────────────────────────────
    def bind(
        self,
        session_id: str,
        scene_id: str,
        *,
        allowed_roots: list[str] | None = None,
        now: str | None = None,
    ) -> dict[str, Any]:
        """校验场景并签发锚令牌; allowed_roots 缺省为空 (无写权限)."""
        if not (session_id or "").strip():
            raise SceneAnchorError("missing_session", "session_id is required")
        card = self.cards.get(scene_id)
        if card is None:
            raise SceneAnchorError("unknown_scene", f"scene not found: {scene_id}")
        lifecycle = str(card.get("lifecycle") or "draft")
        if lifecycle not in ALLOWED_LIFECYCLES:
            raise SceneAnchorError(
                "invalid_lifecycle",
                f"scene lifecycle {lifecycle!r} outside managed set",
            )
        from datetime import UTC, datetime

        token = {
            "schema": SCHEMA,
            "anchor_id": f"anchor-{hashlib.sha256(f'{session_id}:{scene_id}'.encode()).hexdigest()[:12]}",
            "session_id": session_id,
            "scene_id": scene_id,
            "lifecycle": lifecycle,
            "domain": str(card.get("domain") or ""),
            "capability_refs": list(card.get("capability_refs") or []),
            "allowed_roots": [str(r) for r in (allowed_roots or [])],
            "issued_at": now or datetime.now(UTC).isoformat(),
        }
        token["digest"] = token_digest(token)
        return token

    def verify(self, token: dict[str, Any]) -> dict[str, Any]:
        """重算 digest 防篡改; 场景被移除时报 scene_revoked."""
        if not isinstance(token, dict) or not token.get("scene_id"):
            return {"ok": False, "reason": "malformed_token"}
        if token.get("digest") != token_digest(token):
            return {"ok": False, "reason": "digest_mismatch"}
        if self.cards.get(token["scene_id"]) is None:
            return {"ok": False, "reason": "scene_revoked"}
        return {"ok": True, "reason": ""}
