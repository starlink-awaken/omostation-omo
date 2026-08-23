#!/usr/bin/env python3

"""resident-resources — 共享资源层发现/消费入口 (M3.3).

读取 `.omo/_truth/registry/external-connection-fabric.yaml` 的统一资源注册表
(六类: knowledge_source / data_source / resource_provider / method_pack /
tool_capability / channel / model_provider / asset_source), 提供按 kind /
capability 检索的可消费清单。让 resident agent 体系能发现并路由到共享资源
(bos_uri 定位, capabilities 能力契约), 完成"共享资源层 → 运行时消费"闭环。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE

FABRIC_REGISTRY = WORKSPACE / ".omo" / "_truth" / "registry" / "external-connection-fabric.yaml"

# kind → 人类可读类别 (对齐 resource_kinds)
KIND_LABELS = {
    "knowledge_source": "知识",
    "data_source": "数据",
    "resource_provider": "资源提供方",
    "method_pack": "方法",
    "tool_capability": "工具",
    "channel": "通道",
    "model_provider": "模型",
    "asset_source": "资产",
}


def _load_registry() -> dict[str, Any]:
    import yaml  # noqa: PLC0415

    try:
        docs = [d for d in yaml.safe_load_all(FABRIC_REGISTRY.read_text(encoding="utf-8")) if d]
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"invalid fabric registry: {FABRIC_REGISTRY}: {exc}") from exc
    doc = docs[-1] if docs else {}
    if not isinstance(doc, dict):
        raise ValueError(f"invalid fabric registry: {FABRIC_REGISTRY}")
    return doc


def list_resources(*, kind: str | None = None, capability: str | None = None) -> list[dict[str, Any]]:
    """按 kind / capability 过滤返回资源清单."""
    registry = _load_registry()
    resources = registry.get("resources", []) if isinstance(registry, dict) else []
    out: list[dict[str, Any]] = []
    for res in resources:
        if not isinstance(res, dict):
            continue
        if kind and res.get("kind") != kind:
            continue
        if capability and capability not in (res.get("capabilities") or []):
            continue
        out.append(res)
    return out


def summarize(resources: list[dict[str, Any]]) -> dict[str, Any]:
    """汇总统计 (按 kind 分布)."""
    by_kind: dict[str, int] = {}
    for res in resources:
        k = str(res.get("kind") or "unknown")
        by_kind[k] = by_kind.get(k, 0) + 1
    return {
        "total": len(resources),
        "by_kind": by_kind,
        "kinds_covered": len(by_kind),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", help="按资源类别过滤 (如 knowledge_source/tool_capability)")
    parser.add_argument("--capability", help="按能力过滤 (如 search/discover/invoke)")
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    args = parser.parse_args(argv)

    resources = list_resources(kind=args.kind, capability=args.capability)
    if args.json:
        print(
            json.dumps(
                {"summary": summarize(resources), "resources": resources},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    summary = summarize(resources)
    print(f"共享资源: {summary['total']} 个, 覆盖 {summary['kinds_covered']} 类")
    if args.kind:
        print(f"  过滤: kind={args.kind}")
    if args.capability:
        print(f"  过滤: capability={args.capability}")
    for res in resources:
        label = KIND_LABELS.get(str(res.get("kind")), str(res.get("kind")))
        print(
            f"  - {res.get('id')} [{label}] "
            f"provider={res.get('provider')} bos_uri={res.get('bos_uri')} "
            f"caps={','.join(res.get('capabilities') or [])}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
