#!/usr/bin/env python3
"""OMO scene lifecycle CLI — scene execution, calibration, lifecycle management.

Extends OMO CLI with:
  omo scene execute <scene_id> [--signal <json>] [--dry-run]
  omo scene calibrate <scene_id> [--window 30]
  omo scene promote <scene_id> --to <level>
  omo scene demote <scene_id> --to <level> [--reason <reason>]
  omo scene status <scene_id>
  omo scene list [--domain <domain>] [--lifecycle <level>]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
SCENES_DIR = WORKSPACE_ROOT / ".omo" / "_truth" / "scenarios" / "v3"
JOURNEY_ENGINE = WORKSPACE_ROOT / "bin" / "ssot" / "journey-engine.py"
CALIBRATION_ENGINE = WORKSPACE_ROOT / "bin" / "ssot" / "calibration-engine.py"
SCENE_CARD_LIFECYCLE = WORKSPACE_ROOT / "bin" / "ssot" / "scene-card-lifecycle.py"


def _load_scene(scene_id: str) -> dict[str, Any] | None:
    import yaml

    if SCENES_DIR.is_dir():
        for p in SCENES_DIR.glob("*.yaml"):
            try:
                with open(p, encoding="utf-8") as f:
                    docs = list(yaml.safe_load_all(f))
                body = docs[-1] if len(docs) > 1 else docs[0]
                if isinstance(body, dict) and body.get("scene_id") == scene_id:
                    return body
            except Exception:
                continue
    return None


def _load_all_scenes() -> list[dict[str, Any]]:
    import yaml

    scenes = []
    if SCENES_DIR.is_dir():
        for p in sorted(SCENES_DIR.glob("*.yaml")):
            try:
                with open(p, encoding="utf-8") as f:
                    docs = list(yaml.safe_load_all(f))
                body = docs[-1] if len(docs) > 1 else docs[0]
                if isinstance(body, dict):
                    scenes.append(body)
            except Exception:
                continue
    return scenes


def cmd_execute(args: argparse.Namespace) -> int:
    cmd = [sys.executable, str(JOURNEY_ENGINE), "execute", args.scene_id]
    if args.signal:
        cmd.extend(["--signal", json.dumps(args.signal)])
    if args.dry_run:
        cmd.append("--dry-run")
    return subprocess.run(cmd, capture_output=False, cwd=str(WORKSPACE_ROOT)).returncode


def cmd_calibrate(args: argparse.Namespace) -> int:
    cmd = [sys.executable, str(CALIBRATION_ENGINE), "compute", "--scene-id", args.scene_id]
    if args.window:
        cmd.extend(["--window", str(args.window)])
    return subprocess.run(cmd, capture_output=False, cwd=str(WORKSPACE_ROOT)).returncode


def _card_path(scene_id: str) -> Path:
    return SCENES_DIR / f"{scene_id}.yaml"


def cmd_promote(args: argparse.Namespace) -> int:
    if not _load_scene(args.scene_id):
        print(f"ERROR: scene not found: {args.scene_id}", file=sys.stderr)
        return 2
    cmd = [
        sys.executable,
        str(SCENE_CARD_LIFECYCLE),
        "transition",
        "--scene-card",
        str(_card_path(args.scene_id)),
        "--tier",
        args.to_level,
        "--actor",
        args.actor,
    ]
    return subprocess.run(cmd, capture_output=False, cwd=str(WORKSPACE_ROOT)).returncode


def cmd_demote(args: argparse.Namespace) -> int:
    rc = cmd_promote(args)
    if rc == 0 and args.reason:
        print(f"  Reason: {args.reason}")
    return rc


def cmd_status(args: argparse.Namespace) -> int:
    card = _load_scene(args.scene_id)
    if not card:
        print(f"ERROR: scene not found: {args.scene_id}", file=sys.stderr)
        return 2
    print(f"Scene: {card.get('scene_id', '?')}")
    for k in ["name", "scene_class", "scene_type", "domain", "lifecycle", "activation", "owner", "approver", "bet"]:
        print(f"  {k.capitalize():<12}: {card.get(k, '?')}")
    caps = card.get("runtime", {}).get("sandbox", {}).get("capabilities", [])
    if caps:
        print("  Capabilities:")
        for c in caps:
            print(f"    - {c}")
    topo = card.get("topology", {})
    if topo.get("upstream"):
        print(f"  Upstream:    {', '.join(u.get('scene', '?') for u in topo['upstream'])}")
    if topo.get("downstream"):
        print(f"  Downstream:  {', '.join(d.get('scene', '?') for d in topo['downstream'])}")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    scenes = _load_all_scenes()
    if args.domain:
        scenes = [s for s in scenes if s.get("domain") == args.domain]
    if args.lifecycle:
        scenes = [s for s in scenes if s.get("lifecycle") == args.lifecycle]
    if not scenes:
        print("No scenes found.")
        return 0
    print(f"{'ID':<40} {'Class':<12} {'Lifecycle':<12} {'Activation':<12} {'Domain':<12}")
    print("-" * 92)
    for s in scenes:
        print(
            f"{s.get('scene_id', '?'):<40} {s.get('scene_class', '?'):<12} "
            f"{s.get('lifecycle', '?'):<12} {s.get('activation', '?'):<12} {s.get('domain', '?'):<12}"
        )
    print(f"\nTotal: {len(scenes)} scenes")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="omo scene", description="Scene lifecycle management — execution, calibration, promotion"
    )
    sub = parser.add_subparsers(dest="command")

    ep = sub.add_parser("execute", help="Execute a scene journey")
    ep.add_argument("scene_id")
    ep.add_argument("--signal", type=json.loads, default={})
    ep.add_argument("--dry-run", action="store_true")

    cp = sub.add_parser("calibrate", help="Compute calibration score")
    cp.add_argument("scene_id")
    cp.add_argument("--window", type=int, default=30)

    pp = sub.add_parser("promote", help="Promote scene lifecycle level")
    pp.add_argument("scene_id")
    pp.add_argument("--to", dest="to_level", required=True)
    pp.add_argument("--actor", default="omo-scene-cli")

    dp = sub.add_parser("demote", help="Demote scene lifecycle level")
    dp.add_argument("scene_id")
    dp.add_argument("--to", dest="to_level", required=True)
    dp.add_argument("--reason", default="")
    dp.add_argument("--actor", default="omo-scene-cli")

    sp = sub.add_parser("status", help="Show scene status")
    sp.add_argument("scene_id")

    lp = sub.add_parser("list", help="List scenes")
    lp.add_argument("--domain")
    lp.add_argument("--lifecycle")

    args = parser.parse_args(argv)
    command = args.command or "list"

    handlers = {
        "execute": cmd_execute,
        "calibrate": cmd_calibrate,
        "promote": cmd_promote,
        "demote": cmd_demote,
        "status": cmd_status,
        "list": cmd_list,
    }
    handler = handlers.get(command)
    if not handler:
        parser.print_help()
        return 1
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
