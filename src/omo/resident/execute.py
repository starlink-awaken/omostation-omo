#!/usr/bin/env python3

"""execution-adapter — wire execution workers (Pi / multica) into the resident daemon (WP-G).

Registers a non-safe ``execution_agent`` handler: a matching event's payload
carries the instruction prompt; the handler builds a governed delivery_binding
and dispatches to a backend worker:

- pi (默认): 本地 Pi 推理 (pi-worker-adapter.run_worker, 需 omlxc/AetherForge 认证)
- multica: 托管 agent 自动化 (multica autopilot create+trigger, agent=Mika)

Because this handler executes external work it is non-safe — the daemon's
human-approval gate blocks it unless ``--yes`` is supplied.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE

EXECUTE_EVENTS = frozenset({"ExecutionRequested", "WorkPacketDispatched"})
DEFAULT_BACKEND = "pi"
MULTICA_AGENT = "Mika"  # multica 工作区默认执行 agent (runtime=local)
MULTICA_INTEGRATION_NOTE = "multica autopilot 已接入: _run_multica() 走 autopilot create(run_only) + trigger"


def _run_multica(*, prompt: str, run_id: str, timeout_seconds: int) -> dict[str, Any]:
    """Create a run_only autopilot via multica CLI and trigger it once."""
    try:
        create = subprocess.run(
            [
                "multica",
                "autopilot",
                "create",
                "--agent",
                MULTICA_AGENT,
                "--mode",
                "run_only",
                "--title",
                f"resident-exec-{run_id[:24]}",
                "--description",
                prompt[:2000],
                "--output",
                "json",
            ],
            capture_output=True,
            text=True,
            timeout=min(timeout_seconds + 10, 130),
            check=False,
        )
        if create.returncode != 0:
            return {"error": f"multica_create_failed: {create.stderr.strip()[:200]}"}
        created = json.loads(create.stdout)
        autopilot_id = str(
            created.get("id") or (created.get("autopilot") or {}).get("id") if isinstance(created, dict) else ""
        )
        if not autopilot_id:
            return {"error": f"multica_create_no_id: {create.stdout[:200]}"}
        trigger = subprocess.run(
            ["multica", "autopilot", "trigger", autopilot_id, "--output", "json"],
            capture_output=True,
            text=True,
            timeout=min(timeout_seconds + 10, 130),
            check=False,
        )
        if trigger.returncode != 0:
            return {"error": f"multica_trigger_failed: {trigger.stderr.strip()[:200]}", "autopilot_id": autopilot_id}
        return {
            "status": "dispatched",
            "backend": "multica",
            "autopilot_id": autopilot_id,
            "agent": MULTICA_AGENT,
            "trigger": json.loads(trigger.stdout) if trigger.stdout.strip() else {},
        }
    except Exception as exc:  # noqa: BLE001 - execution is best-effort
        return {"error": f"multica_execution_failed: {type(exc).__name__}: {exc}"}


def _load_pi_adapter() -> Any:
    spec = importlib.util.spec_from_file_location(
        "pi_worker_adapter",
        WORKSPACE / "bin" / "gac" / "pi-worker-adapter.py",
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["pi_worker_adapter"] = mod
    spec.loader.exec_module(mod)
    return mod


def _execute(event: dict[str, Any], *, execute: bool) -> dict[str, Any]:
    """Build delivery_binding from event payload and run a worker backend (receipt).

    M3.2 双载体: payload.backend 选择执行后端:
    - pi (默认): 本地 Pi 推理 (pi-worker-adapter, 需 omlxc/AetherForge 认证)
    - multica: 托管 agent 自动化 (multica autopilot create+trigger)
    """
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    prompt = str(payload.get("prompt") or payload.get("instruction") or "")
    if not prompt:
        return {"error": "execution_requires_prompt"}
    binding = {
        "run_id": str(
            event.get("workflow_run_id") or payload.get("run_id") or "exec-" + str(event.get("event_id", ""))[:8]
        ),
        "packet_id": str(payload.get("packet_id") or f"packet-{str(event.get('event_id'))[:8]}"),
        "packet_hash": str(payload.get("packet_hash") or "sha256:0" * 4),
        "instruction_binding": "resident-workpacket-v1",
    }
    timeout = min(int(payload.get("timeout_seconds") or 30), 120)
    backend = str(payload.get("backend") or DEFAULT_BACKEND)
    try:
        if backend == "multica":
            return _run_multica(prompt=prompt, run_id=binding["run_id"], timeout_seconds=timeout)
        if backend != "pi":
            return {"error": f"unknown_backend: {backend}", "binding": binding}
        pi = _load_pi_adapter()
        return pi.run_worker(
            prompt=prompt,
            execute=execute,
            workspace_root=WORKSPACE,
            delivery_binding=binding,
            timeout_seconds=timeout,
        )
    except Exception as exc:  # noqa: BLE001 - execution is best-effort
        return {"error": f"execution_failed: {type(exc).__name__}: {exc}", "binding": binding, "backend": backend}


def register_with_daemon(daemon_module: Any) -> None:
    """Register the execution handler as NON-safe (requires --yes approval)."""
    for event_type in EXECUTE_EVENTS:
        daemon_module.register_handler("execution_agent", _execution_handler, safe=False)


def _execution_handler(event: dict[str, Any]) -> None:
    receipt = _execute(event, execute=True)
    print(f"[execution-agent] receipt={receipt.get('status') or receipt.get('error', 'ok')[:60]}", file=sys.stderr)


def main(argv=None) -> int:
    import argparse  # noqa: PLC0415
    import json  # noqa: PLC0415

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", help="event JSON string")
    parser.add_argument("--dry-run", action="store_true", help="validate binding without executing")
    args = parser.parse_args(argv)
    event = json.loads(args.json) if args.json else json.loads(sys.stdin.read())
    receipt = _execute(event, execute=not args.dry_run)
    print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
