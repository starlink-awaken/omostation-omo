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
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from omo.resident import WORKSPACE

EXECUTE_EVENTS = frozenset({"ExecutionRequested", "WorkPacketDispatched"})
DEFAULT_BACKEND = "pi"
MULTICA_AGENT = "Mika"  # multica 工作区默认执行 agent (runtime=local)
MULTICA_INTEGRATION_NOTE = "multica autopilot 已接入: _run_multica() 走 autopilot create(run_only) + trigger"

# bet-ledger 契约常量 (与 bin/plan/bet-ledger.py 对齐, 不重复导入避免跨仓耦合)
SHA256_REF_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
INSTRUCTION_BINDING_KEYS = frozenset(
    {"instruction_ref", "instruction_version", "content_digest", "instruction_profile"}
)


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
        trigger_data: dict[str, Any] = json.loads(trigger.stdout) if trigger.stdout.strip() else {}
        # agent runtime 可能离线 → 平台跳过运行; 如实反映到顶层 status 避免误读为已执行
        top_status = "dispatched_skipped" if str(trigger_data.get("status")) == "skipped" else "dispatched"
        return {
            "status": top_status,
            "backend": "multica",
            "autopilot_id": autopilot_id,
            "agent": MULTICA_AGENT,
            "trigger": trigger_data,
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


def _default_binding(event: dict[str, Any], payload: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Best-effort binding used for multica / unknown backends (not pi)."""
    return {
        "run_id": run_id,
        "packet_id": str(payload.get("packet_id") or f"packet-{str(event.get('event_id'))[:8]}"),
        "packet_hash": str(payload.get("packet_hash") or "sha256:0" * 4),
        "instruction_binding": "resident-workpacket-v1",
    }


def _resolve_run_binding(run_id: str) -> dict[str, Any] | None:
    """Load the governed run file and build the exact bet-ledger binding.

    Reads ``.omo/_delivery/agent-workflows/runs/<run_id>.yaml`` and returns a
    delivery_binding whose fields come from the real run (packet_id from
    work_packet, work_packet_hash, dict instruction_binding) so the
    pi-worker-adapter's bet-ledger contract (run + packet + instruction triple)
    can pass.  Missing / mismatched / incomplete runs fail closed by returning
    None — the caller surfaces ``binding_run_unavailable`` instead of guessing.
    """
    run_path = WORKSPACE / ".omo" / "_delivery" / "agent-workflows" / "runs" / f"{run_id}.yaml"
    if not run_path.is_file():
        return None
    try:
        loaded = yaml.safe_load(run_path.read_text(encoding="utf-8")) or {}
    except (OSError, UnicodeError, yaml.YAMLError):
        return None
    if not isinstance(loaded, dict) or loaded.get("run_id") != run_id:
        return None
    work_packet = loaded.get("work_packet")
    work_packet_hash = loaded.get("work_packet_hash")
    instruction_binding = loaded.get("instruction_binding")
    if not isinstance(work_packet, dict) or not isinstance(work_packet_hash, str):
        return None
    if not isinstance(instruction_binding, dict) or set(instruction_binding) != INSTRUCTION_BINDING_KEYS:
        return None
    if SHA256_REF_RE.fullmatch(work_packet_hash) is None:
        return None
    packet_id = work_packet.get("packet_id")
    if not isinstance(packet_id, str) or not packet_id:
        return None
    return {
        "run_id": run_id,
        "packet_id": packet_id,
        "packet_hash": work_packet_hash,
        "instruction_binding": instruction_binding,
    }


def _run_cellpool(*, prompt: str, run_id: str, timeout_seconds: int, max_cells: int = 4) -> dict[str, Any]:
    """Dispatch a prompt through the CellPool elastic scheduling fabric.

    Creates (or reuses) a CellPool, dispatches the prompt as an episode to an
    isolated Cell, enforces a hard timeout, and automatically fails over if the
    Cell crashes.  This is the primary integration point for BET-Y1Q4-T6-23
    (Resident Daemon & AGE-v2 CellPool Integration).
    """
    from omo.resident.cell_pool import CellPool  # noqa: PLC0415 — avoid circular import at module level

    pool = CellPool(max_cells=max_cells, auto_scale=False)
    episode_id = f"ep-{run_id[:24]}"

    try:
        import asyncio  # noqa: PLC0415

        receipt = asyncio.run(pool.run_prompt(episode_id, prompt, timeout_seconds=timeout_seconds))
        receipt["run_id"] = run_id
        receipt["backend"] = "cellpool"
        receipt["binding"] = _default_binding({"event_id": run_id}, {}, run_id)
        if receipt.get("status") == "ok":
            receipt["status"] = "dispatched"
        return receipt
    except Exception as exc:  # noqa: BLE001 — best-effort
        return {
            "error": f"cellpool_execution_failed: {type(exc).__name__}: {exc}",
            "run_id": run_id,
            "backend": "cellpool",
            "binding": _default_binding({"event_id": run_id}, {}, run_id),
        }


def _execute(event: dict[str, Any], *, execute: bool) -> dict[str, Any]:
    """Build delivery_binding from event payload and run a worker backend (receipt).

    M3.2 三载体: payload.backend 选择执行后端:
    - pi (默认): 本地 Pi 推理 (pi-worker-adapter, 需 omlxc/AetherForge 认证)
    - multica: 托管 agent 自动化 (multica autopilot create+trigger)
    - cellpool: CellPool 弹性调度 (多 Cell 并发 + 超时/显存熔断 + 故障转移)
    """
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    prompt = str(payload.get("prompt") or payload.get("instruction") or "")
    if not prompt:
        return {"error": "execution_requires_prompt"}
    run_id = str(event.get("workflow_run_id") or payload.get("run_id") or "exec-" + str(event.get("event_id", ""))[:8])
    binding = _default_binding(event, payload, run_id)
    timeout = min(int(payload.get("timeout_seconds") or 30), 120)
    backend = str(payload.get("backend") or DEFAULT_BACKEND)
    max_cells = min(int(payload.get("max_cells") or 4), 16)
    try:
        if backend == "cellpool":
            return _run_cellpool(prompt=prompt, run_id=run_id, timeout_seconds=timeout, max_cells=max_cells)
        if backend == "multica":
            return _run_multica(prompt=prompt, run_id=run_id, timeout_seconds=timeout)
        if backend != "pi":
            return {"error": f"unknown_backend: {backend}", "binding": binding}
        resolved = _resolve_run_binding(run_id)
        if resolved is None:
            return {
                "error": "binding_run_unavailable",
                "run_id": run_id,
                "backend": backend,
                "hint": "pi 执行须有含完整 work_packet + instruction_binding 的真实 run 文件",
            }
        binding = resolved  # 异常时保留真实 run binding 以便排查
        pi = _load_pi_adapter()
        return pi.run_worker(
            prompt=prompt,
            execute=execute,
            workspace_root=WORKSPACE,
            delivery_binding=resolved,
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
