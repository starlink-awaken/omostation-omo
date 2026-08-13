"""Supervised controller for deterministic blueprint compilation and dispatch."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

import yaml
from ecos.ssot.mof.generated.control.mof_control_models import WorkPacket
from ecos.ssot.tools.work_packet_compiler import canonicalize, compute_packet_hash

from .omo_shared import load_yaml
from .omo_task_schema import validate_task_file
from .omo_worker_core import _require_admitted_worker, _require_worker_policy
from .omo_worker_dispatch import dispatch_task
from .workflow_dispatch import admit_workflow
from .workflow_mesh import WorkflowMeshStore


EXECUTABLE_BET_STATES = frozenset({"candidate", "in_progress", "review", "done"})


class BlueprintControlError(ValueError):
    """A blueprint cannot advance through the supervised control contract."""


@dataclass(frozen=True)
class CompiledBlueprintPacket:
    packet: dict[str, Any]
    packet_hash: str


def _safe_relative_path(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    path = PurePosixPath(text)
    canonical = path.as_posix() + ("/" if text.endswith("/") else "")
    if (
        not text
        or text in {".", "./"}
        or text.startswith("/")
        or "\\" in text
        or path.is_absolute()
        or ".." in path.parts
        or canonical != text
    ):
        raise BlueprintControlError(f"unsafe {field_name}: {text}")
    return text


def _required_string_list(container: Mapping[str, Any], field_name: str) -> list[str]:
    value = container.get(field_name)
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item.strip() for item in value)
    ):
        raise BlueprintControlError(f"{field_name} must be a non-empty string list")
    return [item.strip() for item in value]


class BlueprintControlService:
    """Coordinate existing BET, Task, ECOS, Mesh, and worker contracts."""

    def __init__(self, root: Path, *, omo_dir: str | Path = ".omo") -> None:
        self.root = root.resolve()
        self.omo_dir = Path(omo_dir)

    def _load_bet(self, bet_id: str) -> dict[str, Any]:
        ledger_path = self.root / "docs" / "plans" / "3y-bet-ledger.yaml"
        try:
            resolved_ledger = ledger_path.resolve(strict=True)
            resolved_ledger.relative_to(self.root)
            documents = list(
                yaml.safe_load_all(resolved_ledger.read_text(encoding="utf-8"))
            )
        except (OSError, ValueError, yaml.YAMLError) as exc:
            raise BlueprintControlError("canonical BET ledger is unavailable") from exc
        matches = [
            dict(bet)
            for document in documents
            if isinstance(document, Mapping)
            for bet in document.get("bets", [])
            if isinstance(bet, Mapping) and str(bet.get("id")) == bet_id
        ]
        if len(matches) != 1:
            raise BlueprintControlError(f"BET identity is not unique: {bet_id}")
        bet = matches[0]
        if bet.get("status") not in EXECUTABLE_BET_STATES:
            raise BlueprintControlError(f"BET is not executable: {bet_id}")
        return bet

    def _load_task(self, task_id: str) -> tuple[Path, dict[str, Any]]:
        active_dir = self.root / self.omo_dir / "tasks" / "active"
        matches: list[tuple[Path, dict[str, Any]]] = []
        for task_path in active_dir.glob("*.yaml"):
            try:
                task = load_yaml(task_path)
            except (OSError, ValueError):
                continue
            if task.get("id") == task_id:
                matches.append((task_path, task))
        if len(matches) != 1:
            raise BlueprintControlError(f"active Task identity is not unique: {task_id}")
        task_path, task = matches[0]
        errors = validate_task_file(task_path)
        if errors:
            raise BlueprintControlError("invalid active Task: " + "; ".join(errors))
        if task.get("human_approval_required") is not True:
            raise BlueprintControlError("Task must declare an explicit human gate")
        if not str(task.get("approval_ref") or "").strip():
            raise BlueprintControlError("Task human gate must bind an approval record")
        _safe_relative_path(task["approval_ref"], "approval reference")
        return task_path, task

    def _resolve_spec(self, spec_ref: str) -> tuple[str, str]:
        if not spec_ref.startswith("repo://"):
            raise BlueprintControlError("spec_ref must use repo://")
        relative = _safe_relative_path(spec_ref.removeprefix("repo://"), "spec path")
        try:
            path = (self.root / relative).resolve(strict=True)
            path.relative_to(self.root)
        except (OSError, ValueError) as exc:
            raise BlueprintControlError("spec_ref is outside the Workspace") from exc
        if not path.is_file():
            raise BlueprintControlError("spec_ref is not a regular file")
        digest = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
        return relative, digest

    def compile_packet(
        self,
        *,
        bet_id: str,
        task_id: str,
        spec_ref: str,
        spec_version: str,
        expires_at: str,
    ) -> CompiledBlueprintPacket:
        """Compile one immutable WorkPacket without writing control-plane state."""
        try:
            datetime.fromisoformat(expires_at)
        except ValueError as exc:
            raise BlueprintControlError("expires_at must be ISO-8601") from exc
        if not spec_version.strip():
            raise BlueprintControlError("spec_version is required")

        bet = self._load_bet(bet_id)
        task_path, task = self._load_task(task_id)
        spec_path, digest = self._resolve_spec(spec_ref)
        accepted = {
            "spec_ref": spec_ref,
            "spec_version": spec_version,
            "content_digest": digest,
        }
        accepted_bindings = bet.get("accepted_specifications")
        if not isinstance(accepted_bindings, list) or accepted not in [
            dict(binding)
            for binding in accepted_bindings
            if isinstance(binding, Mapping)
        ]:
            raise BlueprintControlError(
                "accepted specification binding or exact digest is missing"
            )

        read_field = "read_surfaces" if task.get("read_surfaces") else "source_docs"
        write_field = "write_surfaces" if task.get("write_surfaces") else "deliverables"
        read_surfaces = [
            _safe_relative_path(path, "read surface")
            for path in _required_string_list(task, read_field)
        ]
        write_surfaces = [
            _safe_relative_path(path, "write surface")
            for path in _required_string_list(task, write_field)
        ]
        if spec_path not in [surface.rstrip("/") for surface in read_surfaces]:
            raise BlueprintControlError("accepted specification is outside read surfaces")
        capabilities = _required_string_list(task, "required_capabilities")
        evidence = _required_string_list(task, "evidence_required")
        done_when = _required_string_list(bet, "done_when")
        verify = bet.get("verify")
        if not isinstance(verify, list) or not verify:
            raise BlueprintControlError("BET verify commands are required")
        verify_commands = [
            str(item.get("cmd", "")).strip()
            for item in verify
            if isinstance(item, Mapping) and str(item.get("cmd", "")).strip()
        ]
        if len(verify_commands) != len(verify):
            raise BlueprintControlError("BET verify commands are invalid")

        packet: dict[str, Any] = {
            "packet_id": "WP-BP-SEED",
            "schema_version": "work-packet/v2",
            "blueprint_ref": f"blueprint://supervised/{task_id}",
            "wave": str(bet.get("window") or "supervised"),
            "bet_id": bet_id,
            "strategic_outcome": str(bet.get("goal") or bet.get("title") or bet_id),
            "objective": str(task.get("title") or task_id),
            "why_now": str(bet.get("why_now") or "accepted specification is executable"),
            "status": str(bet["status"]),
            "authority": {
                "human_gate": True,
                "approval_ref": str(task["approval_ref"]),
                "expires_at": expires_at,
            },
            "scope": {
                "read_surfaces": read_surfaces,
                "write_surfaces": write_surfaces,
                "non_goals": list(bet.get("non_goals") or []),
            },
            "dependencies": {
                "task_id": task_id,
                "task_ref": str(task_path.relative_to(self.root)),
                "depends_on": list(task.get("depends_on") or []),
            },
            "acceptance": {
                "done_when": done_when,
                "verify_commands": verify_commands,
                "evidence_requirements": evidence,
            },
            "budgets": {"expires_at": expires_at},
            "rollback": {
                "strategy": "inverse_patch",
                "required": True,
                "owner": "controller",
                "instructions": str(
                    bet.get("rollback")
                    or "apply the controller-owned inverse patch and verify the baseline"
                ),
            },
            "circuit_breaker": {
                "conditions": [
                    str(bet.get("circuit_breaker") or "stop on scope or approval drift")
                ]
            },
            "assignment": {
                "task_id": task_id,
                "required_capabilities": capabilities,
                "worker_mode": "supervised",
                "expires_at": expires_at,
            },
            "spec_binding": {
                **accepted,
                "decision_ref": f"decision://accepted/{bet_id}",
            },
        }
        seed_hash = compute_packet_hash(canonicalize(packet))
        packet["packet_id"] = f"WP-BP-{seed_hash.removeprefix('sha256:')[:16]}"
        WorkPacket.model_validate(packet)
        packet_hash = compute_packet_hash(canonicalize(packet))
        if packet_hash != compute_packet_hash(canonicalize(packet)):
            raise BlueprintControlError("WorkPacket canonical hash is unstable")
        return CompiledBlueprintPacket(packet=packet, packet_hash=packet_hash)

    def _validate_compiled_packet(
        self, compiled: CompiledBlueprintPacket
    ) -> dict[str, Any]:
        packet = dict(compiled.packet)
        WorkPacket.model_validate(packet)
        measured_hash = compute_packet_hash(canonicalize(packet))
        if measured_hash != compiled.packet_hash:
            raise BlueprintControlError("compiled packet hash mismatch")
        return packet

    def dispatch_packet(
        self,
        compiled: CompiledBlueprintPacket,
        *,
        worker_id: str,
        capability_health: dict[str, Any],
        now: str | None = None,
        transport: str = "cli_prompt",
    ) -> dict[str, Any]:
        """Admit and project a packet without launching a provider process."""
        packet = self._validate_compiled_packet(compiled)
        task_id = str(packet.get("assignment", {}).get("task_id") or "")
        if not task_id:
            raise BlueprintControlError("packet assignment has no Task identity")
        task_path, task = self._load_task(task_id)
        task_ref = str(task_path.relative_to(self.root))
        if packet.get("dependencies", {}).get("task_ref") != task_ref:
            raise BlueprintControlError("packet Task binding mismatch")
        capabilities = _required_string_list(
            packet.get("assignment", {}), "required_capabilities"
        )
        write_surfaces = [
            _safe_relative_path(path, "write surface")
            for path in _required_string_list(packet.get("scope", {}), "write_surfaces")
        ]

        registry = load_yaml(
            self.root / self.omo_dir / "_truth" / "registry" / "workers.yaml"
        )
        worker = _require_admitted_worker(registry, worker_id, transport)
        _require_worker_policy(
            registry,
            worker,
            task,
            allowed_write_paths=write_surfaces,
            workflow_packet={"required_capabilities": capabilities},
        )

        identity = {
            "bet_id": str(packet["bet_id"]),
            "packet_id": str(packet["packet_id"]),
            "packet_hash": compiled.packet_hash,
            "task_ref": task_ref,
        }
        workflow_run_id = f"blueprint-{str(packet['packet_id']).lower()}"
        admission = admit_workflow(
            self.root,
            task_id=task_id,
            backend="supervised-worker",
            required_capabilities=capabilities,
            capability_health=capability_health,
            workflow_run_id=workflow_run_id,
            now=now,
            request_identity=identity,
            omo_dir=self.omo_dir,
        )
        worker_dispatch = dispatch_task(
            self.root,
            task_id=task_id,
            worker_id=worker_id,
            allowed_write_paths=write_surfaces,
            launch=False,
            transport=transport,
            workflow_packet=admission,
            now=now,
            omo_dir=self.omo_dir,
        )
        control_state = worker_dispatch.get("control_state")
        if not isinstance(control_state, Mapping) or control_state.get("transport") != "accepted":
            raise BlueprintControlError("transport acceptance was not durably projected")
        return {
            "state": "transport_accepted",
            "workflow_run_id": admission["workflow_run_id"],
            "admission_id": admission["admission"]["admission_id"],
            "packet_id": identity["packet_id"],
            "packet_hash": identity["packet_hash"],
            "bet_id": identity["bet_id"],
            **worker_dispatch,
        }

    def observe_dispatch(self, dispatch_result: Mapping[str, Any]) -> dict[str, Any]:
        """Observe projections without treating acknowledgements as model readiness."""
        run_id = str(dispatch_result.get("workflow_run_id") or "").strip()
        dispatch_ref = _safe_relative_path(
            dispatch_result.get("dispatch_path"), "dispatch path"
        )
        if not run_id:
            raise BlueprintControlError("dispatch observation requires workflow_run_id")
        dispatch_path = self.root / dispatch_ref
        dispatch = load_yaml(dispatch_path)
        snapshot = WorkflowMeshStore(self.root / self.omo_dir).snapshot(run_id)

        stem = dispatch_path.name.removesuffix("-dispatch.yaml")
        receipt_paths = [
            dispatch_path.with_name(f"{stem}-receipt.yaml"),
            dispatch_path.with_name(f"{stem}-receipt.json"),
        ]
        manifest_paths = [
            dispatch_path.with_name(f"{stem}-manifest.yaml"),
            dispatch_path.with_name(f"{stem}-manifest.json"),
        ]
        receipt_path = next((path for path in receipt_paths if path.is_file()), None)
        manifest_path = next((path for path in manifest_paths if path.is_file()), None)
        receipt = load_yaml(receipt_path) if receipt_path is not None else {}

        control_state = dict(dispatch.get("control_state") or {})
        state = (
            "transport_accepted"
            if control_state.get("transport") == "accepted"
            and snapshot.get("state") in {"dispatched", "running", "succeeded", "verified"}
            else "controller_approval_granted"
        )
        if receipt.get("readiness") == "model_output_observed":
            state = "model_output_observed"
            control_state["readiness"] = "model_output_observed"
        if manifest_path is not None and state == "model_output_observed":
            state = "candidate_collected"
        if snapshot.get("state") == "verified":
            state = "independently_verified"
        return {
            "state": state,
            "workflow_run_id": run_id,
            "mesh_state": snapshot.get("state"),
            "control_state": control_state,
            "receipt_observed": receipt_path is not None,
            "manifest_observed": manifest_path is not None,
        }
