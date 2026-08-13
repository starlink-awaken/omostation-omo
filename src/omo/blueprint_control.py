"""Supervised controller for deterministic blueprint compilation and dispatch."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import signal
import subprocess
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable, NoReturn

import yaml
from ecos.ssot.mof.generated.control.mof_control_models import WorkPacket
from ecos.ssot.tools.work_packet_compiler import canonicalize, compute_packet_hash
from ecos.ssot.tools.work_packet_compiler import (
    build_command_check,
    build_verification_receipt,
)

from .orchestration_contract import OrchestrationContractCoordinator
from .omo_io import write_text_atomic
from .omo_shared import load_yaml
from .omo_task_schema import validate_task_file
from .omo_worker_core import _require_admitted_worker, _require_worker_policy
from .omo_worker_dispatch import dispatch_task
from .workflow_dispatch import admit_workflow
from .workflow_mesh import WorkflowMeshStore, new_workflow_event


EXECUTABLE_BET_STATES = frozenset({"candidate", "in_progress", "review", "done"})


class BlueprintControlError(ValueError):
    """A blueprint cannot advance through the supervised control contract."""


@dataclass(frozen=True)
class CompiledBlueprintPacket:
    packet: dict[str, Any]
    packet_hash: str


Runner = Callable[..., Mapping[str, Any]]


def _sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _canonical_receipt_digest(receipt: Mapping[str, Any]) -> str:
    projected = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    canonical = json.dumps(
        projected, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _is_sha256(value: Any, *, prefixed: bool) -> bool:
    text = str(value or "")
    if prefixed:
        if not text.startswith("sha256:"):
            return False
        text = text.removeprefix("sha256:")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


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
            "budgets": {
                "expires_at": expires_at,
                "max_changed_files": len(write_surfaces),
            },
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

    def _git(
        self,
        args: list[str],
        *,
        input_bytes: bytes | None = None,
        index_file: Path | None = None,
        timeout: int = 30,
        check: bool = True,
    ) -> subprocess.CompletedProcess[bytes]:
        env = os.environ.copy()
        if index_file is not None:
            env["GIT_INDEX_FILE"] = str(index_file)
        try:
            result = subprocess.run(
                ["git", *args],
                cwd=self.root,
                env=env,
                input=input_bytes,
                capture_output=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise BlueprintControlError("git measurement timed out") from exc
        if check and result.returncode != 0:
            raise BlueprintControlError("git measurement failed")
        return result

    def _snapshot_tree(self, index_file: Path) -> str:
        if index_file.exists():
            index_file.unlink()
        self._git(["read-tree", "HEAD"], index_file=index_file)
        self._git(["add", "-A"], index_file=index_file)
        return self._git(["write-tree"], index_file=index_file).stdout.decode().strip()

    def _tree_scope_digest(self, tree: str, surfaces: list[str]) -> str:
        paths = [surface.rstrip("/") for surface in surfaces]
        listing = self._git(["ls-tree", "-r", tree, "--", *paths]).stdout
        return _sha256(listing)

    def _restore_controller_paths(
        self, index_file: Path, baseline_tree: str, post_tree: str
    ) -> str:
        controller_paths = [
            (self.omo_dir / "_knowledge" / "workflow-mesh" / "events.jsonl").as_posix()
        ]
        present = [
            path
            for path in controller_paths
            if self._git(["ls-tree", baseline_tree, "--", path]).stdout
            or self._git(["ls-tree", post_tree, "--", path]).stdout
        ]
        if present:
            self._git(
                ["reset", "-q", baseline_tree, "--", *present],
                index_file=index_file,
            )
            return self._git(["write-tree"], index_file=index_file).stdout.decode().strip()
        return post_tree

    def _dispatch_context(
        self, dispatch_result: Mapping[str, Any]
    ) -> tuple[str, str, str, str]:
        run_id = str(dispatch_result.get("workflow_run_id") or "").strip()
        admission_id = str(dispatch_result.get("admission_id") or "").strip()
        dispatch_id = str(dispatch_result.get("dispatch_id") or "").strip()
        if not all((run_id, admission_id, dispatch_id)):
            raise BlueprintControlError("dispatch identity is incomplete")
        snapshot = WorkflowMeshStore(self.root / self.omo_dir).snapshot(run_id)
        admission = snapshot.get("admission")
        step_ids = admission.get("step_run_ids") if isinstance(admission, Mapping) else None
        if not isinstance(step_ids, list) or len(step_ids) != 1:
            raise BlueprintControlError("dispatch does not bind one admitted step")
        return run_id, admission_id, dispatch_id, str(step_ids[0])

    @staticmethod
    def _default_runner(
        *,
        argv: list[str],
        workspace_root: Path,
        receipt_path: Path,
        timeout_seconds: int,
        on_process_started: Callable[[], None],
    ) -> Mapping[str, Any]:
        command = [*argv, "--receipt", str(receipt_path)]
        process = subprocess.Popen(
            command,
            cwd=workspace_root,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        on_process_started()
        try:
            stdout, stderr = process.communicate(timeout=timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired as cleanup_exc:
                    raise BlueprintControlError(
                        "bounded runner cleanup_unconfirmed"
                    ) from cleanup_exc
            if process.poll() is None:
                raise BlueprintControlError("bounded runner cleanup_unconfirmed") from exc
            raise BlueprintControlError("bounded runner timed out") from exc
        return {"returncode": process.returncode, "stdout": stdout, "stderr": stderr}

    def execute_and_collect(
        self,
        compiled: CompiledBlueprintPacket,
        dispatch_result: Mapping[str, Any],
        *,
        runner: Runner | None = None,
        timeout_seconds: int = 900,
    ) -> dict[str, Any]:
        """Execute one supervised worker and compile independently measured evidence."""
        packet = self._validate_compiled_packet(compiled)
        if dispatch_result.get("state") != "transport_accepted":
            raise BlueprintControlError("dispatch is not transport accepted")
        if (
            dispatch_result.get("packet_id") != packet["packet_id"]
            or dispatch_result.get("packet_hash") != compiled.packet_hash
            or dispatch_result.get("bet_id") != packet["bet_id"]
        ):
            raise BlueprintControlError("dispatch packet binding mismatch")
        run_id, admission_id, dispatch_id, step_run_id = self._dispatch_context(
            dispatch_result
        )
        store = WorkflowMeshStore(self.root / self.omo_dir)
        allowed = _required_string_list(packet["scope"], "write_surfaces")
        dispatch_path = self.root / str(dispatch_result["dispatch_path"])
        projection_path = dispatch_path.with_name(
            dispatch_path.name.removesuffix("-dispatch.yaml") + "-manifest.json"
        )
        if projection_path.is_file():
            projection = json.loads(projection_path.read_text(encoding="utf-8"))
            binding = projection.get("manifest", {})
            if (
                binding.get("packet_id") == packet["packet_id"]
                and binding.get("packet_hash") == compiled.packet_hash
            ):
                return projection
            raise BlueprintControlError("manifest_conflict")
        step_started = False
        baseline_tree = ""
        baseline_digest = ""

        def on_process_started() -> None:
            nonlocal step_started
            if step_started:
                return
            store.append(
                new_workflow_event(
                    "StepStarted",
                    run_id,
                    producer="omo-blueprint-control",
                    payload={
                        "step_run_id": step_run_id,
                        "admission_id": admission_id,
                        "dispatch_id": dispatch_id,
                    },
                    idempotency_key=f"{run_id}:step-started:{dispatch_id}",
                )
            )
            step_started = True

        def reject_execution(message: str, reason: str) -> NoReturn:
            if step_started and store.snapshot(run_id).get("state") == "running":
                store.append(
                    new_workflow_event(
                        "StepFailed",
                        run_id,
                        producer="omo-blueprint-control",
                        payload={
                            "step_run_id": step_run_id,
                            "admission_id": admission_id,
                            "dispatch_id": dispatch_id,
                            "reason": reason,
                        },
                        idempotency_key=f"{run_id}:step-failed:{dispatch_id}",
                    )
                )
            raise BlueprintControlError(message)

        with tempfile.TemporaryDirectory(prefix="omo-blueprint-") as directory:
            temp = Path(directory)
            before_index = temp / "before.index"
            after_index = temp / "after.index"
            receipt_path = temp / "adapter-receipt.json"
            dispatch_doc = load_yaml(dispatch_path)
            worker_id = str(dispatch_doc.get("worker_id") or "").strip()
            if not worker_id:
                raise BlueprintControlError("dispatch worker identity is missing")
            launch_command = str(
                dispatch_doc.get("execution", {}).get("launch_command") or ""
            )
            argv = shlex.split(launch_command)
            if runner is None and not argv:
                raise BlueprintControlError("dispatch has no bounded launch command")
            baseline_tree = self._snapshot_tree(before_index)
            baseline_digest = self._tree_scope_digest(baseline_tree, allowed)
            try:
                result = (runner or self._default_runner)(
                    argv=argv,
                    workspace_root=self.root,
                    receipt_path=receipt_path,
                    timeout_seconds=timeout_seconds,
                    on_process_started=on_process_started,
                )
            except Exception:
                if step_started:
                    store.append(
                        new_workflow_event(
                            "StepFailed",
                            run_id,
                            producer="omo-blueprint-control",
                            payload={
                                "step_run_id": step_run_id,
                                "admission_id": admission_id,
                                "dispatch_id": dispatch_id,
                                "reason": "bounded_runner_failed",
                            },
                            idempotency_key=f"{run_id}:step-failed:{dispatch_id}",
                        )
                    )
                raise
            if not receipt_path.is_file():
                reject_execution("model output receipt is missing", "receipt_missing")
            try:
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                reject_execution("model output receipt is invalid", "receipt_invalid")
            if receipt.get("receipt_sha256") != _canonical_receipt_digest(receipt):
                reject_execution("adapter receipt digest mismatch", "receipt_digest_mismatch")
            supervision = receipt.get("supervision")
            provider_review = (
                supervision.get("provider_review")
                if isinstance(supervision, Mapping)
                else None
            )
            if (
                not isinstance(supervision, Mapping)
                or supervision.get("controller_approval") != "granted"
            ):
                reject_execution("controller approval receipt is invalid", "approval_mismatch")
            if provider_review == "human_required":
                if step_started:
                    store.append(
                        new_workflow_event(
                            "StepFailed",
                            run_id,
                            producer="omo-blueprint-control",
                            payload={
                                "step_run_id": step_run_id,
                                "admission_id": admission_id,
                                "dispatch_id": dispatch_id,
                                "reason": "human_approval_required",
                            },
                            idempotency_key=f"{run_id}:step-failed:{dispatch_id}",
                        )
                    )
                raise BlueprintControlError("provider human approval is unresolved")
            if provider_review != "completed_without_observed_escalation":
                reject_execution("provider review is unresolved", "provider_review_unresolved")
            if receipt.get("worker") != "codex":
                reject_execution("adapter worker identity mismatch", "worker_mismatch")
            if not all(
                (
                    _is_sha256(receipt.get("baseline_digest"), prefixed=True),
                    _is_sha256(receipt.get("post_digest"), prefixed=True),
                    _is_sha256(receipt.get("patch_digest"), prefixed=True),
                    _is_sha256(receipt.get("output_sha256"), prefixed=False),
                )
            ):
                reject_execution("adapter digest fields are invalid", "adapter_digest_invalid")
            if receipt.get("readiness") != "model_output_observed":
                reject_execution("valid model output was not observed", "model_output_missing")
            if receipt.get("status") != "succeeded" or int(result.get("returncode", 1)) != 0:
                if step_started:
                    store.append(
                        new_workflow_event(
                            "StepFailed",
                            run_id,
                            producer="omo-blueprint-control",
                            payload={
                                "step_run_id": step_run_id,
                                "admission_id": admission_id,
                                "dispatch_id": dispatch_id,
                                "reason": "worker_nonzero",
                            },
                            idempotency_key=f"{run_id}:step-failed:{dispatch_id}",
                        )
                    )
                raise BlueprintControlError("bounded runner failed")
            if not step_started:
                raise BlueprintControlError("provider process start was not observed")

            post_tree = self._snapshot_tree(after_index)
            post_tree = self._restore_controller_paths(
                after_index, baseline_tree, post_tree
            )
            patch = self._git(
                ["diff", "--binary", baseline_tree, post_tree, "--"],
                index_file=after_index,
            ).stdout
            changed_paths = sorted(
                value.decode()
                for value in self._git(
                    ["diff", "--name-only", "-z", baseline_tree, post_tree, "--"],
                    index_file=after_index,
                ).stdout.split(b"\0")
                if value
            )
            if receipt.get("changed_paths") != changed_paths:
                reject_execution("adapter changed paths do not match Git", "changed_paths_mismatch")
            patch_digest = _sha256(patch)
            if receipt.get("patch_digest") != patch_digest:
                reject_execution("adapter patch digest does not match Git", "patch_digest_mismatch")
            if any(
                not any(
                    changed == surface.rstrip("/")
                    or (surface.endswith("/") and changed.startswith(surface))
                    for surface in allowed
                )
                for changed in changed_paths
            ):
                reject_execution("Git delta contains an out-of-scope path", "write_scope_violation")
            patch_oid = self._git(["hash-object", "-w", "--stdin"], input_bytes=patch).stdout.decode().strip()

        checks = [
            build_command_check(
                ["bounded-runner"],
                0,
                str(receipt.get("output_sha256") or "model output observed"),
            )
        ]
        claims = [
            {
                "acceptance_id": f"AC{index}",
                "assertion": str(assertion),
                "evidence_refs": [f"git-object://{patch_oid}"],
            }
            for index, assertion in enumerate(packet["acceptance"]["done_when"], 1)
        ]
        assignment_id = "ASG-" + hashlib.sha256(dispatch_id.encode()).hexdigest()[:16]
        manifest = {
            "packet_id": packet["packet_id"],
            "packet_hash": compiled.packet_hash,
            "assignment_id": assignment_id,
            "agent_id": worker_id,
            "status": "candidate",
            "changed_paths": changed_paths,
            "claims": claims,
            "checks": [
                {
                    "command": check["command"],
                    "returncode": check["returncode"],
                    "stdout_hash": check["stdout_hash"],
                }
                for check in checks
            ],
            "recommended_next": "verify",
            "surface_delta": {"files": len(changed_paths), "loc": len(patch.splitlines())},
            "artifact_refs": [f"git-object://{patch_oid}"],
        }
        transport_receipt: dict[str, Any] = {
            "receipt_id": f"codex:{dispatch_id}",
            "workflow_run_id": run_id,
            "step_run_id": step_run_id,
            "bet_id": packet["bet_id"],
            "packet_id": packet["packet_id"],
            "packet_hash": compiled.packet_hash,
            "assignment_id": assignment_id,
            "dispatch_id": dispatch_id,
            "worker_id": worker_id,
            "output_digest": str(receipt["output_sha256"]),
            "changed_paths": changed_paths,
            "observed_at": str(receipt.get("completed_at") or datetime.now().astimezone().isoformat()),
            "provenance_ref": f"receipt://codex/{dispatch_id}",
        }
        transport_receipt["receipt_digest"] = compute_packet_hash(
            canonicalize(transport_receipt)
        )
        store.append(
            new_workflow_event(
                "WorkflowSucceeded",
                run_id,
                producer="omo-blueprint-control",
                payload={"packet_id": packet["packet_id"]},
                idempotency_key=f"{run_id}:candidate-succeeded:{dispatch_id}",
            )
        )
        evidence = OrchestrationContractCoordinator._for_workspace(
            self.root / self.omo_dir, self.root
        ).record_candidate(
            workflow_run_id=run_id,
            step_run_id=step_run_id,
            packet=packet,
            manifest=manifest,
            transport_receipt=transport_receipt,
        )
        projection = {
            "state": "candidate_collected",
            "manifest": manifest,
            "transport_receipt": transport_receipt,
            "adapter_receipt_digest": receipt["receipt_sha256"],
            "baseline_tree": baseline_tree,
            "baseline_digest": baseline_digest,
            "post_tree": post_tree,
            "write_surfaces": allowed,
            "patch_ref": f"git-object://{patch_oid}",
            "patch_digest": patch_digest,
            "evidence": evidence,
        }
        write_text_atomic(
            projection_path,
            json.dumps(projection, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        )
        return projection

    @staticmethod
    def _default_verifier(
        *, argv: list[str], workspace_root: Path, timeout_seconds: int
    ) -> Mapping[str, Any]:
        try:
            result = subprocess.run(
                argv,
                cwd=workspace_root,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=timeout_seconds,
                check=False,
                shell=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise BlueprintControlError("verification command timed out") from exc
        return {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    def verify_candidate(
        self,
        compiled: CompiledBlueprintPacket,
        dispatch_result: Mapping[str, Any],
        collected: Mapping[str, Any],
        *,
        verifier: Runner | None = None,
        timeout_seconds: int = 120,
    ) -> dict[str, Any]:
        """Directly replay packet checks; accept is the only verification path."""
        packet = self._validate_compiled_packet(compiled)
        manifest = collected.get("manifest")
        if not isinstance(manifest, Mapping):
            raise BlueprintControlError("candidate manifest is missing")
        checks = []
        all_green = True
        for command in _required_string_list(packet["acceptance"], "verify_commands"):
            argv = shlex.split(command)
            if not argv:
                raise BlueprintControlError("verification command is empty")
            result = (verifier or self._default_verifier)(
                argv=argv,
                workspace_root=self.root,
                timeout_seconds=timeout_seconds,
            )
            returncode = int(result.get("returncode", 1))
            stdout = result.get("stdout", b"")
            stdout_bytes = stdout.encode() if isinstance(stdout, str) else bytes(stdout)
            checks.append(build_command_check(argv, returncode, stdout_bytes.decode(errors="replace")))
            all_green = all_green and returncode == 0
        receipt = build_verification_receipt(
            packet=packet,
            candidate_packet_hash=compiled.packet_hash,
            measured_packet_hash=compute_packet_hash(canonicalize(packet)),
            executor_model_family="codex",
            verifier_model_family="deterministic-runner",
            verdict="accept" if all_green else "reject",
            read_only=True,
            direct_measurement=True,
            checks=[
                {
                    "command": check["command"],
                    "returncode": check["returncode"],
                    "stdout_hash": check["stdout_hash"],
                }
                for check in checks
            ],
        )
        if not all_green:
            return self.rollback_candidate(dispatch_result, collected)
        event = OrchestrationContractCoordinator._for_workspace(
            self.root / self.omo_dir, self.root
        ).accept_verification(
            workflow_run_id=str(dispatch_result["workflow_run_id"]),
            packet=packet,
            manifest=manifest,
            verification_receipt=receipt,
        )
        return {
            "state": "independently_verified",
            "verification": event,
            "receipt_hash": receipt.receipt_hash,
        }

    def _candidate_binds_dispatch(
        self,
        *,
        store: WorkflowMeshStore,
        run_id: str,
        step_run_id: str,
        dispatch_id: str,
        dispatch_result: Mapping[str, Any],
        collected: Mapping[str, Any],
    ) -> bool:
        manifest = collected.get("manifest")
        receipt = collected.get("transport_receipt")
        source_event = collected.get("evidence")
        if not all(
            isinstance(value, Mapping)
            for value in (manifest, receipt, source_event)
        ):
            return False
        assert isinstance(manifest, Mapping)
        assert isinstance(receipt, Mapping)
        assert isinstance(source_event, Mapping)
        packet_id = dispatch_result.get("packet_id")
        packet_hash = dispatch_result.get("packet_hash")
        bet_id = dispatch_result.get("bet_id")
        assignment_id = manifest.get("assignment_id")
        patch_ref = collected.get("patch_ref")
        artifact_refs = manifest.get("artifact_refs")
        surfaces = collected.get("write_surfaces")
        if (
            not isinstance(patch_ref, str)
            or not patch_ref.startswith("git-object://")
            or not isinstance(artifact_refs, list)
            or patch_ref not in artifact_refs
            or not isinstance(surfaces, list)
            or not surfaces
            or not all(isinstance(surface, str) for surface in surfaces)
            or not _is_sha256(collected.get("patch_digest"), prefixed=True)
            or not _is_sha256(collected.get("baseline_digest"), prefixed=True)
        ):
            return False
        expected_receipt = {
            "workflow_run_id": run_id,
            "step_run_id": step_run_id,
            "dispatch_id": dispatch_id,
            "packet_id": packet_id,
            "packet_hash": packet_hash,
            "bet_id": bet_id,
            "assignment_id": assignment_id,
        }
        if any(receipt.get(key) != value for key, value in expected_receipt.items()):
            return False
        if (
            manifest.get("packet_id") != packet_id
            or manifest.get("packet_hash") != packet_hash
            or not isinstance(assignment_id, str)
            or not assignment_id
        ):
            return False
        receipt_digest = receipt.get("receipt_digest")
        canonical_receipt = {
            key: value for key, value in receipt.items() if key != "receipt_digest"
        }
        if receipt_digest != compute_packet_hash(canonicalize(canonical_receipt)):
            return False
        payload = source_event.get("payload")
        factors = payload.get("decision_factors") if isinstance(payload, Mapping) else None
        if (
            source_event.get("workflow_run_id") != run_id
            or source_event.get("event_type") != "EvidenceRecorded"
            or not isinstance(factors, Mapping)
            or factors.get("packet_id") != packet_id
            or factors.get("packet_hash") != packet_hash
            or factors.get("bet_id") != bet_id
            or factors.get("assignment_id") != assignment_id
            or factors.get("dispatch_id") != dispatch_id
            or factors.get("receipt_digest") != receipt_digest
            or factors.get("artifact_refs_digest")
            != compute_packet_hash(
                json.dumps(
                    sorted(str(ref) for ref in artifact_refs),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            or payload.get("step_run_id") != step_run_id
        ):
            return False
        return any(
            event.get("event_id") == source_event.get("event_id")
            and event.get("payload") == payload
            and event.get("workflow_run_id") == run_id
            for event in store.events()
        )

    def rollback_candidate(
        self,
        dispatch_result: Mapping[str, Any],
        collected: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Reverse only the controller-owned Git blob and prove baseline identity."""
        run_id, admission_id, dispatch_id, step_run_id = self._dispatch_context(
            dispatch_result
        )
        store = WorkflowMeshStore(self.root / self.omo_dir)
        if not self._candidate_binds_dispatch(
            store=store,
            run_id=run_id,
            step_run_id=step_run_id,
            dispatch_id=dispatch_id,
            dispatch_result=dispatch_result,
            collected=collected,
        ):
            return {
                "state": "rollback_unconfirmed",
                "reason": "candidate_binding_mismatch",
            }
        if store.snapshot(run_id).get("state") == "closed":
            return {"state": "closed", "baseline_tree": collected.get("baseline_tree")}
        started = new_workflow_event(
            "CompensationStarted",
            run_id,
            producer="omo-blueprint-control",
            payload={
                "step_run_id": step_run_id,
                "admission_id": admission_id,
                "dispatch_id": dispatch_id,
            },
            idempotency_key=f"{run_id}:compensation:{dispatch_id}",
        )
        existing_types = [
            event["event_type"]
            for event in store.events()
            if event.get("workflow_run_id") == run_id
        ]
        if "CompensationStarted" not in existing_types:
            store.append(started)
        surfaces = list(collected["write_surfaces"])
        patch_ref = str(collected.get("patch_ref") or "")
        oid = patch_ref.removeprefix("git-object://")
        if not patch_ref.startswith("git-object://") or len(oid) != 40:
            return {"state": "rollback_unconfirmed", "reason": "patch_ref_invalid"}
        patch_result = self._git(["cat-file", "blob", oid], check=False)
        if patch_result.returncode != 0:
            return {"state": "rollback_unconfirmed", "reason": "patch_missing"}
        patch = patch_result.stdout
        if collected.get("patch_digest") != _sha256(patch):
            return {"state": "rollback_unconfirmed", "reason": "patch_tampered"}
        check = self._git(
            ["apply", "--reverse", "--check", "--whitespace=nowarn", "-"],
            input_bytes=patch,
            check=False,
        )
        if check.returncode != 0:
            return {"state": "rollback_unconfirmed", "reason": "preimage_mismatch"}
        applied = self._git(
            ["apply", "--reverse", "--whitespace=nowarn", "-"],
            input_bytes=patch,
            check=False,
        )
        if applied.returncode != 0:
            return {"state": "rollback_unconfirmed", "reason": "reverse_apply_failed"}
        with tempfile.TemporaryDirectory(prefix="omo-rollback-") as directory:
            restored_tree = self._snapshot_tree(Path(directory) / "restored.index")
        restored_digest = self._tree_scope_digest(restored_tree, surfaces)
        if restored_digest != collected.get("baseline_digest"):
            return {"state": "rollback_unconfirmed", "reason": "baseline_mismatch"}
        for event_type in (
            "WorkflowRecovered",
            "WorkflowCancelled",
            "WorkflowClosed",
        ):
            store.append(
                new_workflow_event(
                    event_type,
                    run_id,
                    producer="omo-blueprint-control",
                    payload={"rollback_digest": _sha256(patch)},
                    idempotency_key=f"{run_id}:{event_type}:{dispatch_id}",
                )
            )
        return {"state": "closed", "baseline_digest": restored_digest}


class _BlueprintArgumentParser(argparse.ArgumentParser):
    """Raise a controlled error so every facade failure remains JSON."""

    def error(self, message: str) -> NoReturn:
        raise BlueprintControlError(f"invalid blueprint command: {message}")


def _artifact_path(root: Path, reference: str, *, field_name: str, write: bool = False) -> Path:
    """Resolve an explicit repository-relative artifact without following it outside root."""
    relative = _safe_relative_path(reference, field_name)
    candidate = root / relative
    try:
        resolved = candidate.resolve(strict=not write)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise BlueprintControlError(f"unsafe {field_name}") from exc
    if not write and (not resolved.is_file() or resolved.is_symlink()):
        raise BlueprintControlError(f"{field_name} is unavailable")
    return resolved


def _read_json_artifact(root: Path, reference: str, *, field_name: str) -> dict[str, Any]:
    path = _artifact_path(root, reference, field_name=field_name)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BlueprintControlError(f"{field_name} is invalid") from exc
    if not isinstance(payload, dict):
        raise BlueprintControlError(f"{field_name} must contain a JSON object")
    return payload


def _compiled_from_artifact(root: Path, reference: str) -> CompiledBlueprintPacket:
    payload = _read_json_artifact(root, reference, field_name="packet file")
    packet = payload.get("packet")
    packet_hash = payload.get("packet_hash")
    if not isinstance(packet, dict) or not isinstance(packet_hash, str):
        raise BlueprintControlError("packet file has an invalid compiled packet")
    return CompiledBlueprintPacket(packet=packet, packet_hash=packet_hash)


def _candidate_projection_path(root: Path, dispatch_reference: str) -> Path:
    dispatch_path = _artifact_path(root, dispatch_reference, field_name="dispatch file")
    if not dispatch_path.name.endswith("-dispatch.yaml"):
        raise BlueprintControlError("dispatch file name is invalid")
    return dispatch_path.with_name(
        dispatch_path.name.removesuffix("-dispatch.yaml") + "-manifest.json"
    )


def _error_code(error: Exception) -> str:
    message = str(error).lower()
    if "approval" in message:
        return "controller_approval_required"
    if "human_required" in message or "human approval" in message:
        return "human_required"
    if "rollback" in message or "baseline" in message or "preimage" in message:
        return "rollback_unconfirmed"
    if "receipt" in message or "model output" in message or "transport" in message:
        return "input_only_ack_rejected"
    if "invalid blueprint command" in message:
        return "command_invalid"
    return "blueprint_command_failed"


def _emit(payload: Mapping[str, Any]) -> None:
    print(json.dumps(dict(payload), ensure_ascii=False, sort_keys=True))


def _parser() -> _BlueprintArgumentParser:
    parser = _BlueprintArgumentParser(prog="omo blueprint")
    commands = parser.add_subparsers(dest="command", required=True)

    compile_parser = commands.add_parser("compile")
    compile_parser.add_argument("--root", default=".")
    compile_parser.add_argument("--bet-id", required=True)
    compile_parser.add_argument("--task-id", required=True)
    compile_parser.add_argument("--spec-ref", required=True)
    compile_parser.add_argument("--spec-version", required=True)
    compile_parser.add_argument("--expires-at", required=True)
    compile_parser.add_argument("--packet-file", required=True)

    dispatch_parser = commands.add_parser("dispatch")
    dispatch_parser.add_argument("--root", default=".")
    dispatch_parser.add_argument("--packet-file", required=True)
    dispatch_parser.add_argument("--worker-id", required=True)
    dispatch_parser.add_argument("--capability-health-file", required=True)
    dispatch_parser.add_argument("--now")
    dispatch_parser.add_argument("--transport", default="cli_prompt")

    observe_parser = commands.add_parser("observe")
    observe_parser.add_argument("--root", default=".")
    observe_parser.add_argument("--dispatch-file", required=True)

    execute_parser = commands.add_parser("execute")
    execute_parser.add_argument("--root", default=".")
    execute_parser.add_argument("--packet-file", required=True)
    execute_parser.add_argument("--dispatch-file", required=True)
    execute_parser.add_argument("--candidate-file", required=True)
    execute_parser.add_argument("--approval-ref", required=True)
    execute_parser.add_argument("--supervised", action="store_true")
    execute_parser.add_argument("--timeout-seconds", type=int, default=900)

    collect_parser = commands.add_parser("collect")
    collect_parser.add_argument("--root", default=".")
    collect_parser.add_argument("--dispatch-file", required=True)
    collect_parser.add_argument("--candidate-file", required=True)

    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("--root", default=".")
    verify_parser.add_argument("--packet-file", required=True)
    verify_parser.add_argument("--dispatch-file", required=True)
    verify_parser.add_argument("--candidate-file", required=True)
    verify_parser.add_argument("--timeout-seconds", type=int, default=120)

    rollback_parser = commands.add_parser("rollback")
    rollback_parser.add_argument("--root", default=".")
    rollback_parser.add_argument("--dispatch-file", required=True)
    rollback_parser.add_argument("--candidate-file", required=True)
    return parser


def _root(value: str) -> Path:
    try:
        root = Path(value).resolve(strict=True)
    except OSError as exc:
        raise BlueprintControlError("authority root is unavailable") from exc
    if not root.is_dir():
        raise BlueprintControlError("authority root is not a directory")
    return root


def _dispatch_artifact(root: Path, reference: str) -> dict[str, Any]:
    path = _artifact_path(root, reference, field_name="dispatch file")
    try:
        payload = load_yaml(path)
    except (OSError, ValueError) as exc:
        raise BlueprintControlError("dispatch file is invalid") from exc
    blueprint = payload.get("blueprint")
    control_state = payload.get("control_state")
    workflow = payload.get("execution", {}).get("workflow_mesh")
    admission = workflow.get("admission") if isinstance(workflow, Mapping) else None
    if (
        not isinstance(blueprint, Mapping)
        or not isinstance(control_state, Mapping)
        or not isinstance(workflow, Mapping)
        or not isinstance(admission, Mapping)
        or control_state.get("transport") != "accepted"
    ):
        raise BlueprintControlError("dispatch is not transport accepted")
    required = {
        "workflow_run_id": workflow.get("workflow_run_id"),
        "admission_id": admission.get("admission_id"),
        "packet_id": blueprint.get("packet_id"),
        "packet_hash": blueprint.get("packet_hash"),
        "bet_id": blueprint.get("bet_id"),
        "dispatch_id": payload.get("dispatch_id"),
    }
    if any(not isinstance(value, str) or not value for value in required.values()):
        raise BlueprintControlError("dispatch identity is incomplete")
    return {
        "state": "transport_accepted",
        **required,
        "dispatch_path": _safe_relative_path(reference, "dispatch file"),
        "control_state": dict(control_state),
    }


def main(argv: list[str] | None = None) -> int:
    """Run the supervised facade without duplicating controller business logic."""
    try:
        parsed = _parser().parse_args(argv)
        root = _root(parsed.root)
        service = BlueprintControlService(root)

        if parsed.command == "compile":
            compiled = service.compile_packet(
                bet_id=parsed.bet_id,
                task_id=parsed.task_id,
                spec_ref=parsed.spec_ref,
                spec_version=parsed.spec_version,
                expires_at=parsed.expires_at,
            )
            packet_file = _artifact_path(
                root, parsed.packet_file, field_name="packet file", write=True
            )
            write_text_atomic(
                packet_file,
                json.dumps(
                    {"packet": compiled.packet, "packet_hash": compiled.packet_hash},
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
                + "\n",
            )
            _emit(
                {
                    "ok": True,
                    "state": "compiled",
                    "packet_id": compiled.packet["packet_id"],
                    "packet_hash": compiled.packet_hash,
                    "packet_file": _safe_relative_path(parsed.packet_file, "packet file"),
                }
            )
            return 0

        if parsed.command == "dispatch":
            compiled = _compiled_from_artifact(root, parsed.packet_file)
            health = _read_json_artifact(
                root, parsed.capability_health_file, field_name="capability health file"
            )
            result = service.dispatch_packet(
                compiled,
                worker_id=parsed.worker_id,
                capability_health=health,
                now=parsed.now,
                transport=parsed.transport,
            )
            _emit({"ok": True, **result})
            return 0

        dispatch = _dispatch_artifact(root, parsed.dispatch_file)
        if parsed.command == "observe":
            _emit({"ok": True, **service.observe_dispatch(dispatch)})
            return 0

        if parsed.command == "collect":
            projection = _read_json_artifact(
                root,
                str(_candidate_projection_path(root, parsed.dispatch_file).relative_to(root)),
                field_name="candidate projection",
            )
            if projection.get("state") != "candidate_collected":
                raise BlueprintControlError("candidate projection is not collected")
            candidate_file = _artifact_path(
                root, parsed.candidate_file, field_name="candidate file", write=True
            )
            write_text_atomic(
                candidate_file,
                json.dumps(projection, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            )
            _emit(
                {
                    "ok": True,
                    "state": "candidate_collected",
                    "candidate_file": _safe_relative_path(
                        parsed.candidate_file, "candidate file"
                    ),
                }
            )
            return 0

        if parsed.command == "execute":
            if not parsed.supervised:
                raise BlueprintControlError("supervised execution flag is required")
            compiled = _compiled_from_artifact(root, parsed.packet_file)
            approval_ref = str(compiled.packet.get("authority", {}).get("approval_ref") or "")
            if parsed.approval_ref != approval_ref:
                raise BlueprintControlError("controller approval reference mismatch")
            _artifact_path(root, parsed.approval_ref, field_name="approval reference")
            projection = service.execute_and_collect(
                compiled, dispatch, timeout_seconds=parsed.timeout_seconds
            )
            candidate_file = _artifact_path(
                root, parsed.candidate_file, field_name="candidate file", write=True
            )
            write_text_atomic(
                candidate_file,
                json.dumps(projection, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            )
            _emit(
                {
                    "ok": True,
                    "state": "candidate_collected",
                    "candidate_file": _safe_relative_path(
                        parsed.candidate_file, "candidate file"
                    ),
                }
            )
            return 0

        candidate = _read_json_artifact(
            root, parsed.candidate_file, field_name="candidate file"
        )
        if parsed.command == "verify":
            compiled = _compiled_from_artifact(root, parsed.packet_file)
            result = service.verify_candidate(
                compiled, dispatch, candidate, timeout_seconds=parsed.timeout_seconds
            )
            if result.get("state") != "independently_verified":
                _emit({"ok": False, "error": "verification_rejected", **result})
                return 4
            _emit({"ok": True, **result})
            return 0

        result = service.rollback_candidate(dispatch, candidate)
        if result.get("state") != "closed":
            _emit({"ok": False, "error": "rollback_unconfirmed", **result})
            return 4
        _emit({"ok": True, **result})
        return 0
    except (BlueprintControlError, OSError, ValueError, json.JSONDecodeError) as exc:
        _emit({"ok": False, "error": _error_code(exc)})
        return 2
    except Exception:
        _emit({"ok": False, "error": "blueprint_command_failed"})
        return 2
