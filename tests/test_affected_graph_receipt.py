from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from omo.workflow.affected_graph_receipt import validate_affected_graph_receipt
from omo.workflow.core import WorkflowError


def _write_contract(workspace: Path, projects: dict[str, list[str]]) -> Path:
    contract = workspace / "docs" / "layer-contract.yaml"
    contract.parent.mkdir(parents=True)
    layers = "\n".join(f"  {layer}:\n    projects: {json.dumps(names)}" for layer, names in projects.items())
    contract.write_text(
        f"layers:\n{layers}\ndependency_rules:\n  allowed_directions:\n    - from: [L3]\n      to: [L2]\n",
        encoding="utf-8",
    )
    return contract


def _receipt(workspace: Path, changed: list[str], affected: list[str]) -> Path:
    contract = workspace / "docs" / "layer-contract.yaml"
    payload = {
        "schema": "affected-graph-receipt/v1",
        "changed_projects": changed,
        "affected_projects": affected,
        "layer_contract_digest": hashlib.sha256(contract.read_bytes()).hexdigest(),
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["receipt_hash"] = hashlib.sha256(canonical.encode()).hexdigest()
    path = workspace / "receipt.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_valid_receipt_binds_projects_omo_claim(tmp_path: Path) -> None:
    _write_contract(tmp_path, {"L2": ["omo"], "L3": ["cockpit"]})
    receipt = _receipt(tmp_path, ["omo"], ["cockpit", "omo"])

    result = validate_affected_graph_receipt(receipt.name, ["projects/omo/src/omo/workflow/cli.py"], tmp_path)

    assert result["receipt_hash"]
    assert result["affected_projects"] == ["cockpit", "omo"]
    assert result["receipt_ref"] == "receipt.json"
    assert "receipt_path" not in result


@pytest.mark.parametrize("reference", ["dummy", "f" * 64, "missing.json"])
def test_dummy_or_nonexistent_reference_fails_closed(tmp_path: Path, reference: str) -> None:
    _write_contract(tmp_path, {"L2": ["omo"]})

    with pytest.raises(WorkflowError, match="receipt file does not exist"):
        validate_affected_graph_receipt(reference, ["projects/omo"], tmp_path)


def test_tampered_receipt_fails_closed(tmp_path: Path) -> None:
    _write_contract(tmp_path, {"L2": ["omo"], "L3": ["cockpit"]})
    receipt = _receipt(tmp_path, ["omo"], ["cockpit", "omo"])
    payload = json.loads(receipt.read_text())
    payload["affected_projects"] = ["omo"]
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(WorkflowError, match="receipt_hash mismatch"):
        validate_affected_graph_receipt(receipt.name, ["projects/omo"], tmp_path)


def test_layer_contract_drift_fails_closed(tmp_path: Path) -> None:
    contract = _write_contract(tmp_path, {"L2": ["omo"]})
    receipt = _receipt(tmp_path, ["omo"], ["omo"])
    contract.write_text(contract.read_text() + "\n# drift\n", encoding="utf-8")

    with pytest.raises(WorkflowError, match="layer contract digest mismatch"):
        validate_affected_graph_receipt(receipt.name, ["projects/omo"], tmp_path)


def test_missing_claimed_project_fails_closed(tmp_path: Path) -> None:
    _write_contract(tmp_path, {"L2": ["gbrain", "omo"], "L3": ["cockpit"]})
    receipt = _receipt(tmp_path, ["omo"], ["cockpit", "omo"])

    with pytest.raises(WorkflowError, match="claimed projects missing"):
        validate_affected_graph_receipt(receipt.name, ["projects/gbrain/src/gbrain/api.py"], tmp_path)


def test_root_path_requires_explicit_workspace_root_project(tmp_path: Path) -> None:
    _write_contract(tmp_path, {"L2": ["omo"]})
    receipt = _receipt(tmp_path, ["omo"], ["omo"])

    with pytest.raises(WorkflowError, match="workspace-root"):
        validate_affected_graph_receipt(receipt.name, ["docs/README.md"], tmp_path)


def test_ambiguous_projects_prefix_is_not_a_workspace_root_claim(
    tmp_path: Path,
) -> None:
    _write_contract(tmp_path, {"L2": ["omo"]})
    receipt = _receipt(tmp_path, ["workspace-root"], ["workspace-root"])

    with pytest.raises(WorkflowError, match="ambiguous projects path"):
        validate_affected_graph_receipt(receipt.name, ["projects"], tmp_path)


@pytest.mark.parametrize("reference", ["../receipt.json", "nested//receipt.json"])
def test_noncanonical_receipt_reference_fails_closed(tmp_path: Path, reference: str) -> None:
    _write_contract(tmp_path, {"L2": ["omo"]})

    with pytest.raises(WorkflowError, match="canonical workspace-relative"):
        validate_affected_graph_receipt(reference, ["projects/omo"], tmp_path)


def test_absolute_receipt_reference_fails_closed(tmp_path: Path) -> None:
    _write_contract(tmp_path, {"L2": ["omo"]})
    receipt = _receipt(tmp_path, ["omo"], ["omo"])

    with pytest.raises(WorkflowError, match="workspace-relative"):
        validate_affected_graph_receipt(receipt, ["projects/omo"], tmp_path)


def test_symlink_receipt_reference_fails_closed(tmp_path: Path) -> None:
    _write_contract(tmp_path, {"L2": ["omo"]})
    receipt = _receipt(tmp_path, ["omo"], ["omo"])
    symlink = tmp_path / "receipt-link.json"
    symlink.symlink_to(receipt)

    with pytest.raises(WorkflowError, match="must not traverse symlinks"):
        validate_affected_graph_receipt(symlink.name, ["projects/omo"], tmp_path)


def test_surface_only_claim_requires_workspace_root_coverage(tmp_path: Path) -> None:
    _write_contract(tmp_path, {"L2": ["omo"]})
    receipt = _receipt(tmp_path, ["omo"], ["omo"])

    with pytest.raises(WorkflowError, match="workspace-root"):
        validate_affected_graph_receipt(receipt.name, [], tmp_path, claimed_surfaces=["doc-ssot"])


def test_surface_only_claim_accepts_workspace_root_receipt(tmp_path: Path) -> None:
    _write_contract(tmp_path, {"L2": ["omo"]})
    receipt = _receipt(tmp_path, ["workspace-root"], ["workspace-root"])

    result = validate_affected_graph_receipt(receipt.name, [], tmp_path, claimed_surfaces=["doc-ssot"])

    assert result["affected_projects"] == ["workspace-root"]
