"""A8 OMO external adapter transaction lifecycle.

7-stage: reserve -> bind -> readback -> start -> ACK/fence -> release/retire
"""
from __future__ import annotations
import hashlib, json, re
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol
from omo.event_ledger.broker import DuplicateEventError, LedgerBroker

PRODUCER = "omo-external-transaction"
SPACE_ID = "external-transaction"
EVT_RESERVED = "ExternalTransaction.Reserved.v1"
EVT_BOUND = "ExternalTransaction.Bound.v1"
EVT_READBACK_VERIFIED = "ExternalTransaction.ReadbackVerified.v1"
EVT_STARTED = "ExternalTransaction.Started.v1"
EVT_OBSERVED = "ExternalTransaction.Observed.v1"
EVT_FENCED = "ExternalTransaction.Fenced.v1"
EVT_VERIFIED = "ExternalTransaction.Verified.v1"
EVT_RELEASED = "ExternalTransaction.Released.v1"
EVT_RETIRED = "ExternalTransaction.Retired.v1"
EVT_OPERATOR_RESOLVED = "ExternalTransaction.OperatorResolved.v1"
TransactionState = Literal["reserved","bound","readback_verified","started","observed","fenced","verified","released","retired","operator_resolved"]
TERMINAL_STATES = frozenset({"retired","operator_resolved"})

class ExternalAdapter(Protocol):
    def invoke(self, request: dict[str, Any]) -> dict[str, Any]: ...

class TransactionError(Exception):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code; self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)
class TransactionStateError(TransactionError): ...
class TransactionValidationError(TransactionError): ...

@dataclass(frozen=True)
class TransactionRequest:
    adapter_id: str; role_id: str; capability: str; operation: str
    request_digest: str; principal_id: str; lease_deadline_seconds: int = 300
    capsule_id: str | None = None; verifier_role_id: str | None = None
    def validate(self) -> None:
        e: list[str] = []
        if not self.adapter_id: e.append("adapter_id required")
        if not re.match(r"^role:[A-Za-z0-9._-]{1,120}$", self.role_id or ""): e.append(f"role_id {self.role_id!r}")
        if not self.capability: e.append("capability required")
        if not self.operation: e.append("operation required")
        if not re.match(r"^[0-9a-f]{64}$", self.request_digest or ""): e.append("digest 64-hex")
        if not self.principal_id: e.append("principal_id required")
        if self.lease_deadline_seconds < 1: e.append("lease >= 1")
        if e: raise TransactionValidationError("invalid-request", "; ".join(e))

@dataclass(frozen=True)
class AdapterReceipt:
    transaction_id: str; operation: str; result_state: str
    result_digest: str | None = None; provenance_ref: str | None = None
    policy_digest: str | None = None; trace_id: str | None = None
    error_code: str | None = None; observed_at: str = ""
    def to_dict(self) -> dict[str, Any]:
        d = {"transaction_id": self.transaction_id, "operation": self.operation, "result_state": self.result_state}
        for f in ("result_digest","provenance_ref","policy_digest","trace_id","error_code","observed_at"):
            if getattr(self, f): d[f] = getattr(self, f)
        return d

@dataclass(frozen=True)
class ExternalTransaction:
    transaction_id: str; adapter_id: str; role_id: str; capability: str
    operation: str; request_digest: str; principal_id: str
    state: TransactionState; reserved_at: str; lease_deadline: str
    capsule_id: str | None = None; capsule_digest: str | None = None
    verifier_role_id: str | None = None; verifier_role_digest: str | None = None
    started_at: str | None = None; observed_at: str | None = None
    verified_at: str | None = None; released_at: str | None = None
    retired_at: str | None = None; fenced_at: str | None = None
    fence_reason: str | None = None; operator_resolved_at: str | None = None
    operator_resolution: str | None = None; receipt: dict[str, Any] | None = None
    trace_id: str | None = None; version: int = 1
    def to_dict(self) -> dict[str, Any]:
        d = {"transaction_id": self.transaction_id, "adapter_id": self.adapter_id, "role_id": self.role_id,
             "capability": self.capability, "operation": self.operation, "request_digest": self.request_digest,
             "principal_id": self.principal_id, "state": self.state, "reserved_at": self.reserved_at,
             "lease_deadline": self.lease_deadline, "version": self.version}
        for o in ("capsule_id","capsule_digest","verifier_role_id","verifier_role_digest","started_at",
                  "observed_at","verified_at","released_at","retired_at","fenced_at","fence_reason",
                  "operator_resolved_at","operator_resolution","trace_id"):
            v = getattr(self, o)
            if v is not None: d[o] = v
        if self.receipt is not None: d["receipt"] = self.receipt
        return d

def _now_dt() -> datetime: return datetime.now(UTC)
def _tid() -> str:
    import uuid; return f"ext-tx:{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:12]}"
def _suid() -> str:
    import uuid; return uuid.uuid4().hex[:12]

class ExternalTransactionService:
    def __init__(self, broker: LedgerBroker, *, role_registry: Any | None = None,
                 capsule_store: Any | None = None, clock: Any | None = None) -> None:
        self._broker = broker; self._role_registry = role_registry
        self._capsule_store = capsule_store
        self._clock = clock or (lambda: _now_dt().isoformat())
    @classmethod
    def open(cls, db_path: str, **kw: Any) -> ExternalTransactionService:
        return cls(LedgerBroker.connect(db_path), **kw)
    @property
    def broker(self) -> LedgerBroker: return self._broker
    def _fp(self, p: dict[str, Any]) -> ExternalTransaction:
        return ExternalTransaction(transaction_id=p["transaction_id"], adapter_id=p.get("adapter_id",""),
            role_id=p.get("role_id",""), capability=p.get("capability",""), operation=p.get("operation",""),
            request_digest=p.get("request_digest",""), principal_id=p.get("principal_id",""),
            state=p.get("state","reserved"), reserved_at=p.get("reserved_at",""),
            lease_deadline=p.get("lease_deadline",""), capsule_id=p.get("capsule_id"),
            capsule_digest=p.get("capsule_digest"), verifier_role_id=p.get("verifier_role_id"),
            verifier_role_digest=p.get("verifier_role_digest"), started_at=p.get("started_at"),
            observed_at=p.get("observed_at"), verified_at=p.get("verified_at"), released_at=p.get("released_at"),
            retired_at=p.get("retired_at"), fenced_at=p.get("fenced_at"), fence_reason=p.get("fence_reason"),
            operator_resolved_at=p.get("operator_resolved_at"), operator_resolution=p.get("operator_resolution"),
            receipt=p.get("receipt"), trace_id=p.get("trace_id"), version=p.get("version",1))
    def reserve(self, request: TransactionRequest) -> ExternalTransaction:
        request.validate()
        ck = f"ext-tx|{request.adapter_id}|{request.role_id}|{request.request_digest}"
        ex = self._fc(ck)
        if ex: return ex
        for t in self._fara(request.adapter_id, request.role_id):
            if t.request_digest != request.request_digest:
                raise TransactionError("conflicting-reservation", "different digest")
        now = _now_dt()
        dl = (now + timedelta(seconds=request.lease_deadline_seconds)).isoformat()
        t = ExternalTransaction(transaction_id=_tid(), adapter_id=request.adapter_id, role_id=request.role_id,
            capability=request.capability, operation=request.operation, request_digest=request.request_digest,
            principal_id=request.principal_id, state="reserved", reserved_at=now.isoformat(),
            lease_deadline=dl, capsule_id=request.capsule_id, verifier_role_id=request.verifier_role_id,
            trace_id=_suid())
        self._ae(EVT_RESERVED, t.transaction_id, ck, t.to_dict())
        return t
    def bind(self, tid: str, capsule_id: str, verifier_role_id: str | None = None) -> ExternalTransaction:
        t = self._l(tid); self._as(t, "reserved")
        c = None
        if self._capsule_store:
            c = self._capsule_store.get(capsule_id)
            if c is None: raise TransactionValidationError("capsule-not-found", f"capsule {capsule_id!r}")
        if self._role_registry and t.capability:
            v = self._role_registry.verify_role(t.role_id, t.capability)
            if not v.allowed: raise TransactionError("role-not-admitted", f"{t.role_id!r} not admitted for {t.capability!r}")
        vd = None
        if verifier_role_id and self._role_registry:
            r = self._role_registry.get(verifier_role_id)
            if r: vd = r.digest
        u = replace(t, state="bound", capsule_id=capsule_id, capsule_digest=c.digest if c else None,
            verifier_role_id=verifier_role_id or t.verifier_role_id, verifier_role_digest=vd, version=t.version+1)
        self._ae(EVT_BOUND, tid, f"ext-tx|{tid}", u.to_dict(), f"{tid}|bind")
        return u
    def readback_verify(self, tid: str) -> ExternalTransaction:
        t = self._l(tid); self._as(t, "bound")
        if t.capsule_id and self._capsule_store:
            c = self._capsule_store.get(t.capsule_id)
            if c is None: self._f(tid, "capsule-lost"); raise TransactionError("readback-failed", "capsule lost")
            if c.digest != t.capsule_digest: self._f(tid, "capsule-digest-mismatch"); raise TransactionError("readback-failed", "capsule digest")
        if self._role_registry:
            r = self._role_registry.get(t.role_id)
            if r is None: self._f(tid, "role-lost"); raise TransactionError("readback-failed", "role lost")
            if t.capability not in r.capabilities: self._f(tid, "cap-not-in-role"); raise TransactionError("readback-failed", "cap")
        if t.verifier_role_id and self._role_registry:
            v = self._role_registry.get(t.verifier_role_id)
            if v is None: self._f(tid, "verifier-lost"); raise TransactionError("readback-failed", "verifier lost")
            if v.digest != t.verifier_role_digest: self._f(tid, "verifier-digest"); raise TransactionError("readback-failed", "verifier digest")
        u = replace(t, state="readback_verified", version=t.version+1)
        self._ae(EVT_READBACK_VERIFIED, tid, f"ext-tx|{tid}", u.to_dict(), f"{tid}|readback")
        return u
    def start(self, tid: str, ar: dict[str, Any]) -> ExternalTransaction:
        t = self._l(tid); self._as(t, "readback_verified")
        u = replace(t, state="started", started_at=_now_dt().isoformat(), version=t.version+1)
        self._ae(EVT_STARTED, tid, f"ext-tx|{tid}", u.to_dict(), f"{tid}|start")
        return u
    def observe(self, tid: str, r: AdapterReceipt) -> ExternalTransaction:
        t = self._l(tid)
        if t.state != "started": raise TransactionStateError("invalid-state-for-observe", f"{t.state!r}")
        if r.transaction_id != tid: self._f(tid, "receipt-tid-mismatch"); raise TransactionError("identity-mismatch", "tid")
        if r.operation != t.operation: self._f(tid, "receipt-op-mismatch"); raise TransactionError("identity-mismatch", "op")
        if r.result_state in ("unknown","failed","error"):
            self._f(tid, f"adapter-{r.result_state}"); raise TransactionError("fenced", r.result_state)
        u = replace(t, state="observed", observed_at=r.observed_at or _now_dt().isoformat(),
            receipt=r.to_dict(), version=t.version+1)
        self._ae(EVT_OBSERVED, tid, f"ext-tx|{tid}", u.to_dict(), f"{tid}|observe")
        return u
    def verify(self, tid: str) -> ExternalTransaction:
        t = self._l(tid); self._as(t, "observed")
        if t.receipt is None: self._f(tid, "receipt-missing"); raise TransactionError("verification-failed", "receipt missing")
        if t.receipt.get("operation") != t.operation: self._f(tid, "verify-op-mismatch"); raise TransactionError("verification-failed", "op")
        if t.verifier_role_id and self._role_registry:
            v = self._role_registry.get(t.verifier_role_id)
            if v is None: self._f(tid, "verifier-lost-verify"); raise TransactionError("verification-failed", "verifier")
        u = replace(t, state="verified", verified_at=_now_dt().isoformat(), version=t.version+1)
        self._ae(EVT_VERIFIED, tid, f"ext-tx|{tid}", u.to_dict(), f"{tid}|verify")
        return u
    def release(self, tid: str) -> ExternalTransaction:
        t = self._l(tid); self._as(t, "verified")
        u = replace(t, state="released", released_at=_now_dt().isoformat(), version=t.version+1)
        self._ae(EVT_RELEASED, tid, f"ext-tx|{tid}", u.to_dict(), f"{tid}|release")
        return u
    def retire(self, tid: str) -> ExternalTransaction:
        t = self._l(tid); self._as(t, "released")
        u = replace(t, state="retired", retired_at=_now_dt().isoformat(), version=t.version+1)
        self._ae(EVT_RETIRED, tid, f"ext-tx|{tid}", u.to_dict(), f"{tid}|retire")
        return u
    def _f(self, tid: str, reason: str) -> None:
        t = self._l(tid)
        if t.state in TERMINAL_STATES: return
        u = replace(t, state="fenced", fenced_at=_now_dt().isoformat(), fence_reason=reason, version=t.version+1)
        self._ae(EVT_FENCED, tid, f"ext-tx|{tid}", u.to_dict(), f"{tid}|fence")
    def operator_resolve(self, tid: str, res: str) -> ExternalTransaction:
        t = self._l(tid); self._as(t, "fenced")
        u = replace(t, state="operator_resolved", operator_resolved_at=_now_dt().isoformat(),
            operator_resolution=res, version=t.version+1)
        self._ae(EVT_OPERATOR_RESOLVED, tid, f"ext-tx|{tid}", u.to_dict(), f"{tid}|operator-resolve")
        return u
    def get(self, tid: str) -> ExternalTransaction | None:
        try: return self._l(tid)
        except TransactionError: return None
    def list(self, *, state: TransactionState | None = None) -> list[ExternalTransaction]:
        txns = list(self._ra().values())
        return [t for t in txns if state is None or t.state == state]
    def _l(self, tid: str) -> ExternalTransaction:
        ev = self._re(tid)
        if not ev: raise TransactionError("not-found", f"transaction {tid!r}")
        return ev[-1]
    def _ra(self) -> dict[str, ExternalTransaction]:
        s: dict[str, ExternalTransaction] = {}
        for row in self._broker.read(producer=PRODUCER):
            p = json.loads(row["payload_json"])
            if p.get("transaction_id"): s[p["transaction_id"]] = self._fp(p)
        return s
    def _re(self, tid: str) -> list[ExternalTransaction]:
        o: list[ExternalTransaction] = []
        for row in self._broker.read(producer=PRODUCER):
            p = json.loads(row["payload_json"])
            if p.get("transaction_id") == tid: o.append(self._fp(p))
        return o
    def _fc(self, ck: str) -> ExternalTransaction | None:
        for row in self._broker.read(producer=PRODUCER):
            if row.get("correlation_id") == ck:
                p = json.loads(row["payload_json"])
                return self._l(p["transaction_id"])
        return None
    def _fara(self, aid: str, rid: str) -> list[ExternalTransaction]:
        o: list[ExternalTransaction] = []
        for row in self._broker.read(producer=PRODUCER):
            p = json.loads(row["payload_json"])
            if p.get("adapter_id")==aid and p.get("role_id")==rid and p.get("state")=="reserved":
                o.append(self._fp(p))
        return o
    def _as(self, t: ExternalTransaction, exp: str) -> None:
        if t.state != exp: raise TransactionStateError("invalid-state", f"{t.transaction_id!r} {t.state!r}!={exp!r}")
    def _ae(self, et: str, tid: str, ci: str, p: dict[str, Any], ik: str | None = None) -> int:
        try: return self._broker.append(event_type=et, producer=PRODUCER, principal_id=p.get("principal_id",""),
            space_id=SPACE_ID, correlation_id=ci, idempotency_key=ik or f"{tid}|{et}", payload=p)
        except DuplicateEventError: return 0
    def execute_happy_path(self, req: TransactionRequest, ad: ExternalAdapter,
                           cid: str, vid: str | None = None) -> ExternalTransaction:
        t = self.reserve(req); t = self.bind(t.transaction_id, cid, vid)
        t = self.readback_verify(t.transaction_id); t = self.start(t.transaction_id, {})
        r = ad.invoke({"operation": t.operation, "transaction_id": t.transaction_id})
        rc = AdapterReceipt(transaction_id=t.transaction_id, operation=t.operation,
            result_state="succeeded" if r.get("state")=="succeeded" else "degraded",
            result_digest=r.get("result_digest"), provenance_ref=r.get("provenance_ref"),
            policy_digest=r.get("policy_digest"), trace_id=t.trace_id, error_code=r.get("error_code"))
        t = self.observe(t.transaction_id, rc); t = self.verify(t.transaction_id)
        t = self.release(t.transaction_id); t = self.retire(t.transaction_id)
        return t

__all__ = ("AdapterReceipt","ExternalAdapter","ExternalTransaction","ExternalTransactionService",
    "TransactionError","TransactionRequest","TransactionStateError","TransactionState","TransactionValidationError")
