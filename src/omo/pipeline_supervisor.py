"""pipeline_supervisor — 数字大脑全管线常驻编排 (BET-Y1Q4-T2-05).

Resident execute 角色: 定时通道 (07:30 晨报全链) + 文件监听通道 (扫描件→OCR→
待办卡片)。每站 subprocess 调归属 CLI, 单站失败降级续跑, 状态/告警落
.omo/state/pipeline-supervisor-state.json (convergence-pulse health 第四源)。

CLI:
  python -m omo.pipeline_supervisor --tick-morning   # 定时全链 (launchd 07:30)
  python -m omo.pipeline_supervisor --watch          # 文件监听 (OCR 入站)
  python -m omo.pipeline_supervisor --status         # 状态面板
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = "omo.pipeline-supervisor.v1"
EMBED_NODE_URL = "http://100.64.110.118:18700/embed"  # Mac mini mesh 节点 (T3-02 P3)
INBOUND_DIR = Path.home() / "Inbox" / "ocr-inbound"

PIPELINE_VECTORS = "pipeline-vectors.jsonl"
EVENT_LOG = "pipeline-events.jsonl"


def _ws() -> Path:
    cur = Path(__file__).resolve()
    for parent in cur.parents:
        if (parent / "docs" / "project-registry.yaml").is_file():
            return parent
    return Path.cwd()


def _state_path() -> Path:
    return _ws() / ".omo" / "state" / "pipeline-supervisor-state.json"


def _run(cmd: list[str], timeout: int = 300) -> tuple[bool, str]:
    import subprocess

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
        detail = (proc.stdout[-300:] if proc.returncode == 0 else (proc.stderr or proc.stdout)[-300:]).strip()
        return proc.returncode == 0, detail
    except (subprocess.TimeoutExpired, OSError) as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _station(name: str, states: dict[str, dict], fn) -> Any:
    """Run one station with degrade-on-fail semantics (BET circuit_breaker)."""
    t0 = time.monotonic()
    try:
        result = fn()
        states[name] = {"status": "ok", "ms": round((time.monotonic() - t0) * 1000)}
        return result
    except Exception as exc:
        states[name] = {"status": "degraded", "reason": f"{type(exc).__name__}: {exc}"[:200]}
        return None


def _load_state() -> dict[str, Any]:
    path = _state_path()
    if path.is_file():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            pass
    return {"schema": SCHEMA, "morning_runs": [], "inbound_runs": [], "alerts": []}


def _save_state(state: dict[str, Any]) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")


def _append_jsonl(name: str, record: dict[str, Any]) -> None:
    p = _ws() / ".omo" / "state" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _emit_event(topic: str, payload: dict[str, Any]) -> None:
    """Bus station: append structured event (T2-01 event-stream ledger)."""
    _append_jsonl(EVENT_LOG, {"topic": topic, "ts": datetime.now(UTC).isoformat(), **payload})


def _embed_via_node(texts: list[str]) -> list[list[float]] | None:
    """Embed station: Mac mini node first, local omlxc fallback (degradation)."""
    import urllib.request

    req = urllib.request.Request(
        EMBED_NODE_URL,
        data=json.dumps({"texts": texts}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8")).get("vectors")
    except Exception:
        return None  # caller falls back to local


# ── Morning chain (定时通道) ───────────────────────────────────────────


def tick_morning() -> dict[str, Any]:
    """Full chain: radar → bus → embed → render. Degrade per station."""
    ws = _ws()
    states: dict[str, dict] = {}
    now = datetime.now(UTC)
    state = _load_state()

    # 1. radar — morning brief (network degrade → cached snapshot inside radar)
    def radar():
        ok, detail = _run(["python3", str(ws / "bin/bc-os/policy_radar.py"), "--generate-morning-brief"], timeout=600)
        if not ok:
            raise RuntimeError(detail)
        return detail

    radar_out = _station("radar", states, radar)

    day = now.strftime("%Y%m%d")
    brief_path = ws / ".omo" / "state" / "policy-radar" / f"brief-{day}.json"

    # 2. bus — publish brief items as high-priority events (stream ledger)
    def bus():
        if not brief_path.is_file():
            raise RuntimeError(f"brief missing: {brief_path.name}")
        items = json.loads(brief_path.read_text(encoding="utf-8")).get("items", [])
        for item in items[:15]:
            _emit_event(
                "mesh:radar:brief",
                {"priority": "high", "title": item.get("title", "")[:80], "score": item.get("score")},
            )
        return len(items)

    bus_count = _station("bus", states, bus)

    # 3. embed — brief titles into vector cache (remote node → local fallback)
    def embed():
        if not brief_path.is_file():
            raise RuntimeError("brief missing for embed")
        items = json.loads(brief_path.read_text(encoding="utf-8")).get("items", [])
        texts = [f"{i.get('title', '')} {i.get('source', '')}" for i in items]
        if not texts:
            return 0
        vecs = _embed_via_node(texts)
        backend = "mac-mini-node"
        if vecs is None:  # degradation: local omlxc engine
            ok, detail = _run(
                [
                    "uv",
                    "run",
                    "--directory",
                    str(ws / "projects/omlxc"),
                    "python",
                    "-c",
                    "import json,sys; from omlxc.dataplane.embedding_mps import EmbeddingEngine;"
                    "e=EmbeddingEngine(); e.encode(['warmup']);"
                    "print(json.dumps(e.encode(sys.argv[1:])))",
                ]
                + texts,
                timeout=300,
            )
            if not ok:
                raise RuntimeError(detail)
            vecs = json.loads(detail)
            backend = "local-omlxc"
        for item, vec in zip(items, vecs):
            _append_jsonl(
                PIPELINE_VECTORS, {"day": day, "title": item.get("title", "")[:80], "backend": backend, "dim": len(vec)}
            )
        return backend

    embed_backend = _station("embed", states, embed)

    # 4. render — GB/T DOCX edition (failure keeps brief.md, never blocks)
    def render():
        md_path = ws / ".omo" / "state" / "policy-radar" / f"brief-{day}.md"
        if not md_path.is_file():
            return "no-md"
        ok, detail = _run(
            [
                "uv",
                "run",
                "--directory",
                str(ws / "projects/cockpit"),
                "python",
                "-m",
                "cockpit.cli",
                "render",
                "docx",
                "--input",
                str(md_path),
                "--template",
                "standard-gov",
            ],
            timeout=300,
        )
        if not ok:
            raise RuntimeError(detail)
        return "docx-rendered"

    render_out = _station("render", states, render)

    # state + alerts (convergence-pulse health source)
    run_record = {"ts": now.isoformat(), "stations": states, "brief": str(brief_path), "items": bus_count}
    state["morning_runs"].append(run_record)
    state["morning_runs"] = state["morning_runs"][-30:]  # rolling window
    degraded = [n for n, s in states.items() if s["status"] != "ok"]
    if degraded:
        state["alerts"].append({"ts": now.isoformat(), "kind": "morning_degraded", "stations": degraded})
        state["alerts"] = state["alerts"][-50:]
    _save_state(state)
    return {
        "run": run_record,
        "degraded": degraded,
        "radar": radar_out,
        "embed_backend": embed_backend,
        "render": render_out,
    }


# ── Inbound watch (文件监听通道) ───────────────────────────────────────


def process_inbound(image: Path) -> dict[str, Any]:
    """One scanned doc: OCR → embed → IM-triage card. Returns run record."""
    ws = _ws()
    states: dict[str, dict] = {}
    now = datetime.now(UTC)

    def ocr():
        import subprocess

        proc = subprocess.run(
            [
                "uv",
                "run",
                "--directory",
                str(ws / "projects/agora"),
                "python",
                "-m",
                "agora.server.tools_bos.ocr",
                "extract",
                "--file",
                str(image),
            ],
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or proc.stdout)[-300:])
        out = proc.stdout
        idx = out.find("{")  # strip any uv build-log preamble
        if idx < 0:
            raise RuntimeError("no JSON in OCR output")
        return json.loads(out[idx:])

    ocr_out = _station("ocr", states, ocr)

    md_text = ""
    if isinstance(ocr_out, dict):
        md_text = ocr_out.get("markdown", "")[:2000]

    def embed():
        if not md_text:
            raise RuntimeError("no markdown from OCR")
        vecs = _embed_via_node([md_text[:512]])
        if vecs is None:
            raise RuntimeError("embed node unreachable and local fallback not attempted (short text)")
        return len(vecs[0])

    embed_dim = _station("embed", states, embed)

    def card():
        if not md_text:
            raise RuntimeError("no content for card")
        card_dir = ws / ".omo" / "state" / "im-triage"
        card_dir.mkdir(parents=True, exist_ok=True)
        card_file = card_dir / f"pipeline-{image.stem}.json"
        card_file.write_text(
            json.dumps(
                {
                    "cards": [
                        {
                            "message_id": image.stem,
                            "platform": "ocr-inbound",
                            "chat_id": "inbox:ocr",
                            "sender": "扫描件",
                            "action": "query",
                            "payload": md_text[:120],
                            "priority": "high",
                            "deadline_hint_days": None,
                            "status": "pending_approval",
                        }
                    ],
                },
                ensure_ascii=False,
                indent=1,
            ),
            encoding="utf-8",
        )
        return str(card_file.relative_to(ws))

    card_path = _station("card", states, card)

    state = _load_state()
    record = {"ts": now.isoformat(), "image": image.name, "stations": states, "card": card_path}
    state["inbound_runs"].append(record)
    state["inbound_runs"] = state["inbound_runs"][-50:]
    degraded = [n for n, s in states.items() if s["status"] != "ok"]
    if degraded:
        state["alerts"].append(
            {"ts": now.isoformat(), "kind": "inbound_degraded", "stations": degraded, "image": image.name}
        )
        state["alerts"] = state["alerts"][-50:]
    _save_state(state)
    return {"run": record, "degraded": degraded, "embed_dim": embed_dim}


def watch_inbound(poll_s: float = 10.0) -> None:
    """Watch ~/Inbox/ocr-inbound/ for new scans (one-shot per new file)."""
    INBOUND_DIR.mkdir(parents=True, exist_ok=True)
    seen_file = INBOUND_DIR / ".processed.json"
    seen: list[str] = []
    if seen_file.is_file():
        try:
            seen = json.loads(seen_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            seen = []
    print(f"watching {INBOUND_DIR} (poll {poll_s}s, {len(seen)} processed)")
    while True:
        for img in sorted(INBOUND_DIR.glob("*")):
            if img.suffix.lower() not in {".png", ".jpg", ".jpeg", ".heic", ".tif", ".tiff", ".pdf"}:
                continue
            if img.name in seen:
                continue
            print(f"[inbound] processing {img.name} ...")
            result = process_inbound(img)
            print(f"[inbound] {img.name}: degraded={result['degraded'] or 'none'}")
            seen.append(img.name)
            seen_file.write_text(json.dumps(seen[-200:], ensure_ascii=False), encoding="utf-8")
        time.sleep(poll_s)


def status() -> dict[str, Any]:
    state = _load_state()
    morning = state.get("morning_runs", [])
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    return {
        "schema": SCHEMA,
        "today_morning_brief": any(r.get("ts", "").startswith(today) for r in morning),
        "morning_runs_total": len(morning),
        "last_morning": morning[-1] if morning else None,
        "inbound_runs_total": len(state.get("inbound_runs", [])),
        "alerts": state.get("alerts", [])[-5:],
        "watch_dir": str(INBOUND_DIR),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--tick-morning", action="store_true", help="定时通道: 晨报全链")
    group.add_argument("--watch", action="store_true", help="文件监听: OCR 入站")
    group.add_argument("--process-file", metavar="PATH", help="单文件处理 (测试/手动)")
    group.add_argument("--status", action="store_true", help="状态面板")
    args = parser.parse_args(argv)

    if args.tick_morning:
        result = tick_morning()
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0
    if args.watch:
        watch_inbound()
        return 0
    if args.process_file:
        result = process_inbound(Path(args.process_file).expanduser())
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0
    print(json.dumps(status(), ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
