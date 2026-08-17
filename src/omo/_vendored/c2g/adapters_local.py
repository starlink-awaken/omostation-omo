import json
from pathlib import Path

from .domain import BetSchema, PitchSchema, TaskSchema
from .ports import IGovernanceProvider, IStorageProvider


class LocalGovernanceProvider(IGovernanceProvider):
    def validate_pitch(self, pitch: PitchSchema) -> bool:
        if not pitch.title or len(pitch.title) < 3:
            print("❌ Pitch 标题过短。")
            return False
        return True

    def validate_task(self, task: TaskSchema) -> bool:
        return bool(task.title)

    def get_current_phase(self) -> str:
        return "Local Default Phase"


class LocalStorageProvider(IStorageProvider):
    def __init__(self, base_dir: str = ".c2g_data"):
        self.base_dir = Path(base_dir)
        self.pitches_dir = self.base_dir / "pitches"
        self.bets_file = self.base_dir / "bets.json"
        self.tasks_file = self.base_dir / "tasks.json"
        self._init_fs()

    def _init_fs(self):
        self.pitches_dir.mkdir(parents=True, exist_ok=True)
        if not self.bets_file.exists():
            self.bets_file.write_text("[]", encoding="utf-8")
        if not self.tasks_file.exists():
            self.tasks_file.write_text("[]", encoding="utf-8")

    def save_bet(self, bet: BetSchema) -> str:
        # Upsert by goal_id: 重复 goal_id 覆盖而非盲目 append。
        # 修复运行时 bets.json 重复累积 (曾出现同一 goal_id 的 test-draft 双写)。
        data = json.loads(self.bets_file.read_text(encoding="utf-8"))
        record = bet.model_dump()
        for i, existing in enumerate(data):
            if existing.get("goal_id") == bet.goal_id:
                data[i] = record
                break
        else:
            data.append(record)
        self.bets_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        return bet.goal_id

    def save_task(self, task: TaskSchema) -> str:
        # Upsert by task_id (同 save_bet 语义)。
        data = json.loads(self.tasks_file.read_text(encoding="utf-8"))
        record = task.model_dump()
        for i, existing in enumerate(data):
            if existing.get("task_id") == task.task_id:
                data[i] = record
                break
        else:
            data.append(record)
        self.tasks_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        return task.task_id

    def update_bet_status(self, goal_id: str, status: str) -> bool:
        """回写 bet 状态 (active→done/deferred/...)。返回是否命中修改。"""
        data = json.loads(self.bets_file.read_text(encoding="utf-8"))
        changed = False
        for rec in data:
            if rec.get("goal_id") == goal_id:
                rec["status"] = status
                changed = True
        if changed:
            self.bets_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        return changed

    def get_all_bets(self) -> list[BetSchema]:
        data = json.loads(self.bets_file.read_text(encoding="utf-8"))
        return [BetSchema(**d) for d in data]

    def delete_bet(self, goal_id: str) -> int:
        """按 goal_id 删除 bet (清理垃圾/测试污染记录)。返回删除条数。"""
        data = json.loads(self.bets_file.read_text(encoding="utf-8"))
        kept = [r for r in data if r.get("goal_id") != goal_id]
        removed = len(data) - len(kept)
        if removed:
            self.bets_file.write_text(json.dumps(kept, indent=2, ensure_ascii=False), encoding="utf-8")
        return removed

    def get_pitches(self) -> list[PitchSchema]:
        pitches = []
        for pf in self.pitches_dir.glob("*.md"):
            content = pf.read_text(encoding="utf-8")
            pitches.append(PitchSchema(pitch_id=pf.name, title=pf.stem, content=content, created_at=""))
        return pitches

    def delete_pitch(self, pitch_id: str) -> bool:
        pf = self.pitches_dir / pitch_id
        if pf.exists():
            pf.unlink()
            return True
        return False

    def get_active_bets(self) -> list[BetSchema]:
        data = json.loads(self.bets_file.read_text(encoding="utf-8"))
        return [BetSchema(**d) for d in data if d.get("status") == "active"]


def _dedup_by_key(records: list[dict], key: str) -> tuple[list[dict], int]:
    """Dedup records by `key`, keeping first occurrence. Returns (kept, dropped)."""
    seen: dict = {}
    dropped = 0
    for r in records:
        k = r.get(key)
        if k in seen:
            dropped += 1
            continue
        seen[k] = r
    return list(seen.values()), dropped


def consolidate_data(dest_dir, source_files) -> dict:
    """把散落各处的 bets.json / tasks.json 合并进 dest_dir 的权威副本, 按 id 去重.

    收口三处分叉 (workspace 根 / c2g/.c2g_data / c2g/bets.json) 的根因:
    c2g 因 cwd 不同锚定到不同 .c2g_data。本函数把已知散落源 upsert 进单一权威目录,
    去重保留首见 (dest 优先), 天然丢弃重复 goal_id 的垃圾记录。

    Args:
        dest_dir: 权威数据目录 (通常 <workspace_root>/.c2g_data, 或 C2G_DATA_DIR)。
        source_files: 待并入的源, 可为目录 (取其 bets.json/tasks.json) 或文件路径。
    Returns: {kind: {total, dropped_duplicates, sources}} 报告。
    """
    dest = Path(dest_dir)
    LocalStorageProvider(str(dest))  # 确保 dest 结构 + 空文件就位
    report: dict = {}
    for fname, key in (("bets.json", "goal_id"), ("tasks.json", "task_id")):
        target = dest / fname
        records = json.loads(target.read_text(encoding="utf-8"))
        used_sources = []
        for sf in source_files:
            sf = Path(sf)
            cand = sf if sf.name == fname else (sf / fname)
            if not cand.exists() or cand.resolve() == target.resolve():
                continue
            records.extend(json.loads(cand.read_text(encoding="utf-8")))
            used_sources.append(str(cand))
        merged, dropped = _dedup_by_key(records, key)
        target.write_text(json.dumps(merged, indent=2, ensure_ascii=False), encoding="utf-8")
        report[fname] = {
            "total": len(merged),
            "dropped_duplicates": dropped,
            "sources": used_sources,
        }
    return report
