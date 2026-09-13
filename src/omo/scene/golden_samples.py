"""scene/golden_samples.py — 场景金牌样例库 (BET-Y1Q4-T7-05).

成功履约的旅程沉淀为结构化金牌样例, 供同类任务复用与先例检索。
零模型调用, 纯确定性逻辑。

设计决策:
- 样例 digest = sha256(规范 JSON), 去重键 = (scene_id, input_digest)。
- Store 为内存索引 + JSONL 落盘, 无外部存储依赖。
- find_similar 按 calibration 降序, 诚实返回空列表而非编造。
- 守 T7-05 circuit breaker: 本模块只做记录与检索, 不执行系统动作。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any


class GoldenSampleError(Exception):
    """金牌样例操作失败 (附机器可读 code)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _utcnow() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class GoldenSample:
    """一条金牌样例: 某场景一次成功履约的可复用记录."""

    scene_id: str
    input_digest: str
    result_summary: str
    calibration: float = 1.0
    created_at: str = field(default_factory=_utcnow)

    def digest(self) -> str:
        return hashlib.sha256(_canonical_json(asdict(self)).encode("utf-8")).hexdigest()


class GoldenSampleStore:
    """按 scene_id 索引的金牌样例库, 支持 JSONL 持久化."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path
        self._samples: dict[str, dict[str, GoldenSample]] = {}
        if path is not None and path.is_file():
            self.load(path)

    def record(
        self,
        scene_id: str,
        inputs: dict[str, Any],
        result_summary: str,
        calibration: float = 1.0,
    ) -> GoldenSample:
        """记录一条金牌样例; 同 (scene_id, input_digest) 覆盖更新 (去重)."""
        if not scene_id:
            raise GoldenSampleError("empty-scene-id", "scene_id 不能为空")
        if not (0.0 <= calibration <= 1.0):
            raise GoldenSampleError("bad-calibration", f"calibration 必须在 [0,1]: {calibration}")
        input_digest = hashlib.sha256(_canonical_json(inputs).encode("utf-8")).hexdigest()
        sample = GoldenSample(
            scene_id=scene_id,
            input_digest=input_digest,
            result_summary=result_summary,
            calibration=calibration,
        )
        self._samples.setdefault(scene_id, {})[input_digest] = sample
        return sample

    def find_similar(self, scene_id: str, top_k: int = 3) -> list[GoldenSample]:
        """按 calibration 降序返回该场景金牌样例; 无样例返回空列表."""
        bucket = self._samples.get(scene_id, {})
        ranked = sorted(bucket.values(), key=lambda s: s.calibration, reverse=True)
        return ranked[: max(top_k, 0)]

    def count(self, scene_id: str) -> int:
        return len(self._samples.get(scene_id, {}))

    def save(self, path: Path | None = None) -> Path:
        target = path or self._path
        if target is None:
            raise GoldenSampleError("no-path", "未指定持久化路径")
        target.parent.mkdir(parents=True, exist_ok=True)
        lines = [_canonical_json(asdict(s)) for bucket in self._samples.values() for s in bucket.values()]
        target.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        self._path = target
        return target

    def load(self, path: Path) -> int:
        if not path.is_file():
            raise GoldenSampleError("missing-file", f"样例文件不存在: {path}")
        n = 0
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            sample = GoldenSample(**{k: obj[k] for k in GoldenSample.__dataclass_fields__ if k in obj})
            self._samples.setdefault(sample.scene_id, {})[sample.input_digest] = sample
            n += 1
        self._path = path
        return n
