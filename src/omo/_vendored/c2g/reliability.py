"""C2G 可靠性加固模块"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ValidationResult:
    valid: bool
    errors: list[str]
    warnings: list[str]
    suggestions: list[str]


class C2GSafetyGuard:
    def validate_pitch_format_loosely(self, content):
        errors, warnings, suggestions = [], [], []
        if not content.strip():
            return ValidationResult(False, ["内容为空"], [], ["请填写 Pitch"])
        has_title = any(line.strip().startswith("# ") for line in content.split("\n"))
        if not has_title:
            warnings.append("建议添加一级标题")
        return ValidationResult(
            len(content.strip()) > 10, errors, warnings, suggestions
        )


class FallbackTaskGenerator:
    def generate_fallback_task(self, pitch_content, pitch_path):
        title = "战略探索"
        for line in pitch_content.split("\n"):
            if line.strip().startswith("# "):
                title = line.strip()[2:].strip()
                break
        return {
            "task_id": f"fb-{hash(pitch_path) % 10000:04d}",
            "title": f"探索: {title}",
            "status": "planned",
        }
