from typing import Any

from pydantic import BaseModel, Field


class TaskSchema(BaseModel):
    task_id: str
    title: str
    description: str
    status: str = "planned"
    priority: str = "P1"
    assignee: str = "unassigned"
    tags: list[str] = Field(default_factory=list)
    created_at: str
    updated_at: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class BetSchema(BaseModel):
    goal_id: str
    title: str
    description: str
    status: str = "active"
    vector: str = "V2"  # V1 or V2
    appetite: str = "1 week"
    created_at: str


class PitchSchema(BaseModel):
    pitch_id: str
    title: str
    content: str
    upstream_ref: str | None = None
    appetite: str | None = None
    created_at: str
