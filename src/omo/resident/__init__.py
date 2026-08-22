"""omo.resident — resident agent runtime (multi-常驻 agent 动态成长体系).

Migrated from bin/ssot resident scripts (WP-I). Hosts event ingest, the
subscription→execute daemon, knowledge sediment, memory sync, personal-signals,
alert forwarding, decision agent and execution adapters.

Invoked via ``omo resident <subcommand>`` (see cli.py).
"""

from __future__ import annotations

# workspace root from omo/resident/__init__.py:
#   /Workspace/projects/omo/src/omo/resident/__init__.py → parents[5]
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[5]

__all__ = ("WORKSPACE",)
