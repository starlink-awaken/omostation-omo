#!/usr/bin/env python3
from __future__ import annotations

from __future__ import annotations
import argparse
import hashlib
import json
import os
import re
import shlex
import signal
import subprocess
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn
import yaml
from ecos.ssot.mof.generated.control.mof_control_models import WorkPacket
from ecos.ssot.tools.work_packet_compiler import (
from .approval_lifecycle import (
from .omo_io import write_text_atomic
from .omo_shared import load_yaml
from .omo_task_schema import validate_task_file
from .omo_worker_core import (
from .omo_worker_dispatch import dispatch_task
from .orchestration_contract import (
from .workflow_dispatch import admit_workflow
from .workflow_mesh import WorkflowMeshStore, new_workflow_event
from .blueprint_control_helpers import (


def _root(value: str) -> Path:
    try:
        root = Path(value).resolve(strict=True)
    except OSError as exc:
        raise BlueprintControlError("authority root is unavailable") from exc
    if not root.is_dir():
        raise BlueprintControlError("authority root is not a directory")
    return root



# 2026-08-29: _dispatch_artifact extracted to blueprint_control_helpers.py
from .blueprint_control_helpers import _dispatch_artifact
