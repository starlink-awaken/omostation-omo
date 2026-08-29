#!/usr/bin/env python3
from __future__ import annotations


class WorkflowDispatchError(ValueError):
    """Admission or dispatch packet failed a governance gate."""
