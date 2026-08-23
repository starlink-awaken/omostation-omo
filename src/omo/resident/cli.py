"""omo resident CLI — `omo resident <subcommand>`.

Dispatches to the resident agent runtime subcommands migrated from bin/ssot
(WP-I). Keeps bin/ssot wrapper scripts compatible.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

SUBCOMMANDS = {
    "ingest": "omo.resident.ingest",
    "daemon": "omo.resident.daemon",
    "sediment": "omo.resident.sediment",
    "memory": "omo.resident.memory",
    "signals": "omo.resident.signals",
    "alert": "omo.resident.alert",
    "decision": "omo.resident.decision",
    "execute": "omo.resident.execute",
}


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if not args or args[0] not in SUBCOMMANDS:
        print(
            f"usage: omo resident {{{','.join(sorted(SUBCOMMANDS))}}} [options]",
            file=sys.stderr,
        )
        return 1
    sub, rest = args[0], args[1:]
    import importlib

    mod = importlib.import_module(SUBCOMMANDS[sub])
    return mod.main(rest)  # type: ignore[attr-defined]


if __name__ == "__main__":
    sys.exit(main())
