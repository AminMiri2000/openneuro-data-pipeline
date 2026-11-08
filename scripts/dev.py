"""Portable helpers for the Makefile.

Make recipes run in `cmd.exe` on Windows and `sh` elsewhere, which share few
commands. Anything beyond calling Python lives here instead, so every target
behaves the same on every OS. Standard library only: this must run before the
virtualenv exists.

Usage:
    python scripts/dev.py help Makefile
    python scripts/dev.py clean
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

# Local artefacts that `clean` removes. Raw files can be re-downloaded and the
# warehouse rebuilt from them, so nothing here is irreplaceable.
CLEAN_TARGETS = ("data/raw", "data/warehouse.duckdb", "data/warehouse.duckdb.wal")

# Matches "target:   ## description" lines in the Makefile.
HELP_LINE = re.compile(r"^([a-z][a-z-]*):.*?##\s*(.+)$")


def print_help(makefile: Path) -> None:
    """Print each documented Makefile target with its description."""
    for line in makefile.read_text(encoding="utf-8").splitlines():
        match = HELP_LINE.match(line)
        if match:
            target, description = match.groups()
            print(f"  {target:<11} {description}")


def clean(root: Path) -> None:
    """Delete local data and the warehouse."""
    for relative in CLEAN_TARGETS:
        path = root / relative
        if path.is_dir():
            shutil.rmtree(path)
            print(f"removed {relative}/")
        elif path.exists():
            path.unlink()
            print(f"removed {relative}")


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[0] == "help":
        print_help(Path(argv[1]))
    elif argv == ["clean"]:
        clean(Path.cwd())
    else:
        print(__doc__, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
