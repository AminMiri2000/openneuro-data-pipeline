"""Portable helpers for the Makefile.

Make recipes run in `cmd.exe` on Windows and `sh` elsewhere, which share few
commands. Anything beyond calling Python lives here instead, so every target
behaves the same on every OS. Standard library only: this must run before the
virtualenv exists.

Usage:
    python scripts/dev.py help Makefile
    python scripts/dev.py check-python
    python scripts/dev.py clean
    python scripts/dev.py clean-venv
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

# Local artefacts that `clean` removes. Raw files can be re-downloaded and the
# warehouse rebuilt from them, so nothing here is irreplaceable.
CLEAN_TARGETS = ("data/raw", "data/dbt", "data/warehouse.duckdb", "data/warehouse.duckdb.wal")

# Supported interpreter range; keep in sync with requires-python in pyproject.toml.
MIN_PYTHON = (3, 11)
MAX_PYTHON_EXCLUSIVE = (3, 14)
SUPPORTED_TEXT = "3.11 to 3.13"

VENV_DIR = ".venv"

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


def _version_text(version: tuple[int, ...]) -> str:
    return ".".join(str(part) for part in version)


def _venv_version(venv: Path) -> tuple[int, ...] | None:
    """Python version an existing virtualenv was created with, from pyvenv.cfg."""
    config = venv / "pyvenv.cfg"
    if not config.is_file():
        return None
    for line in config.read_text(encoding="utf-8").splitlines():
        key, _, value = line.partition("=")
        if key.strip() in {"version", "version_info"}:
            return tuple(int(part) for part in value.strip().split(".")[:2])
    return None


def check_python(root: Path) -> int:
    """Fail early, with instructions, on an unsupported Python version.

    Without this, installing dbt on an unsupported Python fails deep inside
    pip with a compiler error that does not mention the real cause.
    """
    current = sys.version_info[:2]
    if not MIN_PYTHON <= current < MAX_PYTHON_EXCLUSIVE:
        print(
            f"Python {_version_text(current)} is not supported (need {SUPPORTED_TEXT}).\n"
            "Install Python 3.13, then run for example:\n"
            '  Windows:      make install SYSTEM_PYTHON="py -3.13"\n'
            "  macOS/Linux:  make install SYSTEM_PYTHON=python3.13",
            file=sys.stderr,
        )
        return 1
    existing = _venv_version(root / VENV_DIR)
    if existing is not None and not MIN_PYTHON <= existing < MAX_PYTHON_EXCLUSIVE:
        print(
            f"The existing {VENV_DIR} uses Python {_version_text(existing)}, which is not "
            f"supported. Remove it with `make clean-venv`, then run `make install` again.",
            file=sys.stderr,
        )
        return 1
    print(f"Python {_version_text(current)}: ok")
    return 0


def clean_venv(root: Path) -> int:
    """Delete the virtualenv so `make install` can recreate it."""
    venv = root / VENV_DIR
    if not venv.is_dir():
        return 0
    # On Windows a running program's files cannot be deleted. If this script
    # runs on the venv's own python (an activated venv), deleting it would
    # fail half-way, so stop before touching anything.
    if Path(sys.prefix).resolve() == venv.resolve():
        print(
            f"{VENV_DIR} is active in this terminal, so it cannot delete itself.\n"
            "Run `deactivate`, then `make clean-venv` again.",
            file=sys.stderr,
        )
        return 1
    try:
        shutil.rmtree(venv)
    except PermissionError as exc:
        print(
            f"Could not delete {VENV_DIR}: {exc}\n"
            "Another program is using it (often an editor's Python extension or a\n"
            "terminal where it is activated). Close those, then run `make clean-venv`\n"
            "again; files already removed do not need to be restored.",
            file=sys.stderr,
        )
        return 1
    print(f"removed {VENV_DIR}/")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[0] == "help":
        print_help(Path(argv[1]))
    elif argv == ["check-python"]:
        return check_python(Path.cwd())
    elif argv == ["clean"]:
        clean(Path.cwd())
    elif argv == ["clean-venv"]:
        return clean_venv(Path.cwd())
    else:
        print(__doc__, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
