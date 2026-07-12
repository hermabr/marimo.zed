"""Helpers for watching notebook files: cell parsing and file discovery.

Zed gives the kernel no editor events (unlike marimo.nvim, which syncs on
``InsertLeave``), so the closest equivalent is reacting to the notebook file
being saved: the kernel locates the file an executed cell came from and polls
it for changes. This module holds the pure parts — splitting a file into
``# %%`` cells and finding candidate files for an executed cell.
"""

from __future__ import annotations

import os
import re
import uuid
from pathlib import Path
from typing import Iterator, cast

from marimo._ast.compiler import compile_cell
from marimo._types.ids import CellId_t

CELL_MARKER_RE = re.compile(r"^\s*#\s*%%")

_SKIP_DIRS = {"node_modules", "__pycache__"}
_MAX_FILE_BYTES = 1 << 20
_MAX_FILES_SCANNED = 4000


def split_cells(text: str) -> list[str]:
    """Split file text into cells on ``# %%`` markers.

    A file without markers is a single cell.
    """
    cells: list[list[str]] = [[]]
    for line in text.splitlines():
        if CELL_MARKER_RE.match(line):
            cells.append([])
        else:
            cells[-1].append(line)
    return ["\n".join(chunk) for chunk in cells]


def normalize_cell(code: str) -> str:
    """Canonical cell text: marker lines dropped, trailing whitespace stripped."""
    lines = [
        line.rstrip() for line in code.splitlines() if not CELL_MARKER_RE.match(line)
    ]
    return "\n".join(lines).strip()


def iter_python_files(root: Path) -> Iterator[Path]:
    """Yield project ``.py`` files, skipping hidden dirs, venvs, and junk."""
    scanned = 0
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [
            d
            for d in dirnames
            if not d.startswith(".")
            and d not in _SKIP_DIRS
            and not (Path(dirpath) / d / "pyvenv.cfg").is_file()
        ]
        for filename in filenames:
            if not filename.endswith(".py"):
                continue
            scanned += 1
            if scanned > _MAX_FILES_SCANNED:
                return
            yield Path(dirpath) / filename


def _cell_defs(code: str) -> set[str]:
    try:
        return set(compile_cell(code, cell_id=cast(CellId_t, uuid.uuid4().hex)).defs)
    except Exception:
        return set()


def find_notebook_files(
    code: str, defs: set[str], root: Path
) -> tuple[list[Path], list[Path]]:
    """Locate files an executed cell may have come from.

    Returns ``(exact, by_defs)``: files containing a cell equal to ``code``,
    and ``# %%``-marked files where some cell defines one of ``defs`` — the
    fallback for a cell that was edited before its file was first saved.
    """
    exact: list[Path] = []
    by_defs: list[Path] = []
    for path in iter_python_files(root):
        try:
            if path.stat().st_size > _MAX_FILE_BYTES:
                continue
            text = path.read_text(errors="replace")
        except OSError:
            continue
        cells = [normalize_cell(chunk) for chunk in split_cells(text)]
        if code in cells:
            exact.append(path)
        elif defs and any(CELL_MARKER_RE.match(line) for line in text.splitlines()):
            if any(defs & _cell_defs(cell) for cell in cells if cell):
                by_defs.append(path)
    return exact, by_defs
