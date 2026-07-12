"""A Jupyter kernel that executes code through a reactive marimo runtime.

Each execution request becomes a cell in a marimo notebook graph. Cells are
keyed by the variables they define: re-running ``x = ...`` replaces the
previous cell that defined ``x``, and marimo reactively re-runs every cell
that depends on it, streaming their new outputs back to the client.

The kernel also watches the notebook file on disk (it locates the file an
executed cell came from). Saving the file re-runs the cells that changed —
the Zed equivalent of marimo.nvim's sync-on-InsertLeave — or, in lazy mode,
queues them to run with the next execution. Cells like ``# marimo: lazy``,
``# marimo: eager``, and ``# marimo: watch <path>`` control this at runtime.
"""

from __future__ import annotations

import difflib
import json
import os
import platform
import queue
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, cast

from ipykernel.kernelbase import Kernel

from marimo import __version__ as marimo_version
from marimo._ast.compiler import compile_cell
from marimo._messaging.cell_output import CellChannel, CellOutput
from marimo._messaging.notification import (
    CellNotification,
    CompletedRunNotification,
    InterruptedNotification,
)
from marimo._messaging.serde import deserialize_kernel_message
from marimo._runtime.commands import (
    CreateNotebookCommand,
    SyncGraphCommand,
    UpdateUIElementCommand,
)
from marimo._types.ids import CellId_t

from marimo_zed import __version__
from marimo_zed.bridge import MarimoBridge
from marimo_zed.watcher import find_notebook_files, normalize_cell, split_cells

_TAG_RE = re.compile(r"<[^>]+>")

_DATA_URI_RE = re.compile(r"^data:[\w./+-]+;base64,")

_SETTLE_SECONDS = 0.05

# A cell that is nothing but this comment is a command to the kernel.
_CONTROL_RE = re.compile(r"#\s*marimo:\s*(.+)")

_POLL_SECONDS = 0.2
_DISCOVERY_POLLS = 10  # retry locating a cell's file every N polls (~2s)
_MAX_WATCHED_FILES = 4
_MAX_DISCOVERY_ATTEMPTS = 150  # give up locating a cell's file after ~5 min


def _strip_tags(html: str) -> str:
    return _TAG_RE.sub("", html)


def _strip_data_uri(value: Any) -> Any:
    """Jupyter expects bare base64 for binary mimetypes; marimo sends data URIs."""
    if isinstance(value, str):
        match = _DATA_URI_RE.match(value)
        if match:
            return value[match.end() :]
    return value


class MarimoZedKernel(Kernel):
    implementation = "marimo_zed"
    implementation_version = __version__
    banner = f"marimo {marimo_version} — reactive Python kernel"
    language_info = {
        "name": "python",
        "version": platform.python_version(),
        "mimetype": "text/x-python",
        "file_extension": ".py",
        "pygments_lexer": "ipython3",
        "codemirror_mode": {"name": "ipython", "version": 3},
    }

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.bridge: MarimoBridge | None = None
        # graph state: cell id -> code / defined names / referenced names
        self._cells: dict[str, str] = {}
        self._defs: dict[str, set[str]] = {}
        self._refs: dict[str, set[str]] = {}
        # Cells the runtime's dataflow graph knows about. The runtime only
        # registers a cell when it appears in a sync's run_ids, so cells
        # adopted from a watched file (or carried across a runtime restart)
        # are invisible to it until we explicitly run them.
        self._runtime_cells: set[str] = set()
        self._notifications: queue.Queue[Any] = queue.Queue()
        self._completed_runs = 0
        self._statuses: dict[str, str] = {}
        # Zed associates an output area with the execute_request that created
        # it. Keep that request per marimo cell so reactive reruns can update
        # the cell they belong to instead of appending below the latest cell.
        self._cell_parents: dict[str, dict[str, Any]] = {}
        self._reader_thread: threading.Thread | None = None
        self._reader_stop = threading.Event()
        # file watching: rerun (eager) or queue (lazy) cells when their file
        # is saved; guarded by _graph_lock, which serializes graph mutation
        # and output pumping between the shell thread and the watch thread.
        self._graph_lock = threading.RLock()
        mode = os.environ.get("MARIMO_ZED_EXECUTION", "eager")
        self._execution_mode = mode if mode in ("eager", "lazy") else "eager"
        self._pending_stale: set[str] = set()
        self._watched: dict[str, tuple[float, int]] = {}  # path -> (mtime, size)
        self._file_cells: dict[str, list[str]] = {}  # path -> cells at last sync
        self._undiscovered: dict[str, tuple[frozenset[str], int]] = {}
        self._discover_asap = False
        self._last_execute_parent: dict[str, Any] | None = None
        self._oob_parent: dict[str, Any] | None = None
        self._watch_thread: threading.Thread | None = None
        self._watch_stop = threading.Event()

    # ------------------------------------------------------------------
    # bridge lifecycle
    # ------------------------------------------------------------------

    def _ensure_started(self) -> None:
        if self.bridge is not None and self.bridge.is_running():
            return
        self._stop_bridge()
        self.bridge = MarimoBridge()
        self.bridge.launch()
        self._reader_stop.clear()
        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()
        # Initialize an empty notebook; cells are added via SyncGraphCommand.
        existing_ids = tuple(cast(CellId_t, cid) for cid in self._cells)
        self.bridge.queue_manager.control_queue.put(
            CreateNotebookCommand(
                execution_requests=(),
                cell_ids=existing_ids,
                set_ui_element_value_request=UpdateUIElementCommand(
                    object_ids=[], values=[]
                ),
                auto_run=False,
            )
        )
        # After a (re)start the runtime has no state or graph; previously
        # known cells rejoin run_ids via _expand_run_ids as executions need
        # them (as ancestors or dependents of what runs).
        self._statuses = {}
        self._runtime_cells = set()

    def _stop_bridge(self) -> None:
        self._reader_stop.set()
        bridge = self.bridge
        reader = self._reader_thread
        self.bridge = None
        self._reader_thread = None
        if bridge is not None:
            bridge.close()
        if reader is not None and reader is not threading.current_thread():
            reader.join(timeout=1)

    def _read_loop(self) -> None:
        bridge = self.bridge
        assert bridge is not None
        while not self._reader_stop.is_set():
            try:
                raw = bridge.queue_manager.stream_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            except Exception:
                return
            if raw is None:
                continue
            try:
                self._notifications.put(deserialize_kernel_message(raw))
            except Exception:
                continue

    # ------------------------------------------------------------------
    # execution
    # ------------------------------------------------------------------

    def do_execute(
        self,
        code: str,
        silent: bool,
        store_history: bool = True,
        user_expressions: dict[str, Any] | None = None,
        allow_stdin: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self._last_execute_parent = self.get_parent("shell")
        code = normalize_cell(code)
        if not code:
            return self._ok_reply()

        command = _CONTROL_RE.fullmatch(code)
        if command is not None:
            return self._do_control(command.group(1).strip(), silent)

        try:
            self._ensure_started()
        except Exception as exc:
            return self._error_reply("KernelLaunchError", str(exc), silent)

        try:
            compiled = compile_cell(code, cell_id=cast(CellId_t, uuid.uuid4().hex))
        except SyntaxError as exc:
            return self._error_reply("SyntaxError", str(exc), silent)
        except Exception as exc:
            return self._error_reply(type(exc).__name__, str(exc), silent)

        with self._graph_lock:
            cell_id, delete_ids, _ = self._register_cell(code, compiled)
            if self._last_execute_parent is not None:
                # If this cell previously rendered under another request's
                # output area, blank that area — it would keep showing the
                # cell's old output next to the new one.
                stale_parent = self._cell_parents.get(cell_id)
                if self._parent_msg_id(stale_parent) != self._parent_msg_id(
                    self._last_execute_parent
                ):
                    self._clear_parent_area(stale_parent)
                self._cell_parents[cell_id] = self._last_execute_parent
            self._queue_discovery(code, compiled)

            # Drain notifications left over from a previous run (state only).
            self._drain_pending()

            run_ids = [cell_id]
            self._pending_stale.discard(cell_id)
            if self._pending_stale:
                stale = self._stale_ancestors(cell_id)
                # Dependents of what runs re-execute reactively; queued ones
                # must run with their new code rather than the old.
                stale |= (
                    self._reactively_affected({cell_id} | stale) & self._pending_stale
                )
                stale.discard(cell_id)
                self._pending_stale -= stale
                run_ids = [*sorted(stale), cell_id]

            run_ids = self._expand_run_ids(run_ids)
            baseline_runs = self._completed_runs
            self._clear_cell_outputs(
                self._reactively_affected(set(run_ids)), silent=silent
            )
            self._send_sync(run_ids, delete_ids)

            errored, interrupted = self._pump_until_complete(
                target_id=cell_id, baseline_runs=baseline_runs, silent=silent
            )

        if interrupted:
            return self._error_reply(
                "KeyboardInterrupt", "execution interrupted", silent
            )
        if errored:
            return self._error_reply(
                "MarimoRuntimeError", "cell raised an error (see output above)", silent
            )
        return self._ok_reply()

    def _resolve_cell_id(self, code: str, defs: set[str]) -> tuple[str, list[str]]:
        """Pick the cell id for this execution.

        A cell that redefines names owned by existing cells replaces the
        first such cell and deletes the rest (marimo forbids multiple
        definitions of a name). Definition-free code re-uses a cell with
        identical source, otherwise becomes a new cell.
        """
        if defs:
            overlapping = [
                cid for cid, cell_defs in self._defs.items() if cell_defs & defs
            ]
            if overlapping:
                cell_id = overlapping[0]
                self._defs[cell_id] = defs
                return cell_id, overlapping[1:]
            cell_id = uuid.uuid4().hex
            self._defs[cell_id] = defs
            return cell_id, []
        for cid, cell_code in self._cells.items():
            if cell_code == code and not self._defs.get(cid):
                return cid, []
        cell_id = uuid.uuid4().hex
        self._defs[cell_id] = set()
        return cell_id, []

    def _register_cell(
        self, code: str, compiled: Any
    ) -> tuple[str, list[str], str | None]:
        """Store a cell in the kernel-side graph.

        Returns (cell_id, ids of replaced cells, the cell's previous code).
        """
        cell_id, delete_ids = self._resolve_cell_id(code, set(compiled.defs))
        previous = self._cells.get(cell_id)
        self._cells[cell_id] = code
        self._refs[cell_id] = set(compiled.refs)
        for stale_id in delete_ids:
            self._cells.pop(stale_id, None)
            self._defs.pop(stale_id, None)
            self._refs.pop(stale_id, None)
            self._statuses.pop(stale_id, None)
            self._clear_parent_area(self._cell_parents.pop(stale_id, None))
            self._pending_stale.discard(stale_id)
            self._runtime_cells.discard(stale_id)
        return cell_id, delete_ids, previous

    def _stale_ancestors(self, target_id: str) -> set[str]:
        """Pending-stale cells the target depends on, transitively."""
        needed = set(self._refs.get(target_id, ()))
        ancestors: set[str] = set()
        grew = True
        while grew:
            grew = False
            for cid, defs in self._defs.items():
                if cid in ancestors or cid == target_id:
                    continue
                if defs & needed:
                    ancestors.add(cid)
                    needed |= self._refs.get(cid, set())
                    grew = True
        return ancestors & self._pending_stale

    def _reactively_affected(self, seed_ids: set[str]) -> set[str]:
        """Cells the runtime would re-run if the seeds ran: transitive dependents."""
        provided: set[str] = set()
        for cid in seed_ids:
            provided |= self._defs.get(cid, set())
        affected = set(seed_ids)
        grew = True
        while grew:
            grew = False
            for cid, refs in self._refs.items():
                if cid not in affected and refs & provided:
                    affected.add(cid)
                    provided |= self._defs.get(cid, set())
                    grew = True
        return affected

    def _expand_run_ids(self, run_ids: list[str]) -> list[str]:
        """Add cells the runtime graph lacks but this run needs.

        The runtime registers a cell only when it appears in run_ids
        (sync_graph ignores other entries in ``cells``), so a cell adopted
        from a watched file or predating a runtime restart neither reruns
        as a dependent nor provides values as an ancestor until it is run
        explicitly: include unknown dependents of what runs, then close
        over the unknown ancestors those cells read from.
        """
        seeds = set(run_ids)
        extra = self._reactively_affected(seeds) - self._runtime_cells - seeds
        needed: set[str] = set()
        for cid in seeds | extra:
            needed |= self._refs.get(cid, set())
        grew = True
        while grew:
            grew = False
            for cid, defs in self._defs.items():
                if cid in seeds or cid in extra or cid in self._runtime_cells:
                    continue
                if defs & needed:
                    extra.add(cid)
                    needed |= self._refs.get(cid, set())
                    grew = True
        self._pending_stale -= extra
        return [*run_ids, *sorted(extra)]

    def _send_sync(self, run_ids: list[str], delete_ids: list[str]) -> None:
        assert self.bridge is not None
        self._runtime_cells.update(run_ids)
        self.bridge.queue_manager.control_queue.put(
            SyncGraphCommand(
                cells={
                    cast(CellId_t, cid): cell_code
                    for cid, cell_code in self._cells.items()
                },
                run_ids=[cast(CellId_t, cid) for cid in run_ids],
                delete_ids=[cast(CellId_t, cid) for cid in delete_ids],
            )
        )

    # ------------------------------------------------------------------
    # control commands (`# marimo: ...` cells)
    # ------------------------------------------------------------------

    def _do_control(self, arg: str, silent: bool) -> dict[str, Any]:
        parts = arg.split()
        if parts in (["eager"], ["lazy"]):
            mode = parts[0]
            with self._graph_lock:
                self._execution_mode = mode
            if mode == "lazy":
                self._emit_stream(
                    "stdout",
                    "marimo: lazy — saved changes queue and run with your next execution\n",
                    silent,
                )
            else:
                self._emit_stream(
                    "stdout", "marimo: eager — watched cells rerun on save\n", silent
                )
                self._run_pending_stale(silent)
            return self._ok_reply()
        if parts and parts[0] == "watch" and len(parts) >= 2:
            path = Path(arg.split(None, 1)[1].strip()).expanduser()
            if not path.is_file():
                return self._error_reply(
                    "MarimoCommandError", f"no such file: {path}", silent
                )
            if not self._watch_file(str(path.resolve())):
                return self._error_reply(
                    "MarimoCommandError",
                    f"already watching {_MAX_WATCHED_FILES} files",
                    silent,
                )
            return self._ok_reply()
        return self._error_reply(
            "MarimoCommandError",
            f"unknown marimo command {arg!r} (expected eager, lazy, or watch <path>)",
            silent,
        )

    def _run_pending_stale(self, silent: bool) -> None:
        with self._graph_lock:
            stale = sorted(cid for cid in self._pending_stale if cid in self._cells)
            self._pending_stale.clear()
            if not stale or self.bridge is None or not self.bridge.is_running():
                return
            run_ids = self._expand_run_ids(stale)
            self._drain_pending()
            baseline_runs = self._completed_runs
            self._clear_cell_outputs(
                self._reactively_affected(set(run_ids)), silent=silent
            )
            self._send_sync(run_ids, [])
            self._pump_until_complete(
                target_id=None, baseline_runs=baseline_runs, silent=silent
            )

    # ------------------------------------------------------------------
    # file watching
    # ------------------------------------------------------------------

    def _queue_discovery(self, code: str, compiled: Any) -> None:
        """Remember an executed cell whose source file we have not located."""
        if any(code in cells for cells in self._file_cells.values()):
            return
        if len(self._watched) >= _MAX_WATCHED_FILES:
            return
        self._undiscovered[code] = (frozenset(compiled.defs), 0)
        self._discover_asap = True
        self._ensure_watch_thread()

    def _discover(self) -> None:
        with self._graph_lock:
            pending = dict(self._undiscovered)
        root = Path.cwd()
        for code, (defs, attempts) in pending.items():
            exact, by_defs = find_notebook_files(code, set(defs), root)
            if exact:
                targets = exact[:_MAX_WATCHED_FILES]
            elif len(by_defs) == 1:
                targets = by_defs
            else:
                targets = []
            with self._graph_lock:
                if targets:
                    self._undiscovered.pop(code, None)
                else:
                    attempts += 1
                    if attempts >= _MAX_DISCOVERY_ATTEMPTS:
                        self._undiscovered.pop(code, None)
                    else:
                        self._undiscovered[code] = (defs, attempts)
            for path in targets:
                self._watch_file(str(path.resolve()))

    def _watch_file(self, path: str) -> bool:
        with self._graph_lock:
            if path in self._watched:
                return True
            if len(self._watched) >= _MAX_WATCHED_FILES:
                return False
            try:
                st = os.stat(path)
                text = Path(path).read_text(errors="replace")
            except OSError:
                return False
            self._watched[path] = (st.st_mtime, st.st_size)
            self._apply_file_change(path, text)
            for code in list(self._undiscovered):
                if code in self._file_cells.get(path, []):
                    self._undiscovered.pop(code, None)
            hint = (
                "cells rerun on save"
                if self._execution_mode == "eager"
                else "saved changes run with your next execution"
            )
        try:
            shown = str(Path(path).relative_to(Path.cwd()))
        except ValueError:
            shown = path
        self._emit_oob_stream("stdout", f"marimo: watching {shown} — {hint}\n")
        self._ensure_watch_thread()
        return True

    def _apply_file_change(self, path: str, text: str) -> None:
        """Diff a saved file against its last-seen cells and sync the graph."""
        with self._graph_lock:
            cells = [c for c in (normalize_cell(c) for c in split_cells(text)) if c]
            previous_cells = self._file_cells.get(path)
            self._file_cells[path] = cells
            run_ids: list[str] = []
            broken = False
            if previous_cells is None:
                # First look at this file: adopt and run every new cell.
                for code in cells:
                    self._adopt_cell(code, run_ids)
                removed: list[str] = []
            else:
                previous_set, current_set = set(previous_cells), set(cells)
                adopted: dict[str, str] = {}
                for code in cells:
                    if code not in previous_set:
                        cid = self._adopt_cell(code, run_ids)
                        if cid is None:
                            broken = True
                        else:
                            adopted[code] = cid
                removed = [c for c in previous_cells if c not in current_set]
                self._transfer_parents(previous_cells, cells, adopted)

            delete_ids: list[str] = []
            # A cell that no longer compiles is usually one being edited, not
            # one being deleted — hold off on deletions until the file parses.
            if not broken:
                for code in removed:
                    cid = next(
                        (c for c, src in self._cells.items() if src == code), None
                    )
                    if cid is None:
                        continue
                    delete_ids.append(cid)
                    self._cells.pop(cid, None)
                    self._defs.pop(cid, None)
                    self._refs.pop(cid, None)
                    self._statuses.pop(cid, None)
                    self._clear_parent_area(self._cell_parents.pop(cid, None))
                    self._pending_stale.discard(cid)
                    self._runtime_cells.discard(cid)

            if not run_ids and not delete_ids:
                return
            if (
                self._execution_mode == "lazy"
                or self.bridge is None
                or not self.bridge.is_running()
            ):
                # Deleted cells reach the runtime as orphans on the next sync.
                self._pending_stale.update(run_ids)
                return

            if run_ids and self._pending_stale:
                queued = self._reactively_affected(set(run_ids)) & self._pending_stale
                self._pending_stale -= queued
                run_ids.extend(sorted(queued))
            run_ids = self._expand_run_ids(run_ids)

            self._drain_pending()
            baseline_runs = self._completed_runs
            self._clear_cell_outputs(
                self._reactively_affected(set(run_ids)),
                silent=self._last_execute_parent is None,
            )
            self._send_sync(run_ids, delete_ids)
            if run_ids:
                self._oob_parent = self._last_execute_parent
                try:
                    self._pump_until_complete(
                        target_id=None,
                        baseline_runs=baseline_runs,
                        silent=self._oob_parent is None,
                    )
                finally:
                    self._oob_parent = None

    def _adopt_cell(self, code: str, run_ids: list[str]) -> str | None:
        """Register a cell parsed from a watched file.

        Appends the cell to ``run_ids`` when its code changed. Returns the cell
        id, or None if the cell does not compile.
        """
        try:
            compiled = compile_cell(code, cell_id=cast(CellId_t, uuid.uuid4().hex))
        except Exception:
            return None
        cell_id, _, previous = self._register_cell(code, compiled)
        if previous != code:
            run_ids.append(cell_id)
        return cell_id

    def _transfer_parents(
        self,
        previous_cells: list[str],
        cells: list[str],
        adopted: dict[str, str],
    ) -> None:
        """Keep an edited cell's output area across a change of identity.

        An edit can change which graph cell a file cell maps to (definition-
        free code matches only on identical source, and renamed definitions
        stop overlapping). Align the old and new cell lists and hand each
        replaced cell's output area to its successor, so the rerun lands
        under the same source cell instead of falling back to the area of
        whatever executed last.
        """
        matcher = difflib.SequenceMatcher(a=previous_cells, b=cells, autojunk=False)
        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag != "replace":
                continue
            for old_code, new_code in zip(previous_cells[i1:i2], cells[j1:j2]):
                new_id = adopted.get(new_code)
                if new_id is None or new_id in self._cell_parents:
                    continue
                old_id = next(
                    (cid for cid, src in self._cells.items() if src == old_code),
                    None,
                )
                if old_id is None or old_id == new_id:
                    continue
                parent = self._cell_parents.pop(old_id, None)
                if parent is not None:
                    self._cell_parents[new_id] = parent

    def _ensure_watch_thread(self) -> None:
        if self._watch_thread is not None and self._watch_thread.is_alive():
            return
        self._watch_stop.clear()
        self._watch_thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._watch_thread.start()

    def _watch_loop(self) -> None:
        polls = 0
        while not self._watch_stop.wait(_POLL_SECONDS):
            polls += 1
            for path, signature in list(self._watched.items()):
                try:
                    st = os.stat(path)
                except OSError:
                    continue
                new_signature = (st.st_mtime, st.st_size)
                if new_signature == signature:
                    continue
                self._watched[path] = new_signature
                try:
                    text = Path(path).read_text(errors="replace")
                except OSError:
                    continue
                try:
                    self._apply_file_change(path, text)
                except Exception:
                    pass
            if self._undiscovered and (
                self._discover_asap or polls % _DISCOVERY_POLLS == 0
            ):
                self._discover_asap = False
                try:
                    self._discover()
                except Exception:
                    pass

    def _stop_watching(self) -> None:
        self._watch_stop.set()
        thread = self._watch_thread
        self._watch_thread = None
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1)

    def _drain_pending(self) -> None:
        while True:
            try:
                note = self._notifications.get_nowait()
            except queue.Empty:
                return
            self._track(note)

    def _track(self, note: Any) -> None:
        if isinstance(note, CompletedRunNotification):
            self._completed_runs += 1
        elif isinstance(note, CellNotification) and note.status is not None:
            self._statuses[str(note.cell_id)] = str(note.status)

    def _pump_until_complete(
        self, *, target_id: str | None, baseline_runs: int, silent: bool
    ) -> tuple[bool, bool]:
        """Emit outputs until the reactive run settles.

        Returns (target_errored, interrupted).
        """
        errored = False
        interrupted = False
        settled_since: float | None = None

        while True:
            try:
                try:
                    note = self._notifications.get(timeout=0.02)
                except queue.Empty:
                    note = None
            except KeyboardInterrupt:
                interrupted = True
                if self.bridge is not None:
                    self.bridge.interrupt()
                continue

            if note is not None:
                settled_since = None
                self._track(note)
                if isinstance(note, InterruptedNotification):
                    interrupted = True
                elif isinstance(note, CellNotification):
                    if self._emit_cell_notification(note, silent=silent):
                        if target_id is not None and str(note.cell_id) == target_id:
                            errored = True
                continue

            if self.bridge is None or not self.bridge.is_running():
                stderr = self.bridge.read_stderr() if self.bridge else ""
                self._emit_stream("stderr", f"marimo runtime died.\n{stderr}", silent)
                return True, interrupted

            run_done = self._completed_runs > baseline_runs
            busy = any(
                status in ("running", "queued") for status in self._statuses.values()
            )
            if run_done and not busy:
                if settled_since is None:
                    settled_since = time.monotonic()
                elif time.monotonic() - settled_since >= _SETTLE_SECONDS:
                    return errored, interrupted

    # ------------------------------------------------------------------
    # output conversion
    # ------------------------------------------------------------------

    def _emit_cell_notification(self, note: CellNotification, *, silent: bool) -> bool:
        """Send a cell's console/output to the client. Returns True on error output."""
        errored = False
        parent = self._cell_parents.get(str(note.cell_id))
        console = note.console
        if console is not None:
            outputs = console if isinstance(console, list) else [console]
            for out in outputs:
                if out.channel == CellChannel.STDOUT:
                    self._emit_stream("stdout", str(out.data), silent, parent=parent)
                elif out.channel == CellChannel.STDERR:
                    self._emit_stream("stderr", str(out.data), silent, parent=parent)

        output = note.output
        if output is not None:
            if output.channel == CellChannel.MARIMO_ERROR:
                errored = True
                self._emit_error_output(output, silent, parent=parent)
            elif parent is not None:
                # A cell that has never been executed from the editor has no
                # output area of its own. Routing its value to another cell's
                # area shows it under the wrong cell and duplicates it on
                # every rerun — show only console output and errors from
                # such cells.
                data = self._display_data(output)
                if data:
                    self._send(
                        "display_data",
                        {"data": data, "metadata": {}},
                        silent,
                        parent=parent,
                    )
        return errored

    def _emit_error_output(
        self,
        output: CellOutput,
        silent: bool,
        *,
        parent: dict[str, Any] | None = None,
    ) -> None:
        messages: list[str] = []
        errors = output.data if isinstance(output.data, list) else [output.data]
        for err in errors:
            describe = getattr(err, "describe", None)
            messages.append(describe() if callable(describe) else str(err))
        text = "\n".join(messages) or "marimo reported an error"
        self._send(
            "error",
            {"ename": "MarimoError", "evalue": text, "traceback": [text]},
            silent,
            parent=parent,
        )

    def _display_data(self, output: CellOutput) -> dict[str, Any] | None:
        mimetype = str(output.mimetype)
        data = output.data
        if data in ("", None):
            return None
        if mimetype == "application/vnd.marimo+mimebundle":
            bundle = data
            if isinstance(bundle, str):
                try:
                    bundle = json.loads(bundle)
                except ValueError:
                    return {"text/plain": bundle}
            if isinstance(bundle, dict):
                return {key: _strip_data_uri(val) for key, val in bundle.items()}
            return {"text/plain": str(bundle)}
        # marimo's text/markdown payloads are rendered HTML, not markdown
        if mimetype in ("text/html", "text/markdown", "application/vnd.marimo+html"):
            html = str(data)
            return {"text/html": html, "text/plain": _strip_tags(html)}
        if mimetype == "application/json":
            if isinstance(data, str):
                try:
                    return {"application/json": json.loads(data)}
                except ValueError:
                    return {"text/plain": data}
            return {"application/json": data}
        if mimetype.startswith("image/"):
            return {mimetype: _strip_data_uri(data)}
        if mimetype.startswith(("text/", "application/")):
            return {mimetype: data}
        return {"text/plain": str(data)}

    # ------------------------------------------------------------------
    # jupyter plumbing
    # ------------------------------------------------------------------

    def _send(
        self,
        msg_type: str,
        content: dict[str, Any],
        silent: bool,
        *,
        parent: dict[str, Any] | None = None,
    ) -> None:
        if silent:
            return
        parent = parent or self._oob_parent
        if parent is not None:
            # Watcher-triggered runs happen outside any request; attach their
            # outputs to an execution so Zed displays them. Cell output uses
            # that cell's own execution; other messages use the latest one.
            self.session.send(self.iopub_socket, msg_type, content, parent=parent)
        else:
            self.send_response(self.iopub_socket, msg_type, content)

    def _clear_cell_outputs(self, cell_ids: set[str], *, silent: bool) -> None:
        """Clear output areas that are about to receive reactive replacements."""
        for cell_id in cell_ids:
            parent = self._cell_parents.get(cell_id)
            if parent is not None:
                self._send("clear_output", {"wait": False}, silent, parent=parent)

    @staticmethod
    def _parent_msg_id(parent: dict[str, Any] | None) -> Any:
        if not parent:
            return None
        return parent.get("header", {}).get("msg_id")

    def _clear_parent_area(self, parent: dict[str, Any] | None) -> None:
        """Blank the output area of a cell that was replaced, moved, or deleted.

        Without this, the area keeps showing the cell's last output — a stale
        duplicate of the value now rendered elsewhere (or of nothing at all).
        """
        if parent is None:
            return
        self.session.send(
            self.iopub_socket, "clear_output", {"wait": False}, parent=parent
        )

    def _emit_oob_stream(self, name: str, text: str) -> None:
        parent = self._last_execute_parent
        if parent is None:
            return
        self.session.send(
            self.iopub_socket, "stream", {"name": name, "text": text}, parent=parent
        )

    def _emit_stream(
        self,
        name: str,
        text: str,
        silent: bool,
        *,
        parent: dict[str, Any] | None = None,
    ) -> None:
        if text:
            self._send("stream", {"name": name, "text": text}, silent, parent=parent)

    def _ok_reply(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "execution_count": self.execution_count,
            "payload": [],
            "user_expressions": {},
        }

    def _error_reply(self, ename: str, evalue: str, silent: bool) -> dict[str, Any]:
        self._send(
            "error",
            {"ename": ename, "evalue": evalue, "traceback": [f"{ename}: {evalue}"]},
            silent,
        )
        return {
            "status": "error",
            "execution_count": self.execution_count,
            "ename": ename,
            "evalue": evalue,
            "traceback": [f"{ename}: {evalue}"],
        }

    def do_shutdown(self, restart: bool) -> dict[str, Any]:
        self._stop_watching()
        self._stop_bridge()
        if restart:
            self._cells = {}
            self._defs = {}
            self._refs = {}
            self._runtime_cells = set()
            self._statuses = {}
            self._cell_parents = {}
            self._pending_stale = set()
            self._watched = {}
            self._file_cells = {}
            self._undiscovered = {}
        return {"status": "ok", "restart": restart}
