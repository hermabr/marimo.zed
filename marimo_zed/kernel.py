"""A Jupyter kernel that executes code through a reactive marimo runtime.

Each execution request becomes a cell in a marimo notebook graph. Cells are
keyed by the variables they define: re-running ``x = ...`` replaces the
previous cell that defined ``x``, and marimo reactively re-runs every cell
that depends on it, streaming their new outputs back to the client.
"""

from __future__ import annotations

import json
import platform
import queue
import re
import threading
import time
import uuid
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

_TAG_RE = re.compile(r"<[^>]+>")

_DATA_URI_RE = re.compile(r"^data:[\w./+-]+;base64,")

_SETTLE_SECONDS = 0.05


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
        # graph state: cell id -> code / defined names
        self._cells: dict[str, str] = {}
        self._defs: dict[str, set[str]] = {}
        self._notifications: queue.Queue[Any] = queue.Queue()
        self._completed_runs = 0
        self._statuses: dict[str, str] = {}
        self._reader_thread: threading.Thread | None = None
        self._reader_stop = threading.Event()

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
        # After a (re)start the runtime has no state; previously known cells
        # are re-synced on the next execution.
        self._statuses = {}

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
        if not code.strip():
            return self._ok_reply()

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

        cell_id, delete_ids = self._resolve_cell_id(code, set(compiled.defs))
        self._cells[cell_id] = code
        for stale_id in delete_ids:
            self._cells.pop(stale_id, None)
            self._defs.pop(stale_id, None)
            self._statuses.pop(stale_id, None)

        # Drain notifications left over from a previous run (state only).
        self._drain_pending()

        baseline_runs = self._completed_runs
        assert self.bridge is not None
        self.bridge.queue_manager.control_queue.put(
            SyncGraphCommand(
                cells={
                    cast(CellId_t, cid): cell_code
                    for cid, cell_code in self._cells.items()
                },
                run_ids=[cast(CellId_t, cell_id)],
                delete_ids=[cast(CellId_t, cid) for cid in delete_ids],
            )
        )

        errored, interrupted = self._pump_until_complete(
            target_id=cell_id, baseline_runs=baseline_runs, silent=silent
        )

        if interrupted:
            return self._error_reply("KeyboardInterrupt", "execution interrupted", silent)
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
        self, *, target_id: str, baseline_runs: int, silent: bool
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
                        if str(note.cell_id) == target_id:
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
        console = note.console
        if console is not None:
            outputs = console if isinstance(console, list) else [console]
            for out in outputs:
                if out.channel == CellChannel.STDOUT:
                    self._emit_stream("stdout", str(out.data), silent)
                elif out.channel == CellChannel.STDERR:
                    self._emit_stream("stderr", str(out.data), silent)

        output = note.output
        if output is not None:
            if output.channel == CellChannel.MARIMO_ERROR:
                errored = True
                self._emit_error_output(output, silent)
            else:
                data = self._display_data(output)
                if data:
                    self._send(
                        "display_data", {"data": data, "metadata": {}}, silent
                    )
        return errored

    def _emit_error_output(self, output: CellOutput, silent: bool) -> None:
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

    def _send(self, msg_type: str, content: dict[str, Any], silent: bool) -> None:
        if silent:
            return
        self.send_response(self.iopub_socket, msg_type, content)

    def _emit_stream(self, name: str, text: str, silent: bool) -> None:
        if text:
            self._send("stream", {"name": name, "text": text}, silent)

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
        self._stop_bridge()
        if restart:
            self._cells = {}
            self._defs = {}
            self._statuses = {}
        return {"status": "ok", "restart": restart}
