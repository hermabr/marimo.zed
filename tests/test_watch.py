"""End-to-end tests for file watching: cells rerun when their file is saved."""

from __future__ import annotations

import subprocess
import sys
import time

import pytest
from jupyter_client import BlockingKernelClient
from jupyter_client.connect import write_connection_file

from marimo_zed.watcher import normalize_cell, split_cells

TIMEOUT = 60


def test_split_and_normalize_cells():
    text = "import os\n\n# %% second cell\nx = 1\n\n# %%\ny = x + 1\ny\n"
    cells = [normalize_cell(chunk) for chunk in split_cells(text)]
    assert cells == ["import os", "x = 1", "y = x + 1\ny"]


def test_file_without_markers_is_one_cell():
    cells = [c for c in (normalize_cell(c) for c in split_cells("x = 1\ny = 2\n")) if c]
    assert cells == ["x = 1\ny = 2"]


@pytest.fixture(scope="module")
def notebook_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("nb")


@pytest.fixture(scope="module")
def client(notebook_dir, tmp_path_factory):
    connection_file = str(tmp_path_factory.mktemp("conn") / "kernel.json")
    write_connection_file(fname=connection_file, ip="127.0.0.1", key=b"test-key")
    # The kernel runs from the notebook directory, so file discovery scans it.
    proc = subprocess.Popen(
        [sys.executable, "-m", "marimo_zed", "-f", connection_file],
        cwd=notebook_dir,
    )
    client = BlockingKernelClient(connection_file=connection_file)
    client.load_connection_file()
    client.start_channels()
    try:
        client.wait_for_ready(timeout=TIMEOUT)
        yield client
    finally:
        client.stop_channels()
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def run_with_id(client: BlockingKernelClient, code: str) -> tuple[str, str, list[dict]]:
    """Execute code, return (message id, reply status, reactive outputs)."""
    msg_id = client.execute(code)
    outputs: list[dict] = []
    status = None
    idle = False
    reply = None
    while not (idle and reply is not None):
        if reply is None:
            try:
                candidate = client.get_shell_msg(timeout=0.05)
                if candidate["parent_header"].get("msg_id") == msg_id:
                    reply = candidate
                    status = reply["content"]["status"]
            except Exception:
                pass
        try:
            msg = client.get_iopub_msg(timeout=TIMEOUT if not idle else 0.05)
        except Exception:
            continue
        is_current = msg["parent_header"].get("msg_id") == msg_id
        if msg["msg_type"] == "status" and is_current:
            idle = msg["content"]["execution_state"] == "idle"
        elif msg["msg_type"] in ("stream", "display_data", "execute_result", "error"):
            outputs.append(msg)
    return msg_id, status, outputs


def run(client: BlockingKernelClient, code: str) -> tuple[str, list[dict]]:
    """Execute code, returning outputs from the whole reactive run."""
    _, status, outputs = run_with_id(client, code)
    return status, outputs


def _message_text(msg: dict) -> str:
    content = msg.get("content", {})
    if msg["msg_type"] == "stream":
        return str(content.get("text", ""))
    if msg["msg_type"] in ("display_data", "execute_result"):
        data = content.get("data", {})
        return f"{data.get('text/plain', '')}\n{data.get('text/html', '')}"
    if msg["msg_type"] == "error":
        return str(content.get("evalue", ""))
    return ""


def text_of(outputs: list[dict]) -> str:
    return "\n".join(_message_text(msg) for msg in outputs)


def collect_text(client: BlockingKernelClient, seconds: float) -> str:
    """Gather text from every iopub message for a fixed window."""
    chunks: list[str] = []
    deadline = time.monotonic() + seconds
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            msg = client.get_iopub_msg(timeout=remaining)
        except Exception:
            break
        chunks.append(_message_text(msg))
    return "\n".join(chunks)


def wait_for_text(
    client: BlockingKernelClient, needle: str, timeout: float = TIMEOUT
) -> None:
    """Wait until iopub traffic (any parent) contains `needle`."""
    seen: list[str] = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            msg = client.get_iopub_msg(timeout=0.5)
        except Exception:
            continue
        seen.append(_message_text(msg))
        if needle in "\n".join(seen):
            return
    pytest.fail(f"never saw {needle!r} on iopub; saw: {''.join(seen)!r}")


def wait_for_texts(
    client: BlockingKernelClient, needles: set[str], timeout: float = TIMEOUT
) -> list[dict]:
    """Collect iopub messages until all requested text has appeared."""
    messages: list[dict] = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            message = client.get_iopub_msg(timeout=0.5)
        except Exception:
            continue
        messages.append(message)
        text = "\n".join(_message_text(msg) for msg in messages)
        if all(needle in text for needle in needles):
            return messages
    pytest.fail(
        f"never saw all of {needles!r} on iopub; "
        f"saw: {''.join(_message_text(msg) for msg in messages)!r}"
    )


def write_notebook(notebook_dir, x_line: str) -> None:
    (notebook_dir / "notebook.py").write_text(
        f"# %%\n{x_line}\nx\n\n# %%\ny = x + 1\ny\n"
    )


def test_eager_rerun_on_save(client, notebook_dir):
    write_notebook(notebook_dir, "x = 1")

    x_msg_id, status, _ = run_with_id(client, "x = 1\nx")
    assert status == "ok"
    y_msg_id, status, outputs = run_with_id(client, "y = x + 1\ny")
    assert status == "ok"
    assert "2" in text_of(outputs)

    # Saving a change to the x cell reruns it, and y recomputes reactively.
    # Each replacement is routed to the output area for its own source cell.
    write_notebook(notebook_dir, "x = 41")
    messages = wait_for_texts(client, {"41", "42"})
    x_outputs = [msg for msg in messages if "41" in _message_text(msg)]
    y_outputs = [msg for msg in messages if "42" in _message_text(msg)]
    assert x_outputs
    assert y_outputs
    assert all(msg["parent_header"].get("msg_id") == x_msg_id for msg in x_outputs)
    assert all(msg["parent_header"].get("msg_id") == y_msg_id for msg in y_outputs)

    cleared_parents = {
        msg["parent_header"].get("msg_id")
        for msg in messages
        if msg["msg_type"] == "clear_output"
    }
    assert {x_msg_id, y_msg_id} <= cleared_parents


def test_lazy_defers_until_next_execution(client, notebook_dir):
    status, outputs = run(client, "# marimo: lazy")
    assert status == "ok"
    assert "lazy" in text_of(outputs)

    write_notebook(notebook_dir, "x = 100")
    assert "101" not in collect_text(client, 2.0)

    # Running a dependent cell pulls in the queued stale ancestor.
    status, outputs = run(client, "y")
    assert status == "ok"
    assert "101" in text_of(outputs)


def test_eager_toggle_runs_queued_cells(client, notebook_dir):
    write_notebook(notebook_dir, "x = 4242")
    # Let the watcher ingest the save (still lazy, so nothing runs).
    time.sleep(2.0)

    status, outputs = run(client, "# marimo: eager")
    assert status == "ok"
    assert "4243" in text_of(outputs)


def write_notebook2(notebook_dir, p_line: str) -> None:
    (notebook_dir / "notebook2.py").write_text(
        f"# %%\n{p_line}\n\n# %%\nq = p + 1\nprint('q =', q)\n\n# %%\nr = 10\n"
    )


def test_adopted_dependents_rerun_on_save(client, notebook_dir):
    # Only the `p` cell is ever executed; `q = p + 1` exists in the file but
    # is merely adopted when the file is first watched. Changing `p` on disk
    # must still rerun `q`, even though the runtime has never seen it.
    write_notebook2(notebook_dir, "p = 1")
    status, _ = run(client, "p = 1")
    assert status == "ok"
    wait_for_text(client, "watching notebook2.py")

    write_notebook2(notebook_dir, "p = 41")
    wait_for_text(client, "q = 42")


def test_execute_pulls_in_adopted_ancestors(client, notebook_dir):
    # `r = 10` was adopted from notebook2.py but never executed. A new cell
    # reading `r` must run that ancestor first instead of raising NameError.
    status, outputs = run(client, "s = r + 5\nprint('s =', s)")
    assert status == "ok"
    assert "s = 15" in text_of(outputs)
