"""End-to-end smoke test: drive the kernel over the Jupyter protocol."""

from __future__ import annotations

import subprocess
import sys

import pytest
from jupyter_client import BlockingKernelClient
from jupyter_client.connect import write_connection_file

TIMEOUT = 60


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    connection_file = str(tmp_path_factory.mktemp("conn") / "kernel.json")
    write_connection_file(fname=connection_file, ip="127.0.0.1", key=b"test-key")
    proc = subprocess.Popen([sys.executable, "-m", "marimo_zed", "-f", connection_file])
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


def run(client: BlockingKernelClient, code: str) -> tuple[str, list[dict]]:
    """Execute code, return (reply_status, iopub messages until idle)."""
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
        if msg["parent_header"].get("msg_id") != msg_id:
            continue
        if msg["msg_type"] == "status":
            idle = msg["content"]["execution_state"] == "idle"
        elif msg["msg_type"] in ("stream", "display_data", "execute_result", "error"):
            outputs.append(msg)
    return status, outputs


def text_of(outputs: list[dict]) -> str:
    chunks = []
    for msg in outputs:
        content = msg["content"]
        if msg["msg_type"] == "stream":
            chunks.append(content["text"])
        elif msg["msg_type"] in ("display_data", "execute_result"):
            data = content.get("data", {})
            chunks.append(str(data.get("text/plain", "")))
            chunks.append(str(data.get("text/html", "")))
        elif msg["msg_type"] == "error":
            chunks.append(content.get("evalue", ""))
    return "\n".join(chunks)


def test_define_and_use(client):
    status, _ = run(client, "x = 1")
    assert status == "ok"

    status, _ = run(client, "y = x + 1")
    assert status == "ok"

    status, outputs = run(client, "y")
    assert status == "ok"
    assert "2" in text_of(outputs)


def test_reactive_rerun(client):
    # Redefining x replaces the original cell and reactively re-runs the
    # dependent `y = x + 1` cell and the `y` expression cell.
    status, outputs = run(client, "x = 10")
    assert status == "ok"
    assert "11" in text_of(outputs)


def test_stdout_stream(client):
    status, outputs = run(client, 'print("hello from marimo")')
    assert status == "ok"
    assert "hello from marimo" in text_of(outputs)


def test_runtime_error(client):
    status, outputs = run(client, "1 / 0")
    assert status == "error"
    assert "ZeroDivisionError" in text_of(outputs) or "division" in text_of(outputs)


def test_syntax_error(client):
    status, _ = run(client, "def broken(:")
    assert status == "error"


def test_multiple_definition_replaced(client):
    # `x` and a fresh `z` in one cell: the old x-cell is replaced.
    status, outputs = run(client, "x = 2\nz = x * 3")
    assert status == "ok"
    assert "3" in text_of(outputs)  # y = x + 1 reruns to 3

    status, outputs = run(client, "z")
    assert status == "ok"
    assert "6" in text_of(outputs)
