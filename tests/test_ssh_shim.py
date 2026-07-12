"""Tests for the Zed SSH shim: `python -m ipykernel_launcher` run from a
project root containing the shim must launch marimo-zed, not ipykernel."""

from __future__ import annotations

import json
import os
import socket
import stat
import subprocess
import sys
import time

import pytest
from jupyter_client import BlockingKernelClient

from marimo_zed.install import REPO_ROOT, SHIM_MARKER, main

TIMEOUT = 30
CHANNELS = ("shell", "iopub", "stdin", "control", "hb")


def install_shim(project_dir, monkeypatch, *extra_args):
    monkeypatch.setattr(
        sys, "argv", ["marimo-zed-install", "--ssh-shim", str(project_dir), *extra_args]
    )
    main()
    return project_dir / "ipykernel_launcher.py"


def write_connection_file(path):
    sockets = []
    try:
        for _ in CHANNELS:
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            sockets.append(listener)
        ports = [listener.getsockname()[1] for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()
    info = {
        **{f"{channel}_port": port for channel, port in zip(CHANNELS, ports)},
        "ip": "127.0.0.1",
        "key": "test-key",
        "transport": "tcp",
        "signature_scheme": "hmac-sha256",
        "kernel_name": "test",
    }
    path.write_text(json.dumps(info))
    return info


@pytest.fixture
def fake_uv(tmp_path):
    """A uv stand-in that dumps its argv and environment as JSON."""
    path = tmp_path / "bin" / "uv"
    path.parent.mkdir()
    path.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "connection_file = sys.argv[-1]\n"
        "print(json.dumps({\n"
        "    'argv': sys.argv[1:],\n"
        "    'execution': os.environ.get('MARIMO_ZED_EXECUTION'),\n"
        "    'uv': os.environ.get('MARIMO_ZED_UV'),\n"
        "    'connection_file': connection_file,\n"
        "    'connection': json.load(open(connection_file)),\n"
        "}))\n"
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def test_install_writes_marked_shim(tmp_path, monkeypatch):
    shim = install_shim(tmp_path, monkeypatch)
    text = shim.read_text()
    assert text.startswith(SHIM_MARKER + "\n")
    # Running from a checkout, the shim references this repo editable.
    assert str(REPO_ROOT) in text


def test_shim_hijacks_ipykernel_launcher(tmp_path, monkeypatch, fake_uv):
    project = tmp_path / "project"
    project.mkdir()
    install_shim(project, monkeypatch, "--uv", str(fake_uv))
    connection_file = project / "conn.json"
    original_connection = write_connection_file(connection_file)

    # Exactly what Zed's headless server runs on the remote host.
    out = subprocess.run(
        [sys.executable, "-m", "ipykernel_launcher", "-f", str(connection_file)],
        cwd=project,
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
    )
    assert out.returncode == 0, out.stderr
    report = json.loads(out.stdout)
    assert report["argv"][:-1] == [
        "run",
        "--with-editable",
        str(REPO_ROOT),
        "python",
        "-m",
        "marimo_zed",
        "-f",
    ]
    assert report["connection_file"] != str(connection_file)
    assert not os.path.exists(report["connection_file"])
    assert report["connection"]["key"] == original_connection["key"]
    for channel in CHANNELS:
        assert (
            report["connection"][f"{channel}_port"]
            != original_connection[f"{channel}_port"]
        )
    assert report["execution"] == "eager"
    assert report["uv"] == str(fake_uv)


def test_shim_respects_execution_flag(tmp_path, monkeypatch, fake_uv):
    install_shim(tmp_path, monkeypatch, "--uv", str(fake_uv), "--execution", "lazy")
    connection_file = tmp_path / "conn.json"
    write_connection_file(connection_file)
    out = subprocess.run(
        [sys.executable, "-m", "ipykernel_launcher", "-f", str(connection_file)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
    )
    assert out.returncode == 0, out.stderr
    assert json.loads(out.stdout)["execution"] == "lazy"


def test_shim_holds_connection_until_delayed_backend_starts(tmp_path, monkeypatch):
    marker = tmp_path / "backend-ready"
    fake_uv = tmp_path / "bin" / "uv"
    fake_uv.parent.mkdir()
    fake_uv.write_text(
        f"#!{sys.executable}\n"
        "import json, socket, sys, time\n"
        f"marker = {str(marker)!r}\n"
        "connection = json.load(open(sys.argv[-1]))\n"
        "time.sleep(1)\n"
        "open(marker, 'w').close()\n"
        "listener = socket.socket()\n"
        "listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
        "listener.bind((connection['ip'], connection['iopub_port']))\n"
        "listener.listen()\n"
        "while True:\n"
        "    stream, _ = listener.accept()\n"
        "    stream.settimeout(2)\n"
        "    try:\n"
        "        data = stream.recv(1024)\n"
        "    except (OSError, TimeoutError):\n"
        "        data = b''\n"
        "    if data:\n"
        "        stream.sendall(b'backend:' + data)\n"
        "        time.sleep(0.2)\n"
        "        break\n"
        "    stream.close()\n"
        "listener.close()\n"
    )
    fake_uv.chmod(fake_uv.stat().st_mode | stat.S_IXUSR)

    project = tmp_path / "project"
    project.mkdir()
    install_shim(project, monkeypatch, "--uv", str(fake_uv))
    connection_file = project / "conn.json"
    connection = write_connection_file(connection_file)

    started = time.monotonic()
    process = subprocess.Popen(
        [sys.executable, "-m", "ipykernel_launcher", "-f", str(connection_file)],
        cwd=project,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 3
        while True:
            try:
                client = socket.create_connection(
                    (connection["ip"], connection["iopub_port"]), timeout=0.05
                )
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)

        # The shim accepted this connection before the delayed backend bound.
        assert not marker.exists()
        client.settimeout(5)
        client.sendall(b"hello")
        assert client.recv(1024) == b"backend:hello"
        client.close()
        assert time.monotonic() - started >= 0.8

        _, stderr = process.communicate(timeout=TIMEOUT)
        assert process.returncode == 0, stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_shim_proxies_real_jupyter_kernel(tmp_path, monkeypatch):
    direct_uv = tmp_path / "bin" / "uv"
    direct_uv.parent.mkdir()
    direct_uv.write_text(
        f"#!{sys.executable}\n"
        "import os, sys\n"
        "python_arg = sys.argv.index('python')\n"
        "kernel_args = sys.argv[python_arg + 1:]\n"
        "os.execv(sys.executable, [sys.executable, *kernel_args])\n"
    )
    direct_uv.chmod(direct_uv.stat().st_mode | stat.S_IXUSR)

    project = tmp_path / "project"
    project.mkdir()
    install_shim(project, monkeypatch, "--uv", str(direct_uv))
    connection_file = project / "conn.json"
    write_connection_file(connection_file)

    process = subprocess.Popen(
        [sys.executable, "-m", "ipykernel_launcher", "-f", str(connection_file)],
        cwd=project,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    client = BlockingKernelClient(connection_file=str(connection_file))
    try:
        client.load_connection_file()
        client.start_channels()
        client.wait_for_ready(timeout=15)
        client.kernel_info()
        reply = client.get_shell_msg(timeout=5)
        assert reply["content"]["status"] == "ok"
        assert reply["content"]["implementation"] == "marimo_zed"
        client.shutdown()
        _, stderr = process.communicate(timeout=TIMEOUT)
        assert process.returncode == 0, stderr
    finally:
        client.stop_channels()
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=TIMEOUT)


def test_disable_env_falls_through_to_real_launcher(tmp_path, monkeypatch, fake_uv):
    """MARIMO_ZED_DISABLE strips the shim off sys.path and runs the real
    module (simulated here by a launcher further down sys.path)."""
    project = tmp_path / "project"
    project.mkdir()
    install_shim(project, monkeypatch, "--uv", str(fake_uv))

    site = tmp_path / "site"
    site.mkdir()
    (site / "ipykernel_launcher.py").write_text("print('real launcher ran')\n")

    env = os.environ | {"MARIMO_ZED_DISABLE": "1", "PYTHONPATH": str(site)}
    out = subprocess.run(
        [sys.executable, "-m", "ipykernel_launcher", "-f", "conn.json"],
        cwd=project,
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
        env=env,
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "real launcher ran"


def test_refuses_to_overwrite_foreign_file(tmp_path, monkeypatch):
    launcher = tmp_path / "ipykernel_launcher.py"
    launcher.write_text("# someone else's file\n")
    with pytest.raises(SystemExit, match="not generated by this tool"):
        install_shim(tmp_path, monkeypatch)
    assert launcher.read_text() == "# someone else's file\n"

    install_shim(tmp_path, monkeypatch, "--force")
    assert launcher.read_text().startswith(SHIM_MARKER)

    # Re-installing over our own shim needs no --force.
    install_shim(tmp_path, monkeypatch, "--execution", "lazy")
    assert "lazy" in launcher.read_text()


def test_rejects_missing_directory(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="is not a directory"):
        install_shim(tmp_path / "missing", monkeypatch)
