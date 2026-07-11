"""Bridge to a marimo kernel subprocess over marimo's IPC queues."""

from __future__ import annotations

import os
import signal
import subprocess
from collections import deque

from marimo import __version__ as marimo_version
from marimo._ast.app import App, InternalApp
from marimo._config.manager import get_default_config_manager
from marimo._config.settings import GLOBAL_SETTINGS
from marimo._ipc.queue_manager import QueueManager
from marimo._ipc.types import KernelArgs
from marimo._runtime.commands import AppMetadata


class KernelLaunchError(RuntimeError):
    pass


class MarimoBridge:
    """Launches ``marimo._ipc.launch_kernel`` and exposes its queues.

    The subprocess is launched with ``uv run`` from the current working
    directory, so user code executes in that project's environment. marimo is
    pinned to this process's version to keep the IPC wire format compatible.
    """

    def __init__(self) -> None:
        app = InternalApp(App())
        self.queue_manager, connection_info = QueueManager.create()
        self.stderr_lines: deque[str] = deque(maxlen=200)
        self.process: subprocess.Popen[bytes] | None = None

        config_manager = get_default_config_manager(current_path=None)
        self.kernel_args = KernelArgs(
            configs=app.cell_manager.config_map(),
            app_metadata=AppMetadata(
                query_params={},
                cli_args={},
                app_config=app.config,
                argv=[],
                filename=None,
            ),
            user_config=config_manager.get_config(hide_secrets=False),
            log_level=GLOBAL_SETTINGS.LOG_LEVEL,
            profile_path=None,
            connection_info=connection_info,
            is_run_mode=False,
            virtual_files_supported=False,
            redirect_console_to_browser=True,
        )

    def launch(self) -> None:
        cmd = [
            os.environ.get("MARIMO_ZED_UV", "uv"),
            "run",
            "--with",
            f"marimo=={marimo_version}",
            "--with",
            "pyzmq",
            "python",
            "-m",
            "marimo._ipc.launch_kernel",
        ]
        self.process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=os.environ.copy(),
            cwd=os.getcwd(),
        )

        assert self.process.stdin is not None
        self.process.stdin.write(self.kernel_args.encode_json())
        self.process.stdin.flush()
        self.process.stdin.close()

        assert self.process.stdout is not None
        ready = self.process.stdout.readline().decode("utf-8", errors="replace").strip()
        if ready != "KERNEL_READY":
            stderr = self.read_stderr()
            raise KernelLaunchError(
                f"marimo kernel failed to start.\n\nCommand: {' '.join(cmd)}\n\nStderr:\n{stderr}"
            )

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def read_stderr(self) -> str:
        if self.process is None or self.process.stderr is None:
            return ""
        data = self.process.stderr.read().decode("utf-8", errors="replace")
        if data:
            for line in data.splitlines():
                self.stderr_lines.append(line)
        return data

    def interrupt(self) -> None:
        if self.process is None or self.process.pid is None:
            return
        if os.name == "nt" and self.queue_manager.win32_interrupt_queue is not None:
            self.queue_manager.win32_interrupt_queue.put_nowait(True)
            return
        os.kill(self.process.pid, signal.SIGINT)

    def close(self) -> None:
        if self.process is not None:
            from marimo._runtime import commands

            try:
                self.queue_manager.control_queue.put(commands.StopKernelCommand())
            except Exception:
                pass
            self.queue_manager.close_queues()
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
