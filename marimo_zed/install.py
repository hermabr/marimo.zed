"""Install the marimo kernelspec so Zed (and Jupyter) can find it."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from jupyter_client.kernelspec import KernelSpecManager

REPO_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    parser = argparse.ArgumentParser(description="Install the marimo Jupyter kernelspec")
    parser.add_argument("--name", default="marimo", help="kernelspec name (default: marimo)")
    parser.add_argument(
        "--display-name", default="marimo (reactive)", help="display name shown in kernel pickers"
    )
    parser.add_argument(
        "--uv", default="uv", help="uv executable to put in the kernelspec (default: uv)"
    )
    args = parser.parse_args()

    # The kernel is launched with `uv run` from the directory Zed opened, so
    # both the kernel process and user code run in that project's environment,
    # with marimo-zed and its dependencies layered on top.
    spec = {
        "argv": [
            args.uv,
            "run",
            "--with-editable",
            str(REPO_ROOT),
            "python",
            "-m",
            "marimo_zed",
            "-f",
            "{connection_file}",
        ],
        "display_name": args.display_name,
        "language": "python",
        "interrupt_mode": "signal",
        "metadata": {"debugger": False},
    }

    with tempfile.TemporaryDirectory() as td:
        (Path(td) / "kernel.json").write_text(json.dumps(spec, indent=2))
        dest = KernelSpecManager().install_kernel_spec(td, kernel_name=args.name, user=True)
    print(f"Installed kernelspec '{args.name}' to {dest}")


if __name__ == "__main__":
    main()
