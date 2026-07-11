"""Install the marimo kernelspec so Zed (and Jupyter) can find it."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from jupyter_client.kernelspec import KernelSpecManager

REPO_ROOT = Path(__file__).resolve().parent.parent
GITHUB_SOURCE = "git+https://github.com/hermabr/marimo.zed"


def main() -> None:
    parser = argparse.ArgumentParser(description="Install the marimo Jupyter kernelspec")
    parser.add_argument("--name", default="marimo", help="kernelspec name (default: marimo)")
    parser.add_argument(
        "--display-name", default="marimo (reactive)", help="display name shown in kernel pickers"
    )
    parser.add_argument(
        "--uv", default="uv", help="uv executable to put in the kernelspec (default: uv)"
    )
    parser.add_argument(
        "--source",
        default=None,
        help="where the kernelspec gets marimo-zed from: a local directory "
        "(installed editable) or a uv requirement such as "
        f"{GITHUB_SOURCE} (default: this checkout when run from one, "
        "else the GitHub repo)",
    )
    args = parser.parse_args()

    if args.source is not None:
        source = args.source
    elif (REPO_ROOT / "pyproject.toml").is_file():
        # Running from a checkout (e.g. `uv run marimo-zed-install`).
        source = str(REPO_ROOT)
    else:
        # Running from an installed package (e.g. `uvx --from git+... marimo-zed-install`),
        # where REPO_ROOT is a site-packages directory that uv cannot install from.
        source = GITHUB_SOURCE
    if Path(source).is_dir():
        with_args = ["--with-editable", str(Path(source).resolve())]
    else:
        with_args = ["--with", source]

    # The kernel is launched with `uv run` from the directory Zed opened, so
    # both the kernel process and user code run in that project's environment,
    # with marimo-zed and its dependencies layered on top.
    spec = {
        "argv": [
            args.uv,
            "run",
            *with_args,
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
