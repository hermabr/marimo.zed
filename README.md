# marimo.zed

> Pre-alpha MVP. Expect breaking changes.

A **reactive marimo kernel for Zed's built-in REPL**. Zed's REPL speaks the
Jupyter protocol, so this package implements a Jupyter kernel that routes every
execution through a real [marimo](https://marimo.io) runtime instead of a plain
Python interpreter.

## What you get

- Each code block you run becomes a cell in a marimo notebook graph.
- Cells are keyed by the variables they define: re-running `x = ...` replaces
  the previous cell that defined `x`.
- marimo then **reactively re-runs every dependent cell** and streams their new
  outputs back into Zed. Re-run `x = 10` and your earlier `y = x + 1` and `y`
  cells recompute automatically.
- stdout/stderr, rich HTML outputs (with a plain-text fallback), and marimo
  errors are translated to standard Jupyter messages.

## Architecture

```
Zed REPL ── Jupyter protocol (ZMQ) ──> marimo_zed kernel (ipykernel)
                                            │  compile cell, resolve defs,
                                            │  SyncGraphCommand / notifications
                                            ▼
                                       marimo._ipc.launch_kernel subprocess
                                       (the real marimo reactive runtime)
```

Everything runs through `uv run`, so code executes in the environment of the
project Zed opened — never in a fixed venv:

- The kernelspec launches the kernel with
  `uv run --with-editable <this repo> python -m marimo_zed`, layering
  marimo-zed and its dependencies on top of the project's environment.
- The kernel launches the marimo runtime with
  `uv run --with marimo==<kernel's version> --with pyzmq`, pinning marimo so
  the IPC wire format between kernel and runtime always matches.

The bridge to the marimo runtime subprocess follows the same pattern as
[marimo.nvim](https://github.com/hermabr/marimo.nvim).

## Install

From a checkout:

```sh
uv run marimo-zed-install
```

This writes a `marimo` kernelspec to `~/Library/Jupyter/kernels/marimo` that
references this checkout via `uv run --with-editable`, so there is nothing to
build or publish.

Without a checkout — e.g. on a remote machine you ssh into with Zed, where
kernels run remotely and need their own kernelspec:

```sh
uvx --from git+https://github.com/hermabr/marimo.zed marimo-zed-install
```

The kernelspec then references the GitHub repo via `uv run --with`, so the
kernel resolves the same source on any machine. `--source` overrides what
the kernelspec installs: a git/PyPI requirement, or a local directory
(installed editable). `uv` must be on the PATH Zed sees. The first launch in a new
project resolves the overlay environment and can take a few seconds;
subsequent launches use uv's cache.

## Use in Zed

1. Run `repl: refresh kernelspecs` from the command palette (or restart Zed).
2. Select the kernel for Python in your Zed `settings.json`:

   ```json
   {
     "jupyter": {
       "kernel_selections": {
         "python": "marimo"
       }
     }
   }
   ```

3. Open a Python file and run code with the REPL: `ctrl-shift-enter`
   (`repl: run`) runs the current cell — use `# %%` markers to split a file
   into cells — or the current selection.

Re-run a cell after editing it and watch downstream cells recompute.

## Semantics and limitations (MVP)

- **Cell identity is by definitions.** Code that defines `x` replaces the
  previous cell defining `x`. If one execution redefines names owned by
  several cells, the first is replaced and the rest are deleted from the
  graph.
- **Definition-free code persists.** An expression like `y * 2` or a
  `print(x)` becomes a reactive cell too — it re-runs whenever its inputs
  change. Running identical definition-free code re-uses the existing cell.
- Outputs of reactively re-run cells appear under whichever execution
  triggered them.
- `input()` / stdin is not supported yet.
- The kernel and runtime resolve their environment from the directory Zed
  launches the kernel in (the worktree root). Scratch files outside a uv
  project get an ephemeral env with just marimo-zed's dependencies.
- Interrupt uses SIGINT (`interrupt_mode: signal`); the marimo subprocess
  shares the kernel's process group and is also signalled directly.
- The marimo runtime starts lazily on the first execution and restarts
  automatically if it dies.

## Test

```sh
uv run pytest
```

The tests drive the kernel end-to-end over the Jupyter protocol with
`jupyter_client`, including the reactive-rerun behavior.
