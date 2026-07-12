# marimo.zed

> Pre-alpha MVP. Expect breaking changes. FULLY VIBE-CODED

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
- The kernel watches the notebook file on disk: **saving the file re-runs the
  cells you changed** (and their dependents). An optional lazy mode queues
  saved changes and runs them with your next execution instead.

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

Without a checkout:

```sh
uvx --from git+https://github.com/hermabr/marimo.zed marimo-zed-install
```

The kernelspec then references the GitHub repo via `uv run --with`, so the
kernel resolves the same source on any machine. `--source` overrides what
the kernelspec installs: a git/PyPI requirement, or a local directory
(installed editable). `uv` must be on the PATH Zed sees. The first launch in a new
project resolves the overlay environment and can take a few seconds;
subsequent launches use uv's cache.

### Remote (SSH) projects

Zed never reads Jupyter kernelspecs in SSH remote projects: whichever Python
toolchain you pick in the kernel picker, its headless server runs
`<python> -m ipykernel_launcher -f <connection_file>` from the project root.
So instead of a kernelspec, install a **shim** into the project on the remote
machine:

```sh
cd /path/to/project   # on the remote machine
uvx --from git+https://github.com/hermabr/marimo.zed marimo-zed-install --ssh-shim
```

This writes `ipykernel_launcher.py` into the project root. Python resolves
modules from the working directory before site-packages, so the shim shadows
ipykernel's real launcher and redirects the launch into marimo-zed. In Zed,
just pick any Python toolchain for the project in the REPL kernel picker —
the shim hijacks it.

The generated shim also works around a race in Zed's SSH kernel startup. Zed
can try to open IOPub before `uv` and marimo have finished starting, so the
shim immediately binds the five ports in Zed's connection file, launches the
real kernel on alternate loopback ports, and transparently proxies the TCP
streams once it is ready. The proxy requires no extra dependencies and exits
with the kernel.

Notes:

- The hijack is the default and applies to **every** `python -m
  ipykernel_launcher` started from the project root (that's the point — the
  toolchain choice in Zed's picker doesn't otherwise matter). Set
  `MARIMO_ZED_DISABLE=1` in the environment to fall through to the real
  ipykernel.
- The shim bakes in the absolute path of `uv` found at install time
  (override with `--uv`), since Zed's remote server may run with a minimal
  PATH.
- `--execution` and `--source` work the same as for the kernelspec.
- Add `ipykernel_launcher.py` to the project's `.gitignore` (or commit it, if
  everyone on the project wants the marimo REPL).
- `PYTHONSAFEPATH=1` (or python's `-P`) disables working-directory imports
  and therefore the shim.

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

In SSH remote projects, steps 1–2 don't apply (Zed ignores kernelspecs and
`kernel_selections` there): install the shim as described above and pick any
Python toolchain in the kernel picker instead.

Re-run a cell after editing it and watch downstream cells recompute.

## Automatic re-runs on save

Zed gives the kernel no editor events, so where
[marimo.nvim](https://github.com/hermabr/marimo.nvim) re-runs cells when you
leave insert mode, marimo.zed re-runs them when the notebook file is saved:

- After you run a cell, the kernel locates the file it came from (it scans the
  project for a matching `# %%` cell) and starts polling that file. It prints
  `marimo: watching <file>` when it does. To point it at a file explicitly,
  run a cell containing `# marimo: watch path/to/file.py`.
- **Eager mode (default):** saving the file re-runs every cell whose code
  changed, plus new cells, and marimo reactively recomputes their dependents.
  Existing cells' outputs are replaced in their own output areas.
- Cells found in the watched file that you never executed are adopted into the
  graph without running. They still participate in reactivity: they run once a
  cell they depend on runs, and executing code that reads their variables runs
  them first (instead of raising `NameError`).
- **Lazy mode:** saved changes are queued instead of run. The next execution
  runs any queued cells the executed cell depends on (their dependents
  recompute reactively); the rest stay queued. Cells deleted from the file are
  removed from the graph in both modes.

Toggle at runtime by running a cell containing `# marimo: lazy` or
`# marimo: eager` (switching to eager runs everything queued). Set the
default with `marimo-zed-install --execution lazy`, which writes
`MARIMO_ZED_EXECUTION` into the kernelspec.

For the closest "re-run when I leave insert mode" feel, enable autosave in
Zed (e.g. `"autosave": {"after_delay": {"milliseconds": 500}}`) — while a
cell's code is syntactically invalid mid-edit, the kernel skips it and picks
it up on the next save that parses.

## Semantics and limitations (MVP)

- **Cell identity is by definitions.** Code that defines `x` replaces the
  previous cell defining `x`. If one execution redefines names owned by
  several cells, the first is replaced and the rest are deleted from the
  graph.
- **Definition-free code persists.** An expression like `y * 2` or a
  `print(x)` becomes a reactive cell too — it re-runs whenever its inputs
  change. Running identical definition-free code re-uses the existing cell.
- Outputs of reactively re-run cells replace the prior outputs for those cells.
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
