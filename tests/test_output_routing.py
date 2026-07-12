"""Unit tests for routing cell outputs to the right execute_request area."""

from __future__ import annotations

from jupyter_client.session import Session
from marimo._messaging.cell_output import CellChannel, CellOutput
from marimo._messaging.notification import CellNotification

from marimo_zed.kernel import MarimoZedKernel


def make_kernel() -> tuple[MarimoZedKernel, list[tuple]]:
    """A kernel with stubbed Jupyter plumbing that records sent messages."""
    kernel = MarimoZedKernel.__new__(MarimoZedKernel)
    kernel._cells = {}
    kernel._cell_parents = {}
    kernel._oob_parent = None
    kernel.iopub_socket = object()
    sent: list[tuple] = []
    kernel.session = Session(key=b"test-key")
    kernel.session.send = lambda sock, msg_type, content, parent=None: sent.append(
        (msg_type, content, parent)
    )
    kernel.send_response = lambda sock, msg_type, content: sent.append(
        (msg_type, content, "current-request")
    )
    return kernel, sent


def parent(msg_id: str) -> dict:
    return {"header": {"msg_id": msg_id}}


def output_note(cell_id: str, value: str) -> CellNotification:
    return CellNotification(
        cell_id=cell_id,
        output=CellOutput(
            channel=CellChannel.OUTPUT, mimetype="text/plain", data=value
        ),
    )


def test_output_goes_to_the_cells_own_area():
    kernel, sent = make_kernel()
    kernel._cell_parents = {"cell-1": parent("req-1")}
    kernel._emit_cell_notification(output_note("cell-1", "123"), silent=False)
    assert sent == [
        ("display_data", {"data": {"text/plain": "123"}, "metadata": {}}, parent("req-1"))
    ]


def test_parentless_output_is_dropped_not_misattributed():
    # A cell never executed from the editor has no output area; its value
    # must not render under whatever request happened to run last.
    kernel, sent = make_kernel()
    kernel._oob_parent = parent("req-latest")
    kernel._emit_cell_notification(output_note("adopted-cell", "246"), silent=False)
    assert sent == []


def test_parentless_console_and_errors_still_shown():
    kernel, sent = make_kernel()
    note = CellNotification(
        cell_id="adopted-cell",
        console=CellOutput(
            channel=CellChannel.STDOUT, mimetype="text/plain", data="q = 42\n"
        ),
        output=CellOutput(
            channel=CellChannel.MARIMO_ERROR,
            mimetype="application/vnd.marimo+error",
            data=["boom"],
        ),
    )
    kernel._emit_cell_notification(note, silent=False)
    assert [(msg_type, target) for msg_type, _, target in sent] == [
        ("stream", "current-request"),
        ("error", "current-request"),
    ]


def test_transfer_parents_follows_identity_change():
    # Definition-free code matches only on identical source, so an edit
    # allocates a new cell id; the output area must follow it.
    kernel, _ = make_kernel()
    kernel._cells = {"id-m": "m = 7\nm", "id-new": "m * 4"}
    kernel._cell_parents = {"id-old": parent("req-expr")}
    # The replaced cell is still registered under its old source.
    kernel._cells["id-old"] = "m * 3"
    kernel._transfer_parents(
        ["m = 7\nm", "m * 3"],
        ["m = 7\nm", "m * 4"],
        {"m * 4": "id-new"},
    )
    assert kernel._cell_parents == {"id-new": parent("req-expr")}


def test_transfer_parents_keeps_existing_parent():
    # A cell that kept its id (definition overlap) keeps its own area.
    kernel, _ = make_kernel()
    kernel._cells = {"id-m": "m = 8\nm"}
    kernel._cell_parents = {"id-m": parent("req-m")}
    kernel._transfer_parents(
        ["m = 7\nm"], ["m = 8\nm"], {"m = 8\nm": "id-m"}
    )
    assert kernel._cell_parents == {"id-m": parent("req-m")}
