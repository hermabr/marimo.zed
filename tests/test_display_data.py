"""Unit tests for translating marimo cell outputs to Jupyter display data."""

from __future__ import annotations

import json

from marimo._messaging.cell_output import CellChannel, CellOutput

from marimo_zed.kernel import MarimoZedKernel


def display_data(mimetype: str, data: object) -> dict | None:
    kernel = MarimoZedKernel.__new__(MarimoZedKernel)
    return kernel._display_data(
        CellOutput(channel=CellChannel.OUTPUT, mimetype=mimetype, data=data)
    )


def test_mimebundle_json_string_is_unwrapped():
    # marimo serializes the bundle as a JSON string; the base64 image inside
    # is a data URI, which Jupyter clients don't accept for image mimetypes.
    bundle = json.dumps(
        {
            "image/png": "data:image/png;base64,iVBORw0KGgo=",
            "text/html": "<img src='...'/>",
        }
    )
    data = display_data("application/vnd.marimo+mimebundle", bundle)
    assert data == {
        "image/png": "iVBORw0KGgo=",
        "text/html": "<img src='...'/>",
    }


def test_mimebundle_dict_is_unwrapped():
    data = display_data(
        "application/vnd.marimo+mimebundle",
        {"image/png": "data:image/png;base64,abc123"},
    )
    assert data == {"image/png": "abc123"}


def test_mimebundle_invalid_json_falls_back_to_text():
    data = display_data("application/vnd.marimo+mimebundle", "not json")
    assert data == {"text/plain": "not json"}


def test_image_data_uri_is_stripped():
    data = display_data("image/png", "data:image/png;base64,abc123")
    assert data == {"image/png": "abc123"}


def test_plain_base64_image_is_untouched():
    data = display_data("image/png", "iVBORw0KGgo=")
    assert data == {"image/png": "iVBORw0KGgo="}


def test_html_gets_plain_text_fallback():
    data = display_data("text/html", "<b>hi</b>")
    assert data == {"text/html": "<b>hi</b>", "text/plain": "hi"}
