"""Tests for MultimodalWidget in databao/multimodal/jupyter_widget.py."""

import json
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from databao.agent.multimodal.jupyter_widget import MultimodalWidget
from databao.agent.visualizers.vega_chat import VegaChatResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_thread(
    *,
    text: str = "result text",
    df: pd.DataFrame | None = None,
    plot: Any = None,
) -> MagicMock:
    """Build a minimal mock Thread."""
    thread = MagicMock()
    thread.text.return_value = text
    thread.df.return_value = df
    thread.plot.return_value = plot
    return thread


def _make_vega_result(*, spec: dict[str, Any] | None = None, spec_df: pd.DataFrame | None = None) -> VegaChatResult:
    return VegaChatResult(text="", meta={}, plot=None, code=None, visualizer=None, spec=spec, spec_df=spec_df)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({"a": range(5), "b": range(5, 10)})


@pytest.fixture()
def sample_spec() -> dict[str, Any]:
    return {"mark": "point", "encoding": {"x": {"field": "a"}, "y": {"field": "b"}}}


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


def test_init_sets_text(sample_df: pd.DataFrame) -> None:
    thread = _make_thread(text="hello", df=sample_df)
    widget = MultimodalWidget(thread=thread)

    assert widget.text == "hello"
    assert widget.text_status == "loaded"


def test_init_sets_dataframe_html_when_df_present(sample_df: pd.DataFrame) -> None:
    thread = _make_thread(df=sample_df)
    widget = MultimodalWidget(thread=thread)

    assert widget.dataframe_html_content != ""
    assert widget.dataframe_html_content_status == "loaded"


def test_init_leaves_dataframe_status_initial_when_no_df() -> None:
    thread = _make_thread(df=None)
    widget = MultimodalWidget(thread=thread)

    assert widget.dataframe_html_content == ""
    assert widget.dataframe_html_content_status == "initial"


def test_init_spec_status_is_initial(sample_df: pd.DataFrame) -> None:
    thread = _make_thread(df=sample_df)
    widget = MultimodalWidget(thread=thread)

    assert widget.spec is None
    assert widget.spec_status == "initial"


# ---------------------------------------------------------------------------
# SELECT_MODALITY: CHART tab
# ---------------------------------------------------------------------------


def test_chart_tab_loads_spec(sample_df: pd.DataFrame, sample_spec: dict[str, Any]) -> None:
    plot = _make_vega_result(spec=sample_spec, spec_df=sample_df)
    thread = _make_thread(df=sample_df, plot=plot)
    widget = MultimodalWidget(thread=thread)

    msg = {"action": {"type": "SELECT_MODALITY", "payload": json.dumps("CHART")}}
    widget._handle_client_message(msg, [])

    assert widget.spec_status == "loaded"
    assert widget.spec is not None


def test_chart_tab_idempotent_when_already_loaded(sample_df: pd.DataFrame, sample_spec: dict[str, Any]) -> None:
    plot = _make_vega_result(spec=sample_spec, spec_df=sample_df)
    thread = _make_thread(df=sample_df, plot=plot)
    widget = MultimodalWidget(thread=thread)

    msg = {"action": {"type": "SELECT_MODALITY", "payload": json.dumps("CHART")}}
    widget._handle_client_message(msg, [])
    widget._handle_client_message(msg, [])

    thread.plot.assert_called_once()


def test_chart_tab_sets_failed_status_when_plot_not_vega(sample_df: pd.DataFrame) -> None:
    thread = _make_thread(df=sample_df, plot=MagicMock(spec=[]))  # not a VegaChatResult
    widget = MultimodalWidget(thread=thread)

    msg = {"action": {"type": "SELECT_MODALITY", "payload": json.dumps("CHART")}}
    widget._handle_client_message(msg, [])

    assert widget.spec_status == "failed"


def test_chart_tab_sets_failed_status_when_spec_is_none(sample_df: pd.DataFrame) -> None:
    plot = _make_vega_result(spec=None, spec_df=sample_df)
    thread = _make_thread(df=sample_df, plot=plot)
    widget = MultimodalWidget(thread=thread)

    msg = {"action": {"type": "SELECT_MODALITY", "payload": json.dumps("CHART")}}
    widget._handle_client_message(msg, [])

    assert widget.spec_status == "failed"


# ---------------------------------------------------------------------------
# SELECT_MODALITY: DATAFRAME tab
# ---------------------------------------------------------------------------


def test_dataframe_tab_loads_html_when_df_available(sample_df: pd.DataFrame) -> None:
    thread = _make_thread(df=None)  # no df at init
    widget = MultimodalWidget(thread=thread)
    assert widget.dataframe_html_content_status == "initial"

    thread.df.return_value = sample_df
    msg = {"action": {"type": "SELECT_MODALITY", "payload": json.dumps("DATAFRAME")}}
    widget._handle_client_message(msg, [])

    assert widget.dataframe_html_content_status == "loaded"
    assert widget.dataframe_html_content != ""


def test_dataframe_tab_sets_failed_when_df_is_none() -> None:
    thread = _make_thread(df=None)
    widget = MultimodalWidget(thread=thread)

    msg = {"action": {"type": "SELECT_MODALITY", "payload": json.dumps("DATAFRAME")}}
    widget._handle_client_message(msg, [])

    assert widget.dataframe_html_content_status == "failed"


# ---------------------------------------------------------------------------
# SELECT_MODALITY: DESCRIPTION tab
# ---------------------------------------------------------------------------


def test_description_tab_loads_text() -> None:
    thread = _make_thread(text="initial text")
    widget = MultimodalWidget(thread=thread)
    # Reset to initial to simulate lazy loading scenario
    widget.text_status = "initial"
    widget.text = ""

    thread.text.return_value = "loaded text"
    msg = {"action": {"type": "SELECT_MODALITY", "payload": json.dumps("DESCRIPTION")}}
    widget._handle_client_message(msg, [])

    assert widget.text_status == "loaded"
    assert widget.text == "loaded text"


def test_description_tab_idempotent_when_already_loaded() -> None:
    thread = _make_thread(text="some text")
    widget = MultimodalWidget(thread=thread)
    assert widget.text_status == "loaded"

    call_count_before = thread.text.call_count
    msg = {"action": {"type": "SELECT_MODALITY", "payload": json.dumps("DESCRIPTION")}}
    widget._handle_client_message(msg, [])

    assert thread.text.call_count == call_count_before


# ---------------------------------------------------------------------------
# Message handling edge cases
# ---------------------------------------------------------------------------


def test_unknown_action_type_is_ignored() -> None:
    thread = _make_thread()
    widget = MultimodalWidget(thread=thread)

    msg = {"action": {"type": "UNKNOWN_ACTION", "payload": None}}
    # Should not raise
    widget._handle_client_message(msg, [])


def test_missing_action_key_is_ignored() -> None:
    thread = _make_thread()
    widget = MultimodalWidget(thread=thread)

    widget._handle_client_message({}, [])
    widget._handle_client_message({"action": {}}, [])


def test_malformed_json_payload_is_ignored(sample_df: pd.DataFrame) -> None:
    thread = _make_thread(df=sample_df)
    widget = MultimodalWidget(thread=thread)

    msg = {"action": {"type": "SELECT_MODALITY", "payload": "not valid json {"}}
    # Should not raise — exception is caught and logged
    widget._handle_client_message(msg, [])
