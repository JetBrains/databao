from typing import Any

import altair as alt
import pandas as pd
import pytest
from PIL import Image

from databao.agent.visualizers.vega_chat import VegaChatResult
from databao.agent.visualizers.vega_vis_tool import VegaVisTool


def _make_result(**kwargs: Any) -> VegaChatResult:
    """Helper to construct a VegaChatResult with required base fields.

    Allows overriding/adding fields via kwargs.
    """
    base: dict[str, Any] = dict(text="", meta={}, plot=None, code=None, visualizer=None)
    base.update(kwargs)
    return VegaChatResult(**base)


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({"x": [1, 2, 3], "y": [3, 2, 1]})


@pytest.fixture()
def sample_spec() -> dict[str, Any]:
    # Minimal valid-ish Vega-Lite spec structure for testing
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "mark": "point",
        "encoding": {
            "x": {"field": "x", "type": "quantitative"},
            "y": {"field": "y", "type": "quantitative"},
        },
    }


def test_interactive_returns_none_when_missing_inputs() -> None:
    assert _make_result().interactive() is None
    assert _make_result(spec={}).interactive() is None
    assert _make_result(spec_df=pd.DataFrame()).interactive() is None


def test_interactive_returns_tool_when_present(sample_spec: dict[str, Any], sample_df: pd.DataFrame) -> None:
    result: VegaChatResult = _make_result(spec=sample_spec, spec_df=sample_df)
    tool = result.interactive()
    assert isinstance(tool, VegaVisTool)
    assert len(tool.get_html()) > 0


def test_altair_returns_none_when_missing_inputs() -> None:
    assert _make_result().altair() is None
    assert _make_result(spec={}).altair() is None
    assert _make_result(spec_df=pd.DataFrame()).altair() is None


def test_altair_returns_chart_when_present(sample_spec: dict[str, Any], sample_df: pd.DataFrame) -> None:
    result: VegaChatResult = _make_result(spec=sample_spec, spec_df=sample_df)
    chart = result.altair()
    assert isinstance(chart, alt.Chart)


def test_altair_returns_layered_chart(sample_df: pd.DataFrame) -> None:
    result: VegaChatResult = _make_result(
        spec={"layer": [{"mark": "point"}, {"mark": "line"}]},
        spec_df=sample_df,
    )
    chart = result.altair()
    # Altair represents layered charts via LayerChart
    assert isinstance(chart, alt.LayerChart)


def test_altair_returns_concat_chart(sample_df: pd.DataFrame) -> None:
    result: VegaChatResult = _make_result(
        spec={"hconcat": [{"mark": "bar"}, {"mark": "point"}]},
        spec_df=sample_df,
    )
    chart = result.altair()
    assert isinstance(chart, alt.HConcatChart)


def test_image_returns_none_when_missing_inputs() -> None:
    assert _make_result().image() is None
    assert _make_result(spec={}).image() is None
    assert _make_result(spec_df=pd.DataFrame()).image() is None


def test_image_returns_none_when_no_png_available(
    monkeypatch: pytest.MonkeyPatch, sample_spec: dict[str, Any], sample_df: pd.DataFrame
) -> None:
    import databao.agent.visualizers.vega_chat as vega_chat_mod

    # Force the PNG conversion helper to return None
    monkeypatch.setattr(vega_chat_mod, "vl_to_png_bytes", lambda spec, df: None)

    result: VegaChatResult = _make_result(spec=sample_spec, spec_df=sample_df)
    assert result.image() is None


def test_image_returns_pil_image_when_png_available(sample_spec: dict[str, Any], sample_df: pd.DataFrame) -> None:
    result: VegaChatResult = _make_result(spec=sample_spec, spec_df=sample_df)
    img = result.image()
    assert isinstance(img, Image.Image)
