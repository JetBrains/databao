import pytest

from databao.core.visualizer import VisualisationResult


def test_visualisation_result_get_plot_html_with_no_plot() -> None:
    result = VisualisationResult(text="Test", meta={}, plot=None, code=None)
    assert result._repr_mimebundle_() is None


def test_visualisation_result_get_plot_html_with_invalid_plot() -> None:
    class InvalidPlot:
        pass

    result = VisualisationResult(text="Test", meta={}, plot=InvalidPlot(), code=None)
    assert result._repr_mimebundle_() is None


def test_visualisation_result_altair() -> None:
    import altair as alt
    from vega_datasets import data  # type: ignore[import-untyped]

    alt.renderers.enable("html")  # Otherwise, _repr_mimebundle_ returns None as we are not in a notebook

    cars = data.cars()

    chart = (
        alt.Chart(cars)
        .mark_point()
        .encode(
            x="Horsepower",
            y="Miles_per_Gallon",
            color="Origin",
        )
        .interactive()
    )

    result = VisualisationResult(text="Test representation", meta={}, plot=chart, code=None)
    assert result._repr_mimebundle_() is not None


@pytest.mark.xfail(reason="matplotlib does not support _repr_*_ methods")
def test_visualisation_result_matplotlib() -> None:
    # Discussion in https://github.com/matplotlib/matplotlib/issues/16782
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()
    ax.plot([1, 2, 3, 4], [1, 4, 9, 16])
    ax.set_title("Simple plot")
    ax.set_ylabel("Y axis")
    ax.set_xlabel("X axis")
    result = VisualisationResult(text="Test representation", meta={}, plot=fig, code=None)
    assert result._repr_mimebundle_() is not None
