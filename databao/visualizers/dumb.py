from databao.core import ExecutionResult, VisualisationResult, Visualizer


class DumbVisualizer(Visualizer):
    def visualize(self, request: str | None, data: ExecutionResult, *, stream: bool = False) -> VisualisationResult:
        plot = data.df.plot(kind="bar") if data.df is not None else None
        return VisualisationResult(text="", meta={}, plot=plot, code="", visualizer=self)

    def edit(self, request: str, visualization: VisualisationResult, *, stream: bool = False) -> VisualisationResult:
        return visualization
