from portus.agent.base_agent import ExecutionResult
from portus.visualizer.visualizer import VisualisationResult, Visualizer


class DumbVisualizer(Visualizer):
    def visualize(self, request: str, data: ExecutionResult) -> VisualisationResult:
        plot = data.df.plot(kind="bar") if data.df is not None else None
        return VisualisationResult(text="", meta={}, plot=plot, code="")
