from langchain_core.language_models.chat_models import BaseChatModel

from portus.core import ExecutionResult, VisualisationResult, Visualizer


class DumbVisualizer(Visualizer):
    def visualize(self, request: str, llm: BaseChatModel, data: ExecutionResult) -> VisualisationResult:
        plot = data.df.plot(kind="bar") if data.df is not None else None
        return VisualisationResult(text="", meta={}, plot=plot, code="")
