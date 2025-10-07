from typing import Any, Self

from langchain_core.messages import BaseMessage, HumanMessage
from pandas import DataFrame

from portus.agent.base_agent import BaseAgent, ExecutionResult
from portus.pipe.base_pipe import BasePipe
from portus.vizualizer import VisualisationResult, Visualizer


class Pipe(BasePipe):
    def __init__(self, agent: BaseAgent, visualizer: Visualizer):
        self.agent = agent
        self._messages: list[BaseMessage] = []
        self.last_result: ExecutionResult | None = None
        self.visualizer = visualizer
        self._plot: VisualisationResult | None = None

    @property
    def df(self, *, rows_limit: int | None = None) -> DataFrame | None:
        if self.last_result and self.last_result.df is not None:
            if rows_limit:
                return self.last_result.df.head(rows_limit)
            return self.last_result.df
        return None

    @property
    def plot(self) -> Any:
        """Uses generated plot if present. If not, generates it."""
        if self.last_result:
            if self._plot is None:
                if self.df is None:
                    return None
                request = self.last_result.visualization_prompt
                if request:
                    vis_result = self.visualizer.visualize(request, self.last_result)
                    self._plot = vis_result
                else:
                    return None

            return self._plot.plot if self._plot else None
        return None

    @property
    def text(self) -> str:
        if self.last_result:
            return self.last_result.text
        return ""

    @property
    def meta(self) -> dict[str, Any]:
        if self.last_result and self.last_result.meta is not None:
            return self.last_result.meta
        return {}

    @property
    def sql(self) -> str | None:
        if self.last_result:
            return self.last_result.sql
        return None

    @property
    def code(self) -> str | None:
        if self.last_result and self._plot:
            return self._plot.code
        return None

    def ask(self, query: str) -> Self:
        self._plot = None
        self.last_result = self.agent.execute(self._messages + [HumanMessage(query)])
        self._messages = self.last_result.messages or []
        return self

    @property
    def messages(self) -> list[BaseMessage]:
        return self._messages
