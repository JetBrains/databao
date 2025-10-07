from abc import ABC, abstractmethod
from typing import Any

from langchain_core.messages import BaseMessage
from langgraph.graph.state import CompiledStateGraph

from portus.agent.base_agent import ExecutionResult
from portus.llms import LLMConfig


class Graph(ABC):
    @abstractmethod
    def init_state(self, messages: list[BaseMessage]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def compile(self, model_config: LLMConfig) -> CompiledStateGraph[Any]:
        raise NotImplementedError

    @abstractmethod
    def get_result(self, state: dict[str, Any]) -> ExecutionResult:
        raise NotImplementedError
