from abc import ABC, abstractmethod
from typing import Any

from langchain_core.messages import BaseMessage
from langgraph.graph.state import CompiledStateGraph

from portus.agent.base_agent import ExecutionResult


class Graph(ABC):
    @abstractmethod
    def init_state(self, messages: list[BaseMessage]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def compile(self) -> CompiledStateGraph[Any]:
        raise NotImplementedError

    @abstractmethod
    def get_result(self, state: dict[str, Any]) -> ExecutionResult:
        raise NotImplementedError
