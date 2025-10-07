from pathlib import Path
from typing import Any

from langchain_core.messages import BaseMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from portus.agent.base_agent import BaseAgent, ExecutionResult
from portus.agent.react_duckdb_agent import ReactDuckDBAgent
from portus.data_source.data_collection import DataCollection
from portus.langchain_graphs.graph import Graph
from portus.llms import LLMConfig
from portus.utils import get_today_date_str, read_prompt_template

MAX_ROWS = 12


class LighthouseAgent(BaseAgent):
    def __init__(
        self,
        data_collection: DataCollection,
        graph: Graph,
        llm_config: LLMConfig,
        template_path: Path,
    ):
        self._data_collection = data_collection
        self._graph = graph
        self._compiled_graph = graph.compile(llm_config)
        self._template_path = template_path

    def render_system_prompt(self) -> str:
        # TODO: Add Context support
        prompt_template = read_prompt_template(self._template_path)
        db_schema = ReactDuckDBAgent.describe_duckdb_schema(self._data_collection.get_connection())

        prompt = prompt_template.render(
            date=get_today_date_str(),
            db_schema=db_schema,
        )
        return prompt

    def execute(self, messages: list[BaseMessage]) -> ExecutionResult:
        if messages[0].type != "system":
            messages = [SystemMessage(self.render_system_prompt()), *messages]
        init_state = self._graph.init_state(messages)
        last_state: dict[str, Any] | None = None
        try:
            for chunk in self._compiled_graph.stream(
                init_state,
                stream_mode="values",
                config=RunnableConfig(recursion_limit=50),
            ):
                assert isinstance(chunk, dict)
                last_state = chunk
        except Exception as e:
            return ExecutionResult(text=str(e), df=None, meta={}, sql="", messages=[])
        assert last_state is not None

        return self._graph.get_result(last_state)
