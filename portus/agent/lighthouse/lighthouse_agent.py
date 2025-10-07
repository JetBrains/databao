from pathlib import Path
from typing import Any

from langchain_core.messages import BaseMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from portus.agent.base_agent import BaseAgent, ExecutionResult
from portus.agent.graph import Graph
from portus.data_source.config_classes.schema_inspection_config import (
    InspectionOptions,
    SchemaInspectionConfig,
)
from portus.data_source.database_schema import summarize_schema
from portus.data_source.database_schema_types import DatabaseSchema
from portus.data_source.duckdb.duckdb_collection import DuckDBCollection
from portus.utils import get_today_date_str, read_prompt_template


class LighthouseAgent(BaseAgent):
    def __init__(
        self,
        data_collection: DuckDBCollection,
        graph: Graph,
        schema_inspection_config: SchemaInspectionConfig,
        template_path: Path = Path("agent_system.jinja"),
    ):
        self._data_collection = data_collection
        self._graph = graph
        self._prompt_template = read_prompt_template(template_path)
        self._compiled_graph = graph.compile()

        self._schema_inspection_config = schema_inspection_config

    def render_system_prompt(self, db_schema: DatabaseSchema) -> str:
        # TODO: Add Context support
        schema_summary = summarize_schema(db_schema, self._schema_inspection_config.summary_type)
        prompt = self._prompt_template.render(
            date=get_today_date_str(),
            db_schema=schema_summary,
        )
        return prompt

    def execute(self, messages: list[BaseMessage]) -> ExecutionResult:
        # TODO cache schema inspection and invalidate when the data collection changes
        db_schema = self._data_collection.inspect_schema("full", InspectionOptions())

        if messages[0].type != "system":
            messages = [SystemMessage(self.render_system_prompt(db_schema)), *messages]
        init_state = self._graph.init_state(messages)

        last_state: dict[str, Any] | None = None
        try:
            for chunk in self._compiled_graph.stream(
                init_state,
                stream_mode="values",
                config=RunnableConfig(configurable={"database_schema": db_schema}, recursion_limit=50),
            ):
                assert isinstance(chunk, dict)
                last_state = chunk
        except Exception as e:
            return ExecutionResult(text=str(e), df=None, meta={}, sql="", messages=[])
        assert last_state is not None

        return self._graph.get_result(last_state)
