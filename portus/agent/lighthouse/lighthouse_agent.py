from pathlib import Path
from typing import Any

from langchain_core.messages import BaseMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from portus.agent.base_agent import BaseAgent, ExecutionResult
from portus.agent.graph import Graph
from portus.agent.lighthouse.lighthouse_context import LighthouseContext
from portus.data_source.config_classes.schema_inspection_config import (
    InspectionOptions,
    SchemaInspectionConfig,
)
from portus.data_source.database_schema import summarize_schema
from portus.data_source.database_schema_types import DatabaseSchema
from portus.data_source.duckdb.duckdb_collection import DuckDBCollection


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
        self._schema_inspection_config = schema_inspection_config
        self._template_path = template_path
        self._compiled_graph = graph.compile()

    def render_system_prompt(self, db_schema: DatabaseSchema) -> str:
        schema_summary = summarize_schema(db_schema, self._schema_inspection_config.summary_type)
        lh_context = LighthouseContext(template_path=self._template_path, db_schema=schema_summary)
        return lh_context.render()

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
